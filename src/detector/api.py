"""Core facade: :class:`Detector`.

One query, every database, one merged record::

    from detector import Detector, lookup, distance

    detector = Detector()                             # opens the bundled databases
    info = detector.lookup("8.8.8.8")                 # -> IPInfo
    info.country.iso_code                             # 'US'
    info.cross_check["country"]                       # every source's answer
    info.to_dict()                                    # standard JSON document

    detector.distance("8.8.8.8", "1.1.1.1")           # 1 to 1   -> Distance
    detector.distance("8.8.8.8", ["1.1.1.1", "::1"])  # 1 to N   -> list[Distance]

Async equivalents live in :mod:`detector.aio` (``AsyncDetector``): same surface,
plus concurrent batches and native asyncio downloads.
"""

from __future__ import annotations

import ipaddress
import socket
import threading
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from .cache import LRUCache
from .databases import (
    BUILTIN_DATASETS,
    BUNDLED_KEYS,
    NON_REDISTRIBUTABLE_KEYS,
    OPTIONAL_KEYS,
    Database,
    describe_licenses,
    normalize_record,
    open_databases,
    resolve_data_dir,
    user_cache_dir,
)
from .distance import Distance, distance_between, get_method
from .exceptions import DatabaseError, InvalidIPError, NoDatabaseError
from .models import (
    DEFAULT_LOCALES,
    SCHEMA_VERSION,
    IPInfo,
)
from .preparation import Preparation, shared_preparation

__all__ = ["Detector", "IPLike", "parse_ip"]

IPLike = Union[str, int, "ipaddress.IPv4Address", "ipaddress.IPv6Address", IPInfo]

#: Fields compared across databases in ``IPInfo.cross_check``.
CROSS_CHECK_FIELDS = (
    "country",
    "country_name",
    "continent",
    "subdivision",
    "city",
    "postal",
    "asn",
    "asn_organization",
    "location",
)

#: Address-class probes, evaluated in one pass when a result is built.
_FLAG_NAMES = (
    "is_private",
    "is_global",
    "is_loopback",
    "is_reserved",
    "is_multicast",
    "is_unspecified",
)

def parse_ip(value: IPLike) -> Union[ipaddress.IPv4Address, ipaddress.IPv6Address]:
    """Coerce anything IP-ish into an ``ipaddress`` object."""
    if isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return value
    if isinstance(value, IPInfo):
        value = value.ip
    if isinstance(value, int):
        return ipaddress.ip_address(value)
    if isinstance(value, (bytes, bytearray)):
        return ipaddress.ip_address(bytes(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise InvalidIPError("empty IP", detail=value)
        if "/" in text:  # tolerate CIDR, use the network address
            return ipaddress.ip_network(text, strict=False).network_address
        try:
            return ipaddress.ip_address(text)
        except ValueError:
            host = text
            if text.startswith("[") and "]" in text:  # [::1]:80
                host = text[1 : text.index("]")]
            elif text.count(":") == 1:  # 1.2.3.4:80
                host = text.split(":")[0]
            try:
                return ipaddress.ip_address(host)
            except ValueError as exc:
                raise InvalidIPError(
                    f"not a valid IPv4/IPv6 address: {value!r}", detail=value
                ) from exc
    raise InvalidIPError(f"unsupported IP type: {type(value).__name__}", detail=repr(value))


def _flags_tuple(
    addr: Union[ipaddress.IPv4Address, ipaddress.IPv6Address]
) -> Tuple[Optional[bool], ...]:
    """``(is_private, is_global, is_loopback, is_reserved, is_multicast, is_unspecified)``."""
    get = getattr
    out: List[Optional[bool]] = []
    for name in _FLAG_NAMES:
        try:
            out.append(bool(get(addr, name)))
        except (AttributeError, ValueError):  # pragma: no cover - odd addresses
            out.append(None)
    return tuple(out)


def _en_name(place: Any) -> Optional[str]:
    """Fast English name lookup (the cross-check comparison uses English)."""
    if place is None:
        return None
    names = place.names
    if not names:
        return None
    value = names.get("en")
    if value:
        return value
    for value in names.values():
        if value:
            return value
    return None


class Detector:
    """Thread-safe IP intelligence client. Build once, reuse everywhere."""

    def __init__(
        self,
        *,
        databases: Optional[Mapping[str, Union[str, Path]]] = None,
        db_dir: Optional[Union[str, Path]] = None,
        cache_dir: Optional[Union[str, Path]] = None,
        datasets: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        extra_dirs: Optional[Sequence[Union[str, Path]]] = None,
        locales: Sequence[str] = DEFAULT_LOCALES,
        distance_method: str = "haversine",
        cache_size: int = 4096,
        load_mode: str = "auto",
        extract: bool = True,
        strict: bool = False,
        resolve_dns: bool = False,
        include_raw: bool = True,
        include_cross_check: bool = True,
        include_all_names: bool = True,
        wait: str = "all",
        wait_timeout: Optional[float] = None,
        on_timeout: str = "partial",
    ) -> None:
        self.locales: Tuple[str, ...] = tuple(locales)
        self.distance_method = distance_method
        get_method(distance_method)  # fail fast instead of at the first distance call
        self.strict = strict
        self.resolve_dns = resolve_dns
        self.load_mode = load_mode
        self.include_raw = include_raw
        self.include_cross_check = include_cross_check
        self.include_all_names = include_all_names
        self.db_dir = resolve_data_dir(db_dir)
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir else user_cache_dir()

        # Datasets refreshed by `update_datasets()` land in the cache directory and
        # override the bundled copies carrying the same dataset key.
        dirs = [self.cache_dir, *(Path(item).expanduser() for item in (extra_dirs or ()))]
        self._datasets_filter = tuple(datasets) if datasets else None
        self._exclude_filter = tuple(exclude or ())
        self._wait = (wait or "all").lower()
        self._on_timeout = (on_timeout or "partial").lower()
        self._opened_logicals: set = set()
        self._refresh_lock = threading.Lock()
        self._prep: Optional[Preparation] = None

        if databases:
            # Explicit files win: no discovery, no preparation involved.
            self._databases: List[Database] = list(
                open_databases(
                    databases=databases,
                    cache_dir=self.cache_dir,
                    load_mode=load_mode,
                    extract=extract,
                    strict=strict,
                )
            )
        elif extract and self._has_archives([Path(self.db_dir), *dirs]):
            self._prep = shared_preparation(
                db_dir=db_dir,
                cache_dir=self.cache_dir,
                datasets=datasets,
                exclude=exclude,
                extra_dirs=dirs,
                extract=True,
            )
            self._prep.start()
            self._databases = []
            self._open_ready(prepare_all=(self._wait != "none"))
            if wait_timeout is not None or self._wait in ("all", "any"):
                self._apply_wait_policy(wait_timeout)
        elif extract:
            # No archives to unpack (pure lazy .bz or plain .mmdb): open directly,
            # which is instant because a .bz reader decompresses on demand.
            self._databases = list(
                open_databases(
                    db_dir=db_dir,
                    databases=None,
                    cache_dir=self.cache_dir,
                    load_mode=load_mode,
                    extract=False,
                    strict=strict,
                    include=datasets,
                    exclude=exclude,
                    extra_dirs=dirs,
                )
            )
        else:
            self._databases = list(
                open_databases(
                    db_dir=db_dir,
                    databases=None,
                    cache_dir=self.cache_dir,
                    load_mode=load_mode,
                    extract=False,
                    strict=strict,
                    include=datasets,
                    exclude=exclude,
                    extra_dirs=dirs,
                )
            )
        self._by_kind: Dict[str, List[Database]] = {}
        for db in self._databases:
            self._by_kind.setdefault(db.info.kind, []).append(db)

        self._cache = LRUCache(cache_size)
        self._meta = describe_licenses(self._databases)
        # Built once: regenerating eight database dicts per lookup used to dominate
        # the hot path. The list is shared read-only between all results.
        self._meta_base = {
            "schema_version": SCHEMA_VERSION,
            "databases": self._meta["databases"],
            "attribution": self._meta["attribution"],
        }
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #

    @property
    def databases(self) -> List[Database]:
        return list(self._databases)

    @property
    def dataset_keys(self) -> List[str]:
        """Loaded dataset keys, de-duplicated and in merge-priority order."""
        return list(dict.fromkeys(db.info.key for db in self._databases))

    @property
    def database_uids(self) -> List[str]:
        """One uid per loaded *file* (a dataset may span several files)."""
        return [db.info.uid for db in self._databases]

    @staticmethod
    def available_datasets() -> Dict[str, Dict[str, Any]]:
        """Registry of every dataset this SDK knows about (bundled + optional)."""
        return {
            key: {
                "name": spec.name,
                "kind": spec.kind,
                "license": spec.license,
                "bundled": spec.bundled,
                "redistributable": spec.redistributable,
                "update": spec.update,
                "providers": list(spec.providers),
                "files": list(spec.filenames),
                "sources": list(spec.available_sources),
                "homepage": spec.homepage,
            }
            for key, spec in BUILTIN_DATASETS.items()
        }

    def describe(self) -> Dict[str, Any]:
        """Database inventory, versions, licences and paths - for ops and audits."""
        return {
            "schema_version": SCHEMA_VERSION,
            "data_dir": str(self.db_dir),
            "cache_dir": str(self.cache_dir),
            "locales": list(self.locales),
            "distance_method": self.distance_method,
            "databases": [db.info.to_dict() for db in self._databases],
            "attribution": self._meta["attribution"],
            "bundled_datasets": list(BUNDLED_KEYS),
            "optional_datasets": list(OPTIONAL_KEYS),
            "non_redistributable_datasets": list(NON_REDISTRIBUTABLE_KEYS),
            "files": [db.info.file for db in self._databases],
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "databases": len(self._databases),
            "kinds": {kind: len(items) for kind, items in self._by_kind.items()},
            "cache": self._cache.stats(),
        }

    def _meta_for(self, locales: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Per-result metadata. ``databases`` is a shared read-only list."""
        self._maybe_refresh()
        meta = dict(self._meta_base)
        meta["locales"] = list(locales or self.locales)
        if self._prep is not None:
            meta["preparation"] = self._prep.readiness()
            failed = self._prep.failed
            if failed:
                meta["datasets_failed"] = dict(failed)
        return meta

    # ------------------------------------------------------------------ #
    # Preparation (progressive unpacking)
    # ------------------------------------------------------------------ #

    def _apply_wait_policy(self, wait_timeout: Optional[float]) -> None:
        """Honour ``wait`` / ``wait_timeout`` / ``on_timeout`` before returning."""
        prep = self._prep
        if prep is None:
            return
        policy = self._wait
        if policy == "none":
            satisfied = prep.ready_count > 0
        elif policy == "any":
            satisfied = prep.wait_for_any(wait_timeout)
        else:
            satisfied = prep.wait(wait_timeout)
        if satisfied:
            self._open_ready()
            self._raise_if_strict(prep)
            prep.fail_if_empty()
            if not self._databases:
                raise NoDatabaseError("every database failed to load")
            return
        if self._on_timeout == "error":
            raise prep.timeout_error(wait_timeout)
        # partial: answer with what is ready and keep loading in the background
        self._open_ready()
        self._raise_if_strict(prep)
        if not self._databases:
            prep.wait(None)
            self._open_ready()
        prep.fail_if_empty()
        if not self._databases:
            raise NoDatabaseError("every database failed to load")

    def _has_archives(self, dirs: List[Path]) -> bool:
        """Any archive (``.xz``/``.gz``/``.zst``/parts) left to unpack anywhere?"""
        from .databases import _archive_kind, discover_database_files

        for root in dirs:
            try:
                for path in discover_database_files(root):
                    if _archive_kind(path) is not None:
                        return True
            except OSError:
                continue
        return False

    def _raise_if_strict(self, prep: Preparation) -> None:
        """``strict=True`` means "a broken dataset is an error", not a warning."""
        if self.strict and prep.failed:
            labels = ", ".join(sorted(prep.failed))
            raise DatabaseError(
                f"failed to prepare {labels}", detail=prep.failed[sorted(prep.failed)[0]]
            )

    def _open_ready(self, prepare_all: bool = False) -> None:
        """Open the databases that are unpacked *now*, appending the new ones.

        Called once from the constructor and again lazily from ``_maybe_refresh``
        when the background preparation finishes more units - re-opening is just
        an ``mmap``, so incremental readiness costs microseconds per file.
        """
        prep = self._prep
        if prep is None:
            return
        if prepare_all and not prep.started:
            prep.start()
        ready = prep.ready_paths()
        pending = {name: path for name, path in ready.items() if name not in self._opened_logicals}
        if not pending:
            return
        opened = open_databases(
            db_dir=prep.db_dir,
            cache_dir=self.cache_dir,
            load_mode=self.load_mode,
            extract=False,
            strict=self.strict,
            prepared=pending,
            extra_dirs=prep.extra_dirs,
        )
        for db in opened:
            self._opened_logicals.add(db.path.name)
        self._databases.extend(opened)
        self._reindex()
        cache = getattr(self, "_cache", None)
        if cache is not None and opened:
            # Results computed with fewer databases must not survive: more data
            # means a fuller answer.
            cache.clear()

    def _reindex(self) -> None:
        self._databases.sort(key=lambda db: (db.priority, db.info.key))
        self._by_kind = {}
        for db in self._databases:
            self._by_kind.setdefault(db.info.kind, []).append(db)
        self._meta = describe_licenses(self._databases)
        self._meta_base = {
            "schema_version": SCHEMA_VERSION,
            "databases": self._meta["databases"],
            "attribution": self._meta["attribution"],
        }

    def _maybe_refresh(self) -> None:
        """Pick up datasets unpacked by the background thread since the last call."""
        prep = self._prep
        if prep is None or (not self._databases and not prep.started):
            return
        if len(self._opened_logicals) >= prep.ready_count:
            return
        with self._refresh_lock:
            self._open_ready()

    def refresh(self) -> Dict[str, Any]:
        """Re-scan the data directories and (re)open everything.

        Useful after :func:`~detector.update_datasets` drops newer files into the
        cache directory: the refreshed dataset replaces the bundled one, exactly
        like a fresh client would see it.
        """
        prep = self._prep
        if prep is None:
            self._databases = list(
                open_databases(
                    db_dir=self.db_dir,
                    cache_dir=self.cache_dir,
                    load_mode=self.load_mode,
                    extract=False,
                    strict=self.strict,
                    include=self._datasets_filter,
                    exclude=self._exclude_filter,
                    extra_dirs=[self.cache_dir],
                )
            )
            self._reindex()
            return {"databases": len(self._databases), "preparation": None}
        for db in self._databases:
            try:
                db.close()
            except Exception:  # pragma: no cover - closing must never raise
                pass
        self._databases = []
        self._opened_logicals = set()
        prep._units = prep._scan()  # type: ignore[attr-defined]
        prep._finished = False        # type: ignore[attr-defined]
        prep._started = False         # type: ignore[attr-defined]
        prep.start()
        self._open_ready(prepare_all=True)
        self._apply_wait_policy(None)
        return {"databases": len(self._databases), "preparation": prep.progress()}

    @property
    def preparation(self) -> Optional[Preparation]:
        """The background preparation, or ``None`` for explicit-file clients."""
        return self._prep

    # ------------------------------------------------------------------ #
    # Lookup
    # ------------------------------------------------------------------ #

    def lookup(
        self,
        ip: IPLike,
        *,
        locales: Optional[Sequence[str]] = None,
        raise_on_missing: Optional[bool] = None,
        use_cache: bool = True,
        resolve_dns: Optional[bool] = None,
        include_raw: Optional[bool] = None,
    ) -> IPInfo:
        self._maybe_refresh()
        """Look one IP up across every loaded database.

        Missing data is never an error: the result carries ``found=False`` and
        ``None`` fields. Malformed input always raises ``InvalidIPError``.
        ``raise_on_missing=True`` turns "no data for this address" into an
        exception as well (useful when a missing record must fail loudly).
        """
        addr = parse_ip(ip)
        want_raw = self.include_raw if include_raw is None else include_raw
        key = f"{addr}|{int(want_raw)}"

        if use_cache:
            hit, cached = self._cache.get(key)
            if hit:
                return cached  # type: ignore[return-value]

        info = self._lookup_uncached(addr, locales=locales, include_raw=want_raw)
        if (raise_on_missing if raise_on_missing is not None else self.strict) and not info.found:
            raise InvalidIPError(f"no data for {addr}", detail=str(addr))
        if resolve_dns if resolve_dns is not None else self.resolve_dns:
            info.hostname = _reverse_dns(str(addr))
        if use_cache:
            self._cache.set(key, info)
        return info

    def _lookup_uncached(
        self,
        addr: Union[ipaddress.IPv4Address, ipaddress.IPv6Address],
        *,
        locales: Optional[Sequence[str]] = None,
        include_raw: bool = True,
    ) -> IPInfo:
        text = str(addr)
        (
            is_private,
            is_global,
            is_loopback,
            is_reserved,
            is_multicast,
            is_unspecified,
        ) = _flags_tuple(addr)
        info = IPInfo(
            ip=text,
            version=addr.version,
            is_private=is_private,
            is_global=is_global,
            is_loopback=is_loopback,
            is_reserved=is_reserved,
            is_multicast=is_multicast,
            is_unspecified=is_unspecified,
        )

        version = addr.version
        hits: List[Tuple[Database, Dict[str, Any], int]] = []
        for db in self._databases:
            # Skip files that cannot answer for this address family (an IPv4-only
            # GeoLite2-City database has nothing to say about an IPv6 address).
            if version not in db.ip_versions:
                continue
            found = db.lookup(text)
            if found:
                hits.append((db, found[0], found[1]))

        want_cross = self.include_cross_check
        cross: Dict[str, Dict[str, Any]] = {}
        best_prefix = -1
        best_network: Optional[str] = None
        sources = info.sources
        networks = info.networks
        raw = info.raw

        # Priority order: the first database that fills a field wins; the others
        # only fill gaps. Every answer still lands in cross_check / raw.
        hits.sort(key=lambda item: (item[0].priority, item[0].info.key))
        for db, record, prefix_len in hits:
            parts = normalize_record(record)
            info.found = True
            if info.continent is None and parts.continent is not None:
                info.continent = parts.continent
            if info.country is None and parts.country is not None:
                info.country = parts.country
            if not info.subdivisions and parts.subdivisions:
                info.subdivisions = parts.subdivisions
            if info.city is None and parts.city is not None:
                info.city = parts.city
            if info.location is None and parts.location is not None:
                info.location = parts.location
            if info.postal is None and parts.postal is not None:
                info.postal = parts.postal
            if info.asn is None and parts.asn is not None:
                info.asn = parts.asn
            if parts.traits:
                for key, value in parts.traits.items():
                    info.traits.setdefault(key, value)

            db_info = db.info
            uid = db_info.uid
            sources[uid] = db_info.label
            networks[uid] = network = f"{text}/{prefix_len}"
            if prefix_len > best_prefix:
                best_prefix = prefix_len
                best_network = network
            if include_raw:
                raw[uid] = record
            if want_cross:
                _collect_cross(cross, parts, db_info.label)

        if best_network is not None:
            info.network = best_network
        if want_cross and cross:
            info.cross_check = cross
            agreement: Dict[str, bool] = {}
            conflicts: List[str] = []
            for field, values in cross.items():
                if len(values) < 2:
                    agreement[field] = True
                    continue
                iterator = iter(values.values())
                first = _comparable(next(iterator))
                same = True
                for value in iterator:
                    if _comparable(value) != first:
                        same = False
                        break
                agreement[field] = same
                if not same:
                    conflicts.append(field)
            info.agreement = agreement
            info.conflicts = sorted(conflicts)

        location = info.location
        if location is not None and location.time_zone:
            info.time_zone = location.time_zone
        if info.time_zone is None:
            trait_zone = info.traits.get("time_zone")
            if type(trait_zone) is str:
                info.time_zone = trait_zone
        info.meta = self._meta_for(locales)
        info._all_names = self.include_all_names
        return info

    def lookup_many(
        self,
        ips: Iterable[IPLike],
        *,
        locales: Optional[Sequence[str]] = None,
        ignore_errors: bool = True,
    ) -> List[IPInfo]:
        self._maybe_refresh()
        """Batch lookup: same length and order as the input.

        Sequential on purpose. Measured on an 8-core box with the full set of
        eight datasets: threads give no speedup (the Python-level merge holds the
        GIL: 470 us/ip with threads vs 424 us/ip without) and multiprocessing is
        actively slower (results carrying `raw` records pickle at ~10 KB per
        address, which costs more than the parallel win).

        For real parallelism, shard the input across OS processes - every process
        opens its own memory maps, and the OS page cache is shared, so RAM cost
        does not multiply:

            from concurrent.futures import ProcessPoolExecutor
            from detector import Detector

            def worker(chunk):
                with Detector(cache_size=0) as local:      # mmap, ~1 MB private RSS
                    return [info.to_dict() for info in local.lookup_many(chunk)]

            chunks = [ips[i::4] for i in range(4)]         # 4-way shard
            with ProcessPoolExecutor(4) as pool:
                for part in pool.map(worker, chunks):
                    ...
        """
        items = list(ips)
        if not items:
            return []

        results: List[IPInfo] = []
        for value in items:
            try:
                results.append(self.lookup(value, locales=locales))
            except InvalidIPError as exc:
                if not ignore_errors:
                    raise
                results.append(IPInfo(ip=str(value), found=False, meta={"error": exc.to_dict()}))
        return results

    def stream(
        self,
        ips: Iterable[IPLike],
        *,
        locales: Optional[Sequence[str]] = None,
    ) -> Iterator[IPInfo]:
        """Lazy lookup: constant memory, suitable for unbounded inputs."""
        for value in ips:
            try:
                yield self.lookup(value, locales=locales)
            except InvalidIPError as exc:
                yield IPInfo(ip=str(value), found=False, meta={"error": exc.to_dict()})

    # ------------------------------------------------------------------ #
    # Distance
    # ------------------------------------------------------------------ #

    def _resolve(
        self, items: Iterable[IPLike], locales: Optional[Sequence[str]]
    ) -> Tuple[Dict[str, IPInfo], Dict[str, str]]:
        """Resolve IP-ish values to ``IPInfo``; unparsable ones are reported, not raised."""
        infos: Dict[str, IPInfo] = {}
        errors: Dict[str, str] = {}
        pending: List[str] = []
        for item in items:
            label = item.ip if isinstance(item, IPInfo) else str(item)
            try:
                addr = str(parse_ip(item))
            except InvalidIPError as exc:
                errors[label] = exc.code
                continue
            if addr not in infos and addr not in pending:
                pending.append(addr)
        if pending:
            for info in self.lookup_many(pending, locales=locales):
                infos[info.ip] = info
        return infos, errors

    def distance(
        self,
        source: IPLike,
        targets: Union[IPLike, Iterable[IPLike]],
        *,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> Union[Distance, List[Distance]]:
        """Distance from ``source`` to one target (``Distance``) or many (``list``).

        A target that cannot be parsed produces a row with ``km=None`` and
        ``reason="invalid_ip"`` instead of blowing up the whole batch.
        """
        if isinstance(
            targets, (str, int, bytes, ipaddress.IPv4Address, ipaddress.IPv6Address, IPInfo)
        ):
            return self._one_distance(source, targets, method=method, locales=locales)

        target_list = list(targets)
        method = method or self.distance_method
        infos, errors = self._resolve([source, *target_list], locales=locales)

        source_label = source.ip if isinstance(source, IPInfo) else str(source)
        try:
            left = infos.get(str(parse_ip(source)))
        except InvalidIPError:
            left = None
        if left is None:
            reason = errors.get(source_label, "invalid_ip")
            return [
                Distance(source=source_label, target=str(item), method=method, reason=reason)
                for item in target_list
            ]

        results: List[Distance] = []
        for item in target_list:
            label = item.ip if isinstance(item, IPInfo) else str(item)
            try:
                addr = str(parse_ip(item))
            except InvalidIPError as exc:
                results.append(
                    Distance(source=left.ip, target=label, method=method, reason=exc.code)
                )
                continue
            right = infos.get(addr)
            if right is None:
                results.append(
                    Distance(source=left.ip, target=addr, method=method, reason="not_found")
                )
                continue
            results.append(distance_between(left, right, method=method))
        return results

    distance_1toN = distance  # explicit alias, self-documenting

    def _one_distance(
        self,
        source: IPLike,
        target: IPLike,
        *,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> Distance:
        method = method or self.distance_method
        try:
            left = self.lookup(source, locales=locales)
        except InvalidIPError as exc:
            return Distance(source=str(source), target=str(target), method=method, reason=exc.code)
        try:
            right = self.lookup(target, locales=locales)
        except InvalidIPError as exc:
            return Distance(source=left.ip, target=str(target), method=method, reason=exc.code)
        return distance_between(left, right, method=method)

    def distance_many(
        self,
        sources: Iterable[IPLike],
        targets: Iterable[IPLike],
        *,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> List[Distance]:
        """Full N x M product, with every IP resolved exactly once.

        Entries that cannot be parsed are dropped (use :meth:`distance` when you
        need per-row ``reason`` reporting for bad input).
        """
        resolved, _errors = self._resolve([*sources, *targets], locales=locales)
        source_list = [key for key in dict.fromkeys(_labels(sources)) if key in resolved]
        target_list = [key for key in dict.fromkeys(_labels(targets)) if key in resolved]
        method = method or self.distance_method
        return [
            distance_between(resolved[left], resolved[right], method=method)
            for left in source_list
            for right in target_list
        ]

    def nearest(
        self,
        source: IPLike,
        targets: Iterable[IPLike],
        *,
        limit: int = 5,
        max_km: Optional[float] = None,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> List[Distance]:
        """Closest ``limit`` targets, ascending by distance (no coordinates -> dropped)."""
        outcome = self.distance(source, targets, method=method, locales=locales)
        candidates: List[Distance] = [outcome] if isinstance(outcome, Distance) else list(outcome)
        results = [item for item in candidates if item.available]
        if max_km is not None:
            results = [item for item in results if item.km is not None and item.km <= max_km]
        results.sort(key=lambda item: item.km or 0.0)
        return results[:limit]

    # ------------------------------------------------------------------ #
    # JSON protocol
    # ------------------------------------------------------------------ #

    def request(self, payload: Any) -> Any:
        """Handle a ``{"type","action","data","status"}`` envelope -> ``Response``."""
        from .envelope import handle

        return handle(self, payload)

    def handle(self, payload: Any) -> Dict[str, Any]:
        """Same as :meth:`request`, returning a plain dict."""
        return self.request(payload).to_dict()

    def handle_json(self, payload: Union[str, bytes, Mapping[str, Any]]) -> str:
        """Protocol entry point: JSON in, JSON out."""
        from .envelope import handle, loads

        response = handle(self, loads(payload) if isinstance(payload, (str, bytes)) else payload)
        return response.to_json()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        for db in self._databases:
            try:
                db.close()
            except Exception:  # pragma: no cover - closing must never raise
                pass

    def __enter__(self) -> "Detector":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Detector datasets={','.join(self.dataset_keys)} data_dir={self.db_dir}>"


def _labels(items: Iterable[IPLike]) -> List[str]:
    """Normalized string labels for a batch of IP-ish values (invalid ones pass through)."""
    out: List[str] = []
    for item in items:
        if isinstance(item, IPInfo):
            out.append(item.ip)
            continue
        try:
            out.append(str(parse_ip(item)))
        except InvalidIPError:
            out.append(str(item))
    return out


def _comparable(value: Any) -> Any:
    """Normalize a cross-check value for equality comparison."""
    if type(value) is list:
        return tuple(value)
    return value


def _collect_cross(cross: Dict[str, Dict[str, Any]], parts: Any, source_name: str) -> None:
    """Record what this database said about the shared fields."""
    country = parts.country
    if country is not None:
        if country.iso_code is not None:
            cross.setdefault("country", {})[source_name] = country.iso_code
        en_name = _en_name(country)
        if en_name is not None:
            cross.setdefault("country_name", {})[source_name] = en_name
    continent = parts.continent
    if continent is not None and continent.code is not None:
        cross.setdefault("continent", {})[source_name] = continent.code
    if parts.subdivisions:
        en_name = _en_name(parts.subdivisions[0])
        if en_name is not None:
            cross.setdefault("subdivision", {})[source_name] = en_name
    city = parts.city
    if city is not None:
        en_name = _en_name(city)
        if en_name is not None:
            cross.setdefault("city", {})[source_name] = en_name
    if parts.postal is not None:
        cross.setdefault("postal", {})[source_name] = parts.postal
    asn = parts.asn
    if asn is not None:
        if asn.number is not None:
            cross.setdefault("asn", {})[source_name] = asn.number
        if asn.organization is not None:
            cross.setdefault("asn_organization", {})[source_name] = asn.organization
    location = parts.location
    if location is not None and location.valid:
        cross.setdefault("location", {})[source_name] = [location.latitude, location.longitude]


def _reverse_dns(ip: str, timeout: float = 2.0) -> Optional[str]:
    previous = socket.getdefaulttimeout()
    try:
        socket.setdefaulttimeout(timeout)
        return socket.gethostbyaddr(ip)[0]
    except (OSError, socket.error):  # pragma: no cover - network dependent
        return None
    finally:
        socket.setdefaulttimeout(previous)
