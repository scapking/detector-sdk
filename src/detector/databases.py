"""Database layer: discovery, decompression, MMDB reading and record normalization.

One rule above all: **any file that follows the MaxMind DB v2.0 spec can be
loaded here** - DB-IP, GeoLite2, GeoIP2, the MMDBs published by
``sapics/ip-location-db``, or a private database you built yourself. All
differences are flattened inside :func:`normalize_record`, and any field we do
not map explicitly still survives in ``traits`` / ``raw``.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import lzma
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import maxminddb

from .exceptions import DatabaseError, DatabaseNotFoundError, NoDatabaseError
from .models import ASN, City, Continent, Country, Location, Subdivision

__all__ = [
    "DatasetSpec",
    "MemberSpec",
    "SourceSpec",
    "NON_REDISTRIBUTABLE_KEYS",
    "BUILTIN_DATASETS",
    "DEFAULT_ATTRIBUTION",
    "DATABASE_PRIORITY",
    "DatabaseInfo",
    "Database",
    "NormalizedRecord",
    "detect_kind",
    "package_data_dir",
    "user_cache_dir",
    "resolve_data_dir",
    "discover_database_files",
    "ensure_extracted",
    "concat_parts",
    "extract_many",
    "extraction_dir",
    "logical_name",
    "part_info",
    "load_manifest",
    "normalize_record",
    "open_databases",
    "describe_licenses",
    "sha256_of",
]

DEFAULT_ATTRIBUTION = "IP Geolocation by DB-IP (https://db-ip.com)"

#: Lower value wins when several databases provide the same field.
DATABASE_PRIORITY: Dict[str, int] = {
    "city": 10,
    "enterprise": 15,
    "isp": 20,
    "asn": 30,
    "country": 40,
    "unknown": 90,
}


@dataclass(frozen=True)
class SourceSpec:
    """One download mirror."""

    name: str
    url_template: str  # supports {key} {period} (period = YYYY-MM)
    compressed: bool = True
    variant: str = ""  # which member of a split dataset this URL provides


@dataclass(frozen=True)
class MemberSpec:
    """One MMDB file that makes up a dataset.

    Most datasets are a single combined IPv4+IPv6 file. Some upstream datasets
    (GeoLite2-City on ip-location-db, for instance) are only published as
    separate IPv4 and IPv6 files - those become two members sharing one key.
    """

    filename: str
    variant: str = ""
    sources: Tuple[SourceSpec, ...] = ()


@dataclass(frozen=True)
class DatasetSpec:
    """Definition of one dataset from the ip-location-db / DB-IP / MaxMind family."""

    key: str
    name: str
    kind: str
    license: str
    attribution: str
    homepage: str
    members: Tuple[MemberSpec, ...]
    bundled: bool = True
    redistributable: bool = True
    update: str = "monthly"
    providers: Tuple[str, ...] = ()

    @property
    def filename(self) -> str:
        """First member's filename (backwards-compatible shorthand)."""
        return self.members[0].filename

    @property
    def filenames(self) -> Tuple[str, ...]:
        return tuple(member.filename for member in self.members)

    @property
    def sources(self) -> Tuple[SourceSpec, ...]:
        """Sources of the first member (shorthand for single-file datasets)."""
        return self.members[0].sources

    @property
    def available_sources(self) -> Tuple[str, ...]:
        seen: List[str] = []
        for member in self.members:
            for source in member.sources:
                if source.name not in seen:
                    seen.append(source.name)
        return tuple(seen)

    def url(self, source: str = "sapics", period: Optional[str] = None,
            variant: str = "") -> str:
        today = datetime.now(timezone.utc)
        period = period or f"{today.year:04d}-{today.month:02d}"
        for member in self.members:
            if variant and member.variant != variant:
                continue
            for spec in member.sources:
                if spec.name == source:
                    return spec.url_template.format(key=self.key, period=period)
        raise KeyError(f"unknown source {source!r} (variant={variant!r}) for {self.key!r}")


_SAPICS = "https://github.com/sapics/ip-location-db/releases/download/latest/{name}.mmdb"

_ATTRIB_DBIP = DEFAULT_ATTRIBUTION
_ATTRIB_PDDL = (
    "Country/ASN data from the ip-location-db project "
    "(https://github.com/sapics/ip-location-db), PDDL licensed"
)
_ATTRIB_GEOLITE2 = (
    "This product includes GeoLite2 data created by MaxMind, available from "
    "https://www.maxmind.com"
)


def _sapics(name: str, variant: str = "") -> SourceSpec:
    return SourceSpec("sapics", _SAPICS.format(name=name), compressed=False, variant=variant)


#: Every dataset the upstream ip-location-db project publishes, plus DB-IP's own
#: endpoints for the DB-IP Lite trio.
#:
#: **All of them ship inside the package** (``bundled=True``) so a lookup needs
#: no network access at all. `redistributable=False` marks data whose upstream
#: licence restricts redistribution - see NOTICE before republishing a wheel.
BUILTIN_DATASETS: Dict[str, DatasetSpec] = {
    # ---------------- DB-IP Lite (CC BY 4.0) ----------------
    "dbip-city": DatasetSpec(
        key="dbip-city",
        name="DBIP-City-Lite",
        kind="city",
        license="CC BY 4.0",
        attribution=_ATTRIB_DBIP,
        homepage="https://db-ip.com/db/download/ip-to-city-lite",
        update="monthly",
        providers=("city", "subdivision", "country", "continent", "location"),
        members=(
            MemberSpec(
                "dbip-city-lite.mmdb.xz",
                sources=(
                    SourceSpec(
                        "db-ip",
                        "https://download.db-ip.com/free/dbip-city-lite-{period}.mmdb.gz",
                    ),
                ),
            ),
        ),
    ),
    "dbip-asn": DatasetSpec(
        key="dbip-asn",
        name="DBIP-ASN-Lite",
        kind="asn",
        license="CC BY 4.0",
        attribution=_ATTRIB_DBIP,
        homepage="https://db-ip.com/db/download/ip-to-asn-lite",
        update="monthly",
        providers=("asn",),
        members=(
            MemberSpec(
                "dbip-asn-lite.mmdb.xz",
                sources=(
                    SourceSpec(
                        "db-ip",
                        "https://download.db-ip.com/free/dbip-asn-lite-{period}.mmdb.gz",
                    ),
                    _sapics("dbip-asn"),
                ),
            ),
        ),
    ),
    "dbip-country": DatasetSpec(
        key="dbip-country",
        name="DBIP-Country-Lite",
        kind="country",
        license="CC BY 4.0",
        attribution=_ATTRIB_DBIP,
        homepage="https://db-ip.com/db/download/ip-to-country-lite",
        update="monthly",
        providers=("country", "continent"),
        members=(
            MemberSpec(
                "dbip-country-lite.mmdb.xz",
                sources=(
                    SourceSpec(
                        "db-ip",
                        "https://download.db-ip.com/free/dbip-country-lite-{period}.mmdb.gz",
                    ),
                    _sapics("dbip-country"),
                ),
            ),
        ),
    ),
    # ---------------- MaxMind GeoLite2 (bundled, licence-restricted) ----------------
    "geolite2-city": DatasetSpec(
        key="geolite2-city",
        name="GeoLite2-City",
        kind="city",
        license="GeoLite2 EULA (MaxMind)",
        attribution=_ATTRIB_GEOLITE2,
        homepage="https://github.com/sapics/ip-location-db/tree/main/geolite2-city",
        update="twice weekly",
        redistributable=False,
        providers=("city", "subdivision", "country", "continent", "location", "postal"),
        members=(
            MemberSpec("geolite2-city-ipv4.mmdb.xz", variant="ipv4",
                       sources=(_sapics("geolite2-city-ipv4", "ipv4"),)),
            MemberSpec("geolite2-city-ipv6.mmdb.xz", variant="ipv6",
                       sources=(_sapics("geolite2-city-ipv6", "ipv6"),)),
        ),
    ),
    "geolite2-asn": DatasetSpec(
        key="geolite2-asn",
        name="GeoLite2-ASN",
        kind="asn",
        license="GeoLite2 EULA (MaxMind)",
        attribution=_ATTRIB_GEOLITE2,
        homepage="https://github.com/sapics/ip-location-db/tree/main/geolite2-asn",
        update="twice weekly",
        redistributable=False,
        providers=("asn",),
        members=(MemberSpec("geolite2-asn.mmdb.xz", sources=(_sapics("geolite2-asn"),)),),
    ),
    "geolite2-country": DatasetSpec(
        key="geolite2-country",
        name="GeoLite2-Country",
        kind="country",
        license="GeoLite2 EULA (MaxMind)",
        attribution=_ATTRIB_GEOLITE2,
        homepage="https://github.com/sapics/ip-location-db/tree/main/geolite2-country",
        update="twice weekly",
        redistributable=False,
        providers=("country", "continent"),
        members=(MemberSpec("geolite2-country.mmdb.xz", sources=(_sapics("geolite2-country"),)),),
    ),
    # ---------------- PDDL community data ----------------
    "iptoasn-asn": DatasetSpec(
        key="iptoasn-asn",
        name="IPtoASN-ASN",
        kind="asn",
        license="PDDL",
        attribution=_ATTRIB_PDDL,
        homepage="https://github.com/sapics/ip-location-db/tree/main/iptoasn-asn",
        update="daily",
        providers=("asn",),
        members=(MemberSpec("iptoasn-asn.mmdb.xz", sources=(_sapics("iptoasn-asn"),)),),
    ),
    "iptoasn-country": DatasetSpec(
        key="iptoasn-country",
        name="IPtoASN-Country",
        kind="country",
        license="PDDL",
        attribution=_ATTRIB_PDDL,
        homepage="https://github.com/sapics/ip-location-db/tree/main/iptoasn-country",
        update="daily",
        providers=("country",),
        members=(MemberSpec("iptoasn-country.mmdb.xz", sources=(_sapics("iptoasn-country"),)),),
    ),
    "origin-asn": DatasetSpec(
        key="origin-asn",
        name="Origin-ASN",
        kind="asn",
        license="PDDL",
        attribution=_ATTRIB_PDDL,
        homepage="https://github.com/sapics/ip-location-db/tree/main/origin-asn",
        update="daily",
        providers=("asn",),
        members=(MemberSpec("origin-asn.mmdb.xz", sources=(_sapics("origin-asn"),)),),
    ),
    "user-country": DatasetSpec(
        key="user-country",
        name="User-Country",
        kind="country",
        license="PDDL",
        attribution=_ATTRIB_PDDL,
        homepage="https://github.com/sapics/ip-location-db/tree/main/user-country",
        update="daily",
        providers=("country",),
        members=(MemberSpec("user-country.mmdb.xz", sources=(_sapics("user-country"),)),),
    ),
    "server-country": DatasetSpec(
        key="server-country",
        name="Server-Country",
        kind="country",
        license="PDDL",
        attribution=_ATTRIB_PDDL,
        homepage="https://github.com/sapics/ip-location-db/tree/main/server-country",
        update="daily",
        providers=("country",),
        members=(MemberSpec("server-country.mmdb.xz", sources=(_sapics("server-country"),)),),
    ),
}

#: Everything ships in the package.
BUNDLED_KEYS: Tuple[str, ...] = tuple(BUILTIN_DATASETS)

#: Datasets whose upstream licence does not permit redistribution of the data.
NON_REDISTRIBUTABLE_KEYS: Tuple[str, ...] = tuple(
    spec.key for spec in BUILTIN_DATASETS.values() if not spec.redistributable
)

#: Kept for API compatibility: datasets a user may want to fetch separately.
OPTIONAL_KEYS: Tuple[str, ...] = ()


# --------------------------------------------------------------------------- #
# Paths and cache directories
# --------------------------------------------------------------------------- #


def package_data_dir() -> Path:
    """Directory holding the databases shipped inside the package."""
    return Path(__file__).resolve().parent / "data"


def user_cache_dir() -> Path:
    """Where decompressed ``.mmdb`` files and user-provided databases live."""
    env = os.environ.get("DETECTOR_CACHE_DIR")
    if env:
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".cache"
    return base / "detector"


def resolve_data_dir(db_dir: Optional[os.PathLike] = None) -> Path:
    """Decide which directory holds the "bundled" datasets."""
    if db_dir is not None:
        return Path(db_dir).expanduser()
    env = os.environ.get("DETECTOR_DB_DIR")
    if env:
        return Path(env).expanduser()
    return package_data_dir()


def manifest_key(key: str, variant: str = "") -> str:
    """Manifest entries are keyed per file: ``dataset key[:variant]``."""
    return f"{key}:{variant}" if variant else key


def load_manifest(db_dir: Optional[os.PathLike] = None) -> Dict[str, Dict[str, Any]]:
    """Read ``MANIFEST.json`` (written when the data was built).

    Returns ``{"<dataset key>[:<variant>]": entry}``; empty when absent.
    """
    path = resolve_data_dir(db_dir) / "MANIFEST.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return {
        manifest_key(entry.get("key", ""), entry.get("variant", "")): entry
        for entry in data.get("datasets", [])
    }


def extraction_dir(cache_dir: Optional[os.PathLike] = None) -> Path:
    """Where decompressed copies live.

    Kept in a dedicated ``extracted/`` subdirectory so that scanning the cache
    directory (which is how refreshed datasets override the bundled ones) never
    picks up derived artefacts.
    """
    root = Path(cache_dir).expanduser() if cache_dir else user_cache_dir()
    return root / "extracted"


#: Archive extensions accepted for bundled/updated databases, longest first.
ARCHIVE_SUFFIXES: Tuple[str, ...] = (".mmdb.zst", ".mmdb.gz", ".mmdb.xz", ".zst", ".gz", ".xz")

#: A bundled database may be split into independently decodable parts
#: (``dbip-city-lite.mmdb.part001.xz``). The parts are merged back into a single
#: ``.mmdb`` in the cache directory; because each part is its own compressed
#: stream they decompress in parallel, which is the whole point of splitting.
PART_PATTERN = re.compile(r"^(?P<stem>.+\.mmdb)\.part(?P<index>\d{1,4})\.(?P<kind>gz|xz|zst)$")

#: Supported compression codecs.
ARCHIVE_KINDS: Tuple[str, ...] = ("gz", "xz", "zst")


def part_info(name: str) -> Optional[Tuple[str, int, str]]:
    """``"dbip-city-lite.mmdb.part003.xz"`` -> ``("dbip-city-lite.mmdb", 3, "xz")``."""
    match = PART_PATTERN.match(name)
    if match is None:
        return None
    return match.group("stem"), int(match.group("index")), match.group("kind")


def logical_name(name: str) -> str:
    """The file this name resolves to once decompressed and merged.

    ``dbip-city-lite.mmdb.part003.xz`` -> ``dbip-city-lite.mmdb``
    ``dbip-city-lite.mmdb.xz``         -> ``dbip-city-lite.mmdb``
    ``dbip-city-lite.xz``              -> ``dbip-city-lite``
    ``dbip-city-lite.mmdb``            -> ``dbip-city-lite.mmdb``
    """
    info = part_info(name)
    if info is not None:
        return info[0]
    for kind in ARCHIVE_KINDS:
        if name.endswith(f".mmdb.{kind}"):
            return name[: -len(f".{kind}")]  # keep the ".mmdb" marker
        if name.endswith(f".{kind}"):
            return name[: -len(f".{kind}")]
    return name


def _archive_kind(path: Path) -> Optional[str]:
    """``"gz"`` / ``"xz"`` / ``"zst"`` / ``None`` for a (possibly split) archive."""
    name = path.name
    info = part_info(name)
    if info is not None:
        return info[2]
    for kind in ARCHIVE_KINDS:
        if name.endswith(f".mmdb.{kind}"):
            return kind
    return None


def _open_archive(path: Path, kind: str) -> Any:
    """Open a compressed stream for reading (context manager)."""
    if kind == "gz":
        return gzip.open(path, "rb")
    if kind == "xz":
        return lzma.open(path, "rb")
    if kind == "zst":
        try:
            import zstandard
        except ImportError as exc:  # pragma: no cover - depends on install flavour
            raise DatabaseError(
                f"{path.name} is zstd-compressed; install the optional codec: "
                "pip install 'detector-sdk[zstd]'",
                detail=str(path),
            ) from exc
        handle = open(path, "rb")
        reader = zstandard.ZstdDecompressor().stream_reader(handle)
        return _ClosingReader(reader, handle)
    raise DatabaseError(f"unsupported compression: {kind}", detail=str(path))


class _ClosingReader:
    """Stream reader that also closes the underlying file handle."""

    def __init__(self, reader: Any, handle: Any) -> None:
        self._reader = reader
        self._handle = handle

    def read(self, size: int = -1) -> bytes:
        return self._reader.read(size)

    def __enter__(self) -> "_ClosingReader":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._reader.close()
        finally:
            self._handle.close()


def _parts_of(path: Path) -> List[Path]:
    """Every sibling part of the same logical database, ordered by index."""
    info = part_info(path.name)
    if info is None:
        return [path]
    stem, _, kind = info
    siblings = []
    for candidate in sorted(path.parent.iterdir()):
        other = part_info(candidate.name)
        if candidate.is_file() and other is not None and other[0] == stem and other[2] == kind:
            siblings.append(candidate)
    if path not in siblings:  # pragma: no cover - defensive
        siblings.append(path)
    return sorted(siblings, key=lambda item: part_info(item.name)[1])  # type: ignore[index]


def _part_siblings(target_dir: Path, member: Any) -> List[Path]:
    """Part files of a (possibly split) bundled member, in index order."""
    stem = member.filename
    for suffix in ARCHIVE_SUFFIXES:
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    if str(stem).endswith(".mmdb"):
        pass
    elif not str(stem).endswith(".mmdb"):
        stem = f"{stem}.mmdb"
    if not target_dir.is_dir():
        return []
    found = [
        item for item in target_dir.iterdir()
        if item.is_file() and part_info(item.name) is not None
        and part_info(item.name)[0] == stem
    ]
    return sorted(found, key=lambda item: part_info(item.name)[1])


def _extraction_workers() -> int:
    """Threads used while decompressing. Compression codecs release the GIL."""
    return max(1, min(8, (os.cpu_count() or 2)))


def _decompress_file_source(path: Path, kind: str, target: Path) -> Path:
    """Decompress one archive to ``target`` (a staging path), safely."""
    tmp = target.with_name(f"{target.name}.tmp{os.getpid()}")
    try:
        with _open_archive(path, kind) as src, open(tmp, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 20)
        os.replace(tmp, target)
    except (OSError, EOFError, lzma.LZMAError) as exc:
        tmp.unlink(missing_ok=True)
        raise DatabaseError(f"failed to decompress {path}: {exc}", detail=str(path)) from exc
    return target


def _lock_path_for(cache_dir: Path, logical: str) -> Path:
    return cache_dir / f".{logical}.lock"


def _assemble(staged: Sequence[Path], target: Path) -> None:
    """Concatenate staged parts, in order, into ``target`` (atomic replace)."""
    tmp = target.with_name(f"{target.name}.merge{os.getpid()}")
    try:
        with open(tmp, "wb") as dst:
            for chunk_path in staged:
                with open(chunk_path, "rb") as src:
                    shutil.copyfileobj(src, dst, length=1 << 22)
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
        for chunk_path in staged:
            chunk_path.unlink(missing_ok=True)


def concat_parts(parts: Sequence[Path], target: Path) -> Path:
    """Decode parts straight into ``target`` in index order (single pass).

    Used by the manifest builder, where a temporary assembly is enough; the
    runtime path stages parts on a thread pool first so decoding runs in
    parallel.
    """
    try:
        with open(target, "wb") as dst:
            for part in parts:
                kind = _archive_kind(part)
                if kind is None:  # pragma: no cover - defensive
                    raise DatabaseError(f"not an archive: {part}", detail=str(part))
                with _open_archive(part, kind) as src:
                    shutil.copyfileobj(src, dst, length=1 << 22)
    except (OSError, EOFError, lzma.LZMAError) as exc:
        target.unlink(missing_ok=True)
        raise DatabaseError(f"failed to assemble {target.name}: {exc}", detail=str(target)) from exc
    return target


def _part_size_map(db_dir: Path) -> Dict[str, Dict[str, int]]:
    """``{logical name: {part name: uncompressed bytes}}`` from ``MANIFEST.json``.

    With the uncompressed size of every part known up front, a split database can
    be written straight to its final offsets - one write pass instead of the
    decode-to-staging + concatenate round trip.
    """
    try:
        data = load_manifest(db_dir)
    except Exception:  # pragma: no cover - manifest is optional
        return {}
    out: Dict[str, Dict[str, int]] = {}
    for entry in data.values():
        logical = entry.get("file")
        parts = entry.get("parts") or []
        sizes = entry.get("part_sizes") or []
        if logical and parts and len(parts) == len(sizes):
            out[str(logical)] = dict(zip(parts, sizes))
    return out


def _decompress_at(path: Path, kind: str, handle: Any, offset: int) -> int:
    """Decode one archive straight into ``handle`` starting at ``offset``.

    Each worker opens its own handle and writes a disjoint byte range, so the
    parts of one database can be decoded concurrently into a single file.
    """
    handle.seek(offset)
    written = 0
    try:
        with _open_archive(path, kind) as src:
            while True:
                block = src.read(1 << 22)
                if not block:
                    break
                handle.write(block)
                written += len(block)
    except (OSError, EOFError, lzma.LZMAError) as exc:
        raise DatabaseError(f"failed to decompress {path}: {exc}", detail=str(path)) from exc
    return written


def _decode_bytes(path: Path, kind: str) -> bytes:
    """Whole decompressed part, in memory (used by the pipelined merge)."""
    with _open_archive(path, kind) as src:
        return src.read()


def _merge_parts(parts: Sequence[Path], target: Path, workers: int) -> None:
    """Decode parts in parallel and append them, in order, with one writer.

    Decoding scales with cores, while the file is written sequentially from a
    single thread: one write pass, no staging copy and no re-reading - which
    matters because on a slow disk the I/O (not the codec) is the bottleneck.
    Parts in flight are bounded, so peak memory stays around two parts.
    """
    if len(parts) == 1:
        _decompress_file_source(parts[0], _archive_kind(parts[0]) or "xz", target)
        return

    largest = max(part.stat().st_size for part in parts)
    budget = 128 << 20
    window = max(2, min(workers, max(1, budget // max(largest, 1))))
    staged = target.with_name(f"{target.name}.partial{os.getpid()}")
    order = list(parts)
    cursor = 0
    in_flight: Dict[int, Any] = {}
    try:
        with ThreadPoolExecutor(max_workers=window, thread_name_prefix="detector") as pool:

            def submit_next() -> None:
                nonlocal cursor
                if cursor < len(order) and len(in_flight) < window:
                    part = order[cursor]
                    in_flight[cursor] = pool.submit(
                        _decode_bytes, part, _archive_kind(part) or "xz"
                    )
                    cursor += 1

            for _ in range(min(window, len(order))):
                submit_next()
            with open(staged, "wb") as handle:
                for index in range(len(order)):
                    data = in_flight.pop(index).result()
                    handle.write(data)
                    del data
                    submit_next()
        os.replace(staged, target)
    except (OSError, EOFError, lzma.LZMAError, DatabaseError) as exc:
        staged.unlink(missing_ok=True)
        if isinstance(exc, DatabaseError):
            raise
        raise DatabaseError(f"failed to assemble {target.name}: {exc}", detail=str(target)) from exc


def extract_many(
    paths: Sequence[Path],
    cache_dir: Optional[os.PathLike] = None,
) -> List[Path]:
    """Decompress/merge several (possibly split) archives, decoding in parallel.

    Returns one resolved path per input, in the same order. Idempotent: an
    assembled database in the cache directory is reused as-is.

    Split databases are written **once**: when the uncompressed size of every
    part is known (the shipped ``MANIFEST.json`` records it), the parts are
    decoded concurrently straight into their final byte offsets, so no staging
    copy or concatenation is needed. Without sizes the parts are staged and
    concatenated. Either way the merged file is byte-identical to the original.
    """
    kinds = [_archive_kind(path) for path in paths]
    if all(kind is None for kind in kinds):
        return [Path(path) for path in paths]

    cache_dir_path = extraction_dir(cache_dir)
    units: Dict[str, Tuple[Path, List[Tuple[Path, str]]]] = {}
    for path, kind in zip(paths, kinds):
        if kind is None:
            continue
        target = cache_dir_path / logical_name(path.name)
        units[target.name] = (target, [(part, kind) for part in _parts_of(path)])

    resolved: Dict[str, Path] = {}
    missing: Dict[str, Tuple[Path, List[Tuple[Path, str]]]] = {}
    for name, unit in units.items():
        target = unit[0]
        if target.is_file() and target.stat().st_size > 0:
            resolved[name] = target
        else:
            missing[name] = unit

    if missing:
        try:
            cache_dir_path.mkdir(parents=True, exist_ok=True)
        except OSError:  # read-only HOME -> fall back to a temp dir
            cache_dir_path = Path(tempfile.mkdtemp(prefix="detector-"))
            for name, (target, sources) in list(missing.items()):
                missing[name] = (cache_dir_path / target.name, sources)

        staging = cache_dir_path / ".staging"
        staging.mkdir(parents=True, exist_ok=True)
        locks = [_file_lock(_lock_path_for(cache_dir_path, name)) for name in sorted(missing)]
        for lock in locks:
            lock.__enter__()
        try:
            workers = _extraction_workers()
            merged: List[Tuple[Path, List[Path]]] = []
            todo: List[Tuple[str, Path, str, Path]] = []
            for name, (target, sources) in missing.items():
                if target.is_file() and target.stat().st_size > 0:  # another process won
                    resolved[name] = target
                    continue
                part_paths = [source for source, _kind in sources]
                if len(part_paths) > 1:
                    # Split database: parallel decode + one sequential write pass.
                    merged.append((target, part_paths))
                else:
                    todo.append((name, part_paths[0], sources[0][1],
                                 staging / f"{name}.0001.partial"))

            with ThreadPoolExecutor(max_workers=max(2, workers),
                                    thread_name_prefix="detector") as pool:
                futures = [
                    pool.submit(_merge_parts, parts, target, workers) for target, parts in merged
                ]
                if todo:
                    list(pool.map(
                        lambda item: _decompress_file_source(item[1], item[2], item[3]), todo
                    ))
                for (target, _parts), future in zip(merged, futures):
                    future.result()
                    resolved[target.name] = target

            for name, _source, _kind, staged in todo:
                if name in resolved:
                    continue
                os.replace(staged, missing[name][0])
                resolved[name] = missing[name][0]
        finally:
            for lock in reversed(locks):
                lock.__exit__(None, None, None)
            try:
                if not any(staging.iterdir()):
                    staging.rmdir()
            except OSError:  # pragma: no cover - best effort
                pass

    out: List[Path] = []
    for path, kind in zip(paths, kinds):
        if kind is None:
            out.append(Path(path))
        else:
            out.append(resolved[logical_name(path.name)])
    return out


def ensure_extracted(path: Path, cache_dir: Optional[os.PathLike] = None) -> Path:
    """Decompress ``.mmdb.gz`` / ``.mmdb.xz`` / ``.mmdb.zst`` into ``<cache>/extracted``.

    Split databases (``.partNNN.xz``) are reassembled into a single ``.mmdb``.
    Idempotent, safe across processes (``flock``), and atomic: readers never see
    a half-written database.
    """
    return extract_many([Path(path)], cache_dir)[0]


class _file_lock:
    """``flock`` on Unix, no-op elsewhere (replace is atomic either way)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: Optional[int] = None

    def __enter__(self) -> "_file_lock":
        try:
            import fcntl

            self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(self._fd, fcntl.LOCK_EX)
        except (ImportError, OSError):  # pragma: no cover - Windows
            self._fd = None
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._fd is not None:
            try:
                import fcntl

                fcntl.flock(self._fd, fcntl.LOCK_UN)
            except (ImportError, OSError):  # pragma: no cover
                pass
            os.close(self._fd)
            self._fd = None


def discover_database_files(db_dir: Optional[os.PathLike] = None) -> List[Path]:
    """List every ``.mmdb`` / ``.mmdb.gz`` in the directory, sorted by name.

    Sort order defines merge priority only as a tie-breaker; the real ordering
    uses :data:`DATABASE_PRIORITY` (per field group).
    """
    root = resolve_data_dir(db_dir)
    if not root.is_dir():
        return []
    return [
        item
        for item in sorted(root.iterdir())
        if item.is_file()
        and (item.name.endswith(".mmdb") or _archive_kind(item) is not None)
    ]


# --------------------------------------------------------------------------- #
# Database objects
# --------------------------------------------------------------------------- #


def detect_kind(database_type: str) -> str:
    text = (database_type or "").lower()
    if "enterprise" in text:
        return "enterprise"
    if "city" in text:
        return "city"
    if "asn" in text:
        return "asn"
    if "isp" in text:
        return "isp"
    if "country" in text:
        return "country"
    if "anonymous" in text:
        return "anonymous"
    return "unknown"


def _license_for(database_type: str) -> Tuple[str, str]:
    text = (database_type or "").lower()
    if "dbip" in text:
        return "CC BY 4.0", DEFAULT_ATTRIBUTION
    if "geolite2" in text or "geoip2" in text:
        return (
            "GeoLite2/GeoIP2 EULA (MaxMind)",
            "This product includes GeoLite2 data created by MaxMind, "
            "available from https://www.maxmind.com",
        )
    return "unknown (self-provided)", ""


@dataclass
class DatabaseInfo:
    key: str
    name: str
    kind: str
    database_type: str
    file: str
    variant: str = ""
    build_date: Optional[str] = None
    build_epoch: Optional[int] = None
    description: str = ""
    license: str = "unknown"
    attribution: str = ""
    source_url: Optional[str] = None
    languages: Tuple[str, ...] = ()
    sha256: Optional[str] = None
    size: Optional[int] = None
    providers: Tuple[str, ...] = ()
    ip_versions: Tuple[int, ...] = (4, 6)

    @property
    def uid(self) -> str:
        """Unique key for one *file*: ``dbip-city`` or ``geolite2-city-ipv4``."""
        return f"{self.key}-{self.variant}" if self.variant else self.key

    @property
    def label(self) -> str:
        """Human readable source label used in ``sources`` / ``cross_check``."""
        base = self.name or self.database_type or self.key
        return f"{base} ({self.variant})" if self.variant else base

    def to_meta(self) -> Dict[str, Any]:
        """Compact form used in every result's ``meta.databases``.

        The full record (source URL, languages, providers, size) lives in
        :meth:`to_dict` and is available through ``Detector.describe()``.
        """
        return {
            "key": self.key,
            "variant": self.variant,
            "name": self.name,
            "kind": self.kind,
            "ip_versions": list(self.ip_versions),
            "build_date": self.build_date,
            "license": self.license,
            "sha256": self.sha256,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "variant": self.variant,
            "name": self.name,
            "kind": self.kind,
            "database_type": self.database_type,
            "file": self.file,
            "build_date": self.build_date,
            "description": self.description,
            "license": self.license,
            "attribution": self.attribution,
            "source_url": self.source_url,
            "languages": list(self.languages),
            "sha256": self.sha256,
            "size": self.size,
            "providers": list(self.providers),
            "ip_versions": list(self.ip_versions),
        }


class Database:
    """Reader wrapper around a single MMDB file."""

    def __init__(
        self,
        path: os.PathLike,
        *,
        key: Optional[str] = None,
        name: Optional[str] = None,
        variant: str = "",
        kind: Optional[str] = None,
        manifest: Optional[Mapping[str, Any]] = None,
        source_url: Optional[str] = None,
        providers: Tuple[str, ...] = (),
        license: Optional[str] = None,
        attribution: Optional[str] = None,
        load_mode: str = "auto",
    ) -> None:
        self.path = Path(path)
        if not self.path.is_file():
            raise DatabaseNotFoundError(f"database file not found: {self.path}", detail=str(self.path))
        try:
            self._reader = maxminddb.open_database(str(self.path), _reader_mode(load_mode))
        except Exception as exc:  # maxminddb raises a zoo of exception types
            raise DatabaseNotFoundError(
                f"cannot open MMDB: {exc}", detail=str(self.path)
            ) from exc

        metadata = self._reader.metadata()
        database_type = getattr(metadata, "database_type", "") or ""
        self._ip_versions = _address_families(
            getattr(metadata, "ip_version", 6), database_type
        )
        kind = kind or detect_kind(database_type)
        detected_license, detected_attribution = _license_for(database_type)
        entry = dict(manifest or {})
        # Resolution order: registry/manifest > metadata sniffing > placeholder.
        resolved_license = entry.get("license") or license or detected_license
        resolved_attribution = entry.get("attribution") or attribution or detected_attribution
        build_epoch = getattr(metadata, "build_epoch", None)
        self.info = DatabaseInfo(
            key=key or kind,
            name=name or database_type or (key or kind),
            variant=variant,
            kind=kind,
            database_type=entry.get("database_type") or database_type,
            file=self.path.name,
            build_date=entry.get("build_date") or _epoch_to_date(build_epoch),
            build_epoch=build_epoch,
            description=(getattr(metadata, "description", {}) or {}).get("en", ""),
            license=resolved_license,
            attribution=resolved_attribution,
            source_url=entry.get("source_url") or source_url or entry.get("homepage"),
            languages=tuple(getattr(metadata, "languages", ()) or ()),
            sha256=entry.get("sha256"),
            size=self.path.stat().st_size,
            providers=tuple(providers),
            ip_versions=self._ip_versions,
        )

    # ---------------- lookups ----------------

    def lookup(self, ip: str) -> Optional[Tuple[Dict[str, Any], int]]:
        """Return ``(record, prefix_len)`` or ``None`` when nothing matches.

        Also returns ``None`` when the address family does not apply to this file
        (an IPv6 lookup against an IPv4-only database), instead of raising.
        """
        try:
            record, prefix_len = self._reader.get_with_prefix_len(ip)
        except ValueError:
            return None
        if not record:
            return None
        return record, prefix_len

    def supports(self, version: int) -> bool:
        """Can this file answer for IPv4 (4) or IPv6 (6) addresses?"""
        return version in self.ip_versions

    @property
    def ip_versions(self) -> Tuple[int, ...]:
        """Address families this file covers: ``(4,)``, ``(6,)`` or ``(4, 6)``."""
        return self._ip_versions

    @property
    def kind(self) -> str:
        return self.info.kind

    @property
    def priority(self) -> int:
        return DATABASE_PRIORITY.get(self.info.kind, 90)

    def close(self) -> None:
        self._reader.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Database {self.info.database_type} {self.path.name}>"


def _reader_mode(load_mode: str) -> int:
    mode = (load_mode or "auto").lower()
    if mode == "memory":
        return maxminddb.MODE_MEMORY
    if mode == "mmap":
        return getattr(maxminddb, "MODE_MMAP_EXT", maxminddb.MODE_MMAP)
    return maxminddb.MODE_AUTO


def _address_families(ip_version: int, database_type: str) -> Tuple[int, ...]:
    """Which address families a file can answer.

    ``metadata.ip_version`` alone is not enough: an IPv4-only GeoLite2 build also
    reports ``6`` (MaxMind's reader always walks a 128-bit tree). The database
    type carries the split for the ip-location-db builds (``city ipv4`` /
    ``city ipv6``), so use both signals and default to "both".
    """
    if ip_version == 4:
        return (4,)
    text = (database_type or "").lower()
    tokens = text.replace("(", " ").replace(")", " ").split()
    if "ipv4" in tokens or text.endswith("ipv4"):
        return (4,)
    if "ipv6" in tokens or text.endswith("ipv6"):
        return (6,)
    return (4, 6)


def _epoch_to_date(epoch: Optional[int]) -> Optional[str]:
    if not epoch:
        return None
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m-%d")


def spec_for_filename(filename: str) -> Optional[DatasetSpec]:
    """Map ``dbip-city-lite-2026-09.mmdb.gz`` back to its :class:`DatasetSpec`."""
    best: Optional[DatasetSpec] = None
    best_length = 0
    for spec in BUILTIN_DATASETS.values():
        for member in spec.members:
            if _matches(filename, member.filename) and len(member.filename) > best_length:
                best = spec
                best_length = len(member.filename)
    return best


def member_for_filename(filename: str) -> Tuple[Optional[DatasetSpec], Optional[MemberSpec]]:
    """Resolve a filename to ``(dataset, member)`` - the split-aware variant."""
    best: Tuple[Optional[DatasetSpec], Optional[MemberSpec]] = (None, None)
    best_length = 0
    for spec in BUILTIN_DATASETS.values():
        for member in spec.members:
            if _matches(filename, member.filename) and len(member.filename) > best_length:
                best = (spec, member)
                best_length = len(member.filename)
    return best


def open_databases(
    *,
    db_dir: Optional[os.PathLike] = None,
    databases: Optional[Mapping[str, os.PathLike]] = None,
    cache_dir: Optional[os.PathLike] = None,
    load_mode: str = "auto",
    extract: bool = True,
    strict: bool = True,
    prepared: Optional[Mapping[str, Path]] = None,
    include: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
    extra_dirs: Optional[Sequence[os.PathLike]] = None,
) -> List[Database]:
    """Open the database set.

    Multi-file datasets (GeoLite2-City = IPv4 + IPv6 files) are opened as one
    dataset key with per-file ``variant`` labels.

    * ``databases`` given -> use exactly those files (``{"my-geoip2": "/path/x.mmdb"}``)
    * otherwise scan ``db_dir`` (package data dir, overridable with ``DETECTOR_DB_DIR``),
      plus any ``extra_dirs`` (typically the user cache dir where ``detector db update``
      drops newer files). Later directories win over the bundled copies of the same
      dataset key.
    * ``.mmdb.gz`` files are decompressed into the cache directory on demand
    * ``include`` / ``exclude`` filter by dataset key (``dbip-city``, ``iptoasn-asn``, ...)
    """
    manifest = load_manifest(db_dir)
    cache_path = Path(cache_dir).expanduser() if cache_dir else None
    opened: List[Database] = []

    if databases:
        for key, path in databases.items():
            opened.append(Database(path, key=key, manifest=manifest.get(key, {}), load_mode=load_mode))
        if not opened:
            raise NoDatabaseError("databases mapping is empty")
        return opened

    roots: List[Path] = []
    primary = resolve_data_dir(db_dir)
    if primary.is_dir():
        roots.append(primary)
    for extra in extra_dirs or ():
        extra_path = Path(extra).expanduser()
        if extra_path.is_dir() and extra_path != primary:
            roots.append(extra_path)

    if not roots:
        raise NoDatabaseError(
            "no database directory found. Check that the package data is installed, "
            "or pass Detector(databases={'city': '/path/GeoLite2-City.mmdb'})."
        )

    include_set = set(include) if include else None
    exclude_set = set(exclude or ())

    # Later roots override earlier ones for the same (dataset key, variant).
    # A dataset may consist of several files (GeoLite2-City ships separate IPv4
    # and IPv6 databases); they all share one key and differ by variant.
    chosen: Dict[Tuple[str, str], Tuple[Path, Optional[DatasetSpec], Optional[MemberSpec]]] = {}
    for root in roots:
        for path in discover_database_files(root):
            spec, member = member_for_filename(path.name)
            key = spec.key if spec else _strip_suffix(path.name)
            variant = member.variant if member else ""
            previous = chosen.get((key, variant))
            if previous is not None:
                # Parts of one split database all match the same member: keep the
                # first (part001), the merger walks the rest of the group anyway.
                if part_info(path.name) is not None and part_info(previous[0].name) is not None:
                    continue
            chosen[(key, variant)] = (path, spec, member)

    selected = [
        (key, variant, path, spec)
        for (key, variant), (path, spec, _member) in sorted(chosen.items())
        if (include_set is None or key in include_set) and key not in exclude_set
    ]

    # Decompress (and merge split parts) for every selected file in one batch:
    # the codecs release the GIL, so this parallelises across cores.
    if prepared is not None:
        # The caller (Preparation) unpacked the files; open exactly what is ready.
        pairs = [
            ((key, variant, path, spec), Path(prepared[logical_name(path.name)]))
            for (key, variant, path, spec) in selected
            if logical_name(path.name) in prepared
        ]
    elif extract and selected:
        try:
            resolved_all = extract_many([item[2] for item in selected], cache_path)
        except DatabaseError:
            if strict:
                raise
            resolved_all = [item[2] for item in selected]
        pairs = list(zip(selected, resolved_all))
    else:
        pairs = [(item, item[2]) for item in selected]

    for (key, variant, _path, spec), resolved in pairs:
        entry = manifest.get(manifest_key(key, variant), {})
        try:
            opened.append(
                Database(
                    resolved,
                    key=key,
                    name=(spec.name if spec else None),
                    variant=variant,
                    kind=(spec.kind if spec else None),
                    manifest=entry,
                    source_url=(spec.homepage if spec else None),
                    providers=(spec.providers if spec else ()),
                    license=(spec.license if spec else None),
                    attribution=(spec.attribution if spec else None),
                    load_mode=load_mode,
                )
            )
        except DatabaseError:
            if strict:
                raise
    if not opened:
        raise NoDatabaseError("every database failed to load")
    opened.sort(key=lambda db: (db.priority, db.info.key))
    return opened


def _strip_suffix(name: str) -> str:
    """``dbip-city-lite-2026-09.mmdb.gz`` -> ``dbip-city-lite-2026-09``.

    Split artefacts (``dbip-city-lite.mmdb.part002.xz``) strip down to the same
    stem, so a part matches the bundled member it belongs to.
    """
    logical = logical_name(name)
    if logical.endswith(".mmdb"):
        return logical[: -len(".mmdb")]
    return logical


def _matches(filename: str, bundled_name: str) -> bool:
    """``dbip-city-lite-2026-09.mmdb.gz`` matches bundled ``dbip-city-lite.mmdb.xz``."""
    return _strip_suffix(filename).startswith(_strip_suffix(bundled_name))


def describe_licenses(databases: Sequence[Database]) -> Dict[str, Any]:
    """Collect the compact database list and attribution strings for ``meta``."""
    seen: List[str] = []
    entries: List[Dict[str, Any]] = []
    for db in databases:
        entries.append(db.info.to_meta())
        text = (db.info.attribution or "").strip()
        if text and text not in seen:
            seen.append(text)
    return {
        "databases": entries,
        "attribution": " | ".join(seen),
    }


# --------------------------------------------------------------------------- #
# Record normalization: flatten any MMDB schema into one structure
# --------------------------------------------------------------------------- #

#: Keys consumed by the mapper. Everything else lands in ``traits``.
CONSUMED_KEYS = frozenset(
    {
        "continent",
        "country",
        "registered_country",
        "represented_country",
        "traits",
        "city",
        "location",
        "subdivisions",
        "postal",
        "autonomous_system_number",
        "autonomous_system_organization",
        "continent_code",
        "country_code",
        "country_iso_code",
        "country_name",
        "country_geoname_id",
        "is_in_european_union",
        "subdivision_name",
        "region",
        "region_name",
        "city_name",
        "latitude",
        "longitude",
        "postal_code",
        "zip_code",
        "asn",
        "as_number",
        "as_name",
        "organization",
        "isp",
        "asn_organization",
    }
)


@dataclass
class NormalizedRecord:
    """One MMDB record after normalization."""

    continent: Optional[Continent] = None
    country: Optional[Country] = None
    subdivisions: List[Subdivision] = field(default_factory=list)
    city: Optional[City] = None
    location: Optional[Location] = None
    postal: Optional[str] = None
    asn: Optional[ASN] = None
    #: All remaining vendor fields (MaxMind ``traits`` merged with leftovers).
    traits: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return not any(
            [
                self.continent,
                self.country,
                self.subdivisions,
                self.city,
                self.location and self.location.valid,
                self.asn and self.asn.found,
                self.traits,
            ]
        )

    def to_dict(self, *, locales: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return {
            "continent": self.continent.to_dict(locales=locales) if self.continent else None,
            "country": self.country.to_dict(locales=locales) if self.country else None,
            "subdivisions": [item.to_dict(locales=locales) for item in self.subdivisions],
            "city": self.city.to_dict(locales=locales) if self.city else None,
            "location": self.location.to_dict(locales=locales) if self.location else None,
            "postal": self.postal,
            "asn": self.asn.to_dict(locales=locales) if self.asn else None,
            "traits": dict(self.traits),
        }


def _names_of(container: Mapping[str, Any]) -> Dict[str, str]:
    """Copy a ``names`` sub-map, dropping empty/non-string entries."""
    names = container.get("names")
    if type(names) is dict and names:
        return {key: value for key, value in names.items() if type(value) is str and value}
    return {}


#: Flat keys used by the one-field community databases (fast path).
_FLAT_COUNTRY_KEYS = ("country_code", "country_iso_code")


def normalize_record(record: Mapping[str, Any]) -> NormalizedRecord:
    """Map MaxMind-style nested schemas, DB-IP schemas and flat variants.

    Written for speed: the community databases answer with one or two keys, so
    those hit an early-exit path, and the general path avoids helper-call and
    ``abc`` overhead on the hot loop (this runs once per database per lookup).
    Nothing is discarded - unrecognized keys are copied into ``traits``.
    """
    out = NormalizedRecord()

    # ---- fast paths for single-field records ----
    if len(record) == 1:
        key, value = next(iter(record.items()))
        if key in _FLAT_COUNTRY_KEYS:
            out.country = Country(iso_code=value if type(value) is str else _as_str(value))
            return out
        if key == "autonomous_system_number":
            out.asn = ASN(number=value if type(value) is int else _as_int(value))
            return out
        if key == "autonomous_system_organization":
            out.asn = ASN(organization=value if type(value) is str else _as_str(value))
            return out

    get = record.get
    traits = get("traits")
    if type(traits) is dict and traits:
        out.traits.update(traits)

    # ---- continent ----
    continent = get("continent")
    if type(continent) is dict:
        out.continent = Continent(
            names=_names_of(continent),
            geoname_id=_as_int(continent.get("geoname_id")),
            code=_as_str(continent.get("code")),
        )
    else:
        code = get("continent_code")
        if code:
            out.continent = Continent(code=code if type(code) is str else str(code))

    # ---- country ----
    country = get("country")
    if type(country) is not dict:
        country = get("registered_country")
    if type(country) is dict:
        is_in_eu = country.get("is_in_european_union")
        if is_in_eu is None:
            is_in_eu = get("is_in_european_union")
        out.country = Country(
            names=_names_of(country),
            geoname_id=_as_int(country.get("geoname_id")),
            iso_code=_as_str(country.get("iso_code") or country.get("code")),
            is_in_eu=_as_bool(is_in_eu),
        )
    else:
        iso = get("country_code") or get("country_iso_code")
        if iso:
            flat_name = get("country_name")
            out.country = Country(
                names={"en": flat_name} if type(flat_name) is str and flat_name else {},
                geoname_id=_as_int(get("country_geoname_id")),
                iso_code=iso if type(iso) is str else str(iso),
                is_in_eu=_as_bool(get("is_in_european_union")),
            )

    # ---- subdivisions ----
    subdivisions = get("subdivisions")
    if type(subdivisions) is list and subdivisions:
        for item in subdivisions:
            if type(item) is dict:
                out.subdivisions.append(
                    Subdivision(
                        names=_names_of(item),
                        geoname_id=_as_int(item.get("geoname_id")),
                        iso_code=_as_str(item.get("iso_code") or item.get("code")),
                    )
                )
    if not out.subdivisions:
        flat_sub = get("subdivision_name") or get("region_name") or get("region")
        if type(flat_sub) is str and flat_sub.strip():
            out.subdivisions.append(Subdivision(names={"en": flat_sub.strip()}))
        else:
            # flat schema splits state code / name into state1 / state2
            state_name = get("state2")
            state_code = get("state1")
            if (type(state_name) is str and state_name.strip()) or (
                type(state_code) is str and state_code.strip()
            ):
                names = {}
                if type(state_name) is str and state_name.strip():
                    names["en"] = state_name.strip()
                out.subdivisions.append(
                    Subdivision(
                        names=names,
                        iso_code=state_code.strip() if type(state_code) is str and state_code else None,
                    )
                )

    # ---- city ----
    city = get("city")
    if type(city) is dict:
        out.city = City(names=_names_of(city), geoname_id=_as_int(city.get("geoname_id")))
    elif type(city) is str and city.strip():
        # flat schema (ip-location-db's GeoLite2 builds): "city": "Mountain View"
        out.city = City(names={"en": city.strip()})
    else:
        flat_city = get("city_name")
        if type(flat_city) is str and flat_city.strip():
            out.city = City(names={"en": flat_city.strip()})

    # ---- coordinates ----
    location = get("location")
    latitude = get("latitude")
    longitude = get("longitude")
    if type(location) is dict or latitude is not None or longitude is not None:
        source = location if type(location) is dict else {}
        if latitude is None:
            latitude = source.get("latitude")
        if longitude is None:
            longitude = source.get("longitude")
        accuracy = source.get("accuracy_radius")
        time_zone = source.get("time_zone") or get("timezone") or get("time_zone")
        if type(time_zone) is not str:
            trait_zone = out.traits.get("time_zone") or out.traits.get("timezone")
            time_zone = trait_zone if type(trait_zone) is str else None
        out.location = Location(
            latitude=latitude if type(latitude) is float else _as_float(latitude),
            longitude=longitude if type(longitude) is float else _as_float(longitude),
            time_zone=time_zone,
            accuracy_radius=accuracy if type(accuracy) is int else _as_int(accuracy),
        )
        if not out.location.valid:  # coordinates parked in traits (older GeoIP2 shape)
            trait_lat = out.traits.get("latitude")
            trait_lon = out.traits.get("longitude")
            out.location.latitude = trait_lat if type(trait_lat) is float else _as_float(trait_lat)
            out.location.longitude = trait_lon if type(trait_lon) is float else _as_float(trait_lon)

    # ---- postal ----
    postal = get("postal")
    if type(postal) is dict:
        out.postal = _as_str(postal.get("code"))
    elif postal:
        out.postal = postal if type(postal) is str else str(postal)
    if not out.postal:
        flat_postal = get("postal_code") or get("zip_code") or get("postcode")
        if flat_postal:
            out.postal = flat_postal if type(flat_postal) is str else str(flat_postal)

    # ---- ASN ----
    number = get("autonomous_system_number")
    if number is None:
        number = get("asn")
    if number is None:
        number = get("as_number")
    if number is None:
        number = out.traits.get("autonomous_system_number")
    organization = get("autonomous_system_organization")
    if organization is None:
        organization = (
            get("as_name")
            or get("organization")
            or get("isp")
            or get("asn_organization")
            or out.traits.get("autonomous_system_organization")
        )
    if number is not None or organization:
        out.asn = ASN(
            number=None if type(number) is dict else (number if type(number) is int else _as_int(number)),
            organization=organization if type(organization) is str else _as_str(organization),
        )

    # ---- keep everything else ----
    for key, value in record.items():
        if key not in CONSUMED_KEYS:
            out.traits.setdefault(key, value)

    return out


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "y")


def sha256_of(path: os.PathLike, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()
