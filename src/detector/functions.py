"""The public API: two coroutines, each polymorphic over single and batch input.

    from detector import info, distance

    await info("8.8.8.8")                       # -> dict (the standard JSON document)
    await info(["8.8.8.8", "1.1.1.1"])          # -> [dict, dict]
    await info(generator_of_millions)           # -> [dict, ...]  windowed, bounded memory
    await info("8.8.8.8", as_object=True)       # -> IPInfo

    await distance("8.8.8.8", "1.1.1.1")                       # -> dict
    await distance("8.8.8.8", ["1.1.1.1", "::1"])              # -> [dict, dict]
    await distance(["8.8.8.8", "1.1.1.1"], ["::1", "9.9.9.9"]) # -> [dict, ...] (N x M)

Both entry points also accept the JSON envelope, so the same two functions cover
the protocol case::

    await info({"type": "ipv4", "action": "info", "data": {"ip": "8.8.8.8"}})      # -> dict
    await distance({"type": "ipv4", "action": "distance",
                    "data": {"ip": "8.8.8.8", "list": ["1.1.1.1"]}})               # -> dict
    # as_object=True gives the Response / IPInfo / Distance models instead

Everything configurable is passed per call, or once through :func:`configure`.
Clients are pooled per option set, so repeated calls with the same options reuse
one memory-mapped client instead of reopening the databases.

First use unpacks the bundled databases into the cache directory (76 MB of
compressed data -> 251 MB of MMDB files). Split archives are decoded in parallel,
and :func:`warmup` lets you pay that cost up front - at import time in a
container, or in the background while your application boots.
"""

from __future__ import annotations

import asyncio
import ipaddress
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .aio import AsyncDetector
from .distance import Distance
from .envelope import error_response, handle
from .exceptions import IPIntelError
from .models import IPInfo

__all__ = ["info", "distance", "warmup", "ready", "progress", "configure", "close"]

#: Scalar inputs. Any other iterable is treated as a batch.
_SCALARS = (
    str,
    bytes,
    bytearray,
    int,
    ipaddress.IPv4Address,
    ipaddress.IPv6Address,
    IPInfo,
)

_clients: Dict[Tuple[Any, ...], AsyncDetector] = {}
_defaults: Dict[str, Any] = {}
_lock = threading.Lock()

#: One :class:`~detector.preparation.Preparation` per cache directory: ``warmup()``,
#: ``ready()``, ``progress()`` and the import-time kick-off all share it.
_preparations: Dict[str, Any] = {}
_prep_lock = threading.Lock()


def _preparation(cache_dir: Optional[str] = None, datasets: Optional[Iterable[str]] = None) -> Any:
    """The shared preparation for this cache directory (same instance a client uses)."""
    from .databases import user_cache_dir
    from .preparation import shared_preparation

    resolved = Path(cache_dir).expanduser() if cache_dir else user_cache_dir()
    # Same identity as Detector builds, so warmup()/ready()/progress() observe the
    # very same background preparation the client uses.
    return shared_preparation(
        cache_dir=resolved,
        datasets=list(datasets) if datasets else None,
        extra_dirs=[resolved],
        extract=True,
    )




def _env_defaults() -> Dict[str, Any]:
    """Environment overrides applied to every call: DETECTOR_WAIT_TIMEOUT, ..."""
    import os

    out: Dict[str, Any] = {}
    timeout = os.environ.get("DETECTOR_WAIT_TIMEOUT")
    if timeout:
        try:
            out["wait_timeout"] = float(timeout)
        except ValueError:
            pass
    on_timeout = os.environ.get("DETECTOR_ON_TIMEOUT")
    if on_timeout:
        out["on_timeout"] = on_timeout.strip().lower()
    return out


def configure(**options: Any) -> None:
    """Set defaults for every later call.

    Options are :class:`~detector.api.Detector` constructor arguments::

        configure(datasets=["dbip-city", "dbip-asn"], locales=("zh-CN", "en"),
                  include_raw=False, cache_size=8192, max_concurrency=64)

    Per-call keyword arguments override these. Pooled clients are dropped so the
    new defaults take effect immediately.
    """
    with _lock:
        _defaults.clear()
        _defaults.update(options)
        _clients.clear()


async def close() -> None:
    """Close every pooled client (optional: scripts can just exit)."""
    with _lock:
        clients = list(_clients.values())
        _clients.clear()
    for client in clients:
        try:
            await client.aclose()
        except Exception:  # pragma: no cover - closing must never raise
            pass


async def warmup(
    *,
    datasets: Optional[Iterable[str]] = None,
    cache_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Unpack the bundled databases now instead of on the first query.

    Unpacking is unavoidable once per machine (~90 MB of parts -> 251 MB of MMDB
    files). It is *progressive*: the cheapest databases (country/ASN) become
    queryable in a fraction of a second while the big city file is still being
    written, and the actual work happens in a background thread.

    This coroutine starts that work (if it is not running yet) and waits for all
    of it, so it belongs in a container build step, a start-up hook, or a
    background task::

        await warmup()                                 # everything bundled
        await warmup(datasets=["dbip-city", "dbip-asn"])   # only these

    Returns a report::

        {"files": 12, "parts": 20, "bytes": 250600000, "seconds": 10.8,
         "cache_dir": "/root/.cache/detector/extracted", "already_ready": False}
    """
    return await asyncio.to_thread(_warmup_sync, datasets, cache_dir)


def _warmup_sync(
    datasets: Optional[Iterable[str]] = None,
    cache_dir: Optional[str] = None,
) -> Dict[str, Any]:
    import time

    prep = _preparation(cache_dir, datasets)
    started = time.perf_counter()
    already = prep.finished or prep.is_ready()
    prep.wait(None)
    elapsed = time.perf_counter() - started
    progress = prep.progress()
    return {
        "files": progress["ready"],
        "parts": sum(1 for unit in prep._units if len(unit.paths) > 1),
        "bytes": sum(
            unit.resolved.stat().st_size for unit in prep._units if unit.resolved is not None
        ),
        "seconds": round(elapsed if not already else 0.0, 3),
        "cache_dir": progress["cache_dir"],
        "already_ready": bool(already),
        "failed": progress["failed"],
    }


def _lazy_bundled() -> int:
    """Number of lazy (``.bz``) datasets shipped; 0 when the data is archived."""
    from .databases import package_data_dir

    try:
        return sum(1 for item in package_data_dir().glob("*.mmdb.bz"))
    except OSError:  # pragma: no cover
        return 0


async def ready(timeout: Optional[float] = None, *, wait: str = "all", **options: Any) -> bool:
    """Is the database set ready to answer complete queries?

    ``await ready()`` blocks until everything is unpacked (lazy ``.bz`` data is
    ready the moment the client opens, so this returns immediately); ``await
    ready(2.0)`` is a bounded poll; ``wait="any"`` returns as soon as the first
    database is usable. Never raises: failures surface in :func:`progress`.
    """
    prep = _preparation(options.get("cache_dir"), options.get("datasets"))
    if prep.total == 0 and _lazy_bundled():
        return True
    return await asyncio.to_thread(prep.wait_for_any if wait == "any" else prep.wait, timeout)


async def progress(**options: Any) -> Dict[str, Any]:
    """Snapshot of the preparation: readiness, per-database timings, failures.

    For lazily-shipped data (``.bz`` containers) there is nothing to unpack: the
    report reflects the ready bundled set immediately.
    """
    prep = _preparation(options.get("cache_dir"), options.get("datasets"))
    lazy = _lazy_bundled()
    if prep.total == 0 and lazy:
        return {
            "ready": lazy, "total": lazy, "loading": False, "complete": True,
            "failed": {}, "pending": [], "timings": {}, "seconds": 0.0,
            "cache_dir": str(prep.cache_dir),
        }
    snapshot = dict(prep.progress())
    return snapshot

def _is_single(value: Any) -> bool:
    return isinstance(value, _SCALARS) or not hasattr(value, "__iter__")


def _looks_like_json(value: Any) -> bool:
    """A string/bytes payload that starts like a JSON object or array."""
    if not isinstance(value, (str, bytes, bytearray)):
        return False
    raw = value if isinstance(value, (bytes, bytearray)) else value.encode()
    return bytes(raw).lstrip()[:1] in (b"{", b"[")


def _is_envelope(value: Any) -> bool:
    """True when the caller passed protocol JSON instead of IP addresses.

    Objects/lists of objects are envelopes; so is a JSON string. Everything else
    is an address (or a batch of addresses).
    """
    if isinstance(value, Mapping):
        return any(key in value for key in ("action", "data", "status"))
    if isinstance(value, (list, tuple)):
        return bool(value) and isinstance(value[0], Mapping)
    return _looks_like_json(value)


def _hashable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((str(key), _hashable(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


async def _client(**options: Any) -> AsyncDetector:
    """A live client for this option set, created and pooled on demand."""
    merged = {**_env_defaults(), **_defaults, **options}
    key = tuple(sorted((name, _hashable(value)) for name, value in merged.items()))
    with _lock:
        client = _clients.get(key)
    if client is not None and not client._closed:
        return client
    client = await AsyncDetector.create(**merged)
    with _lock:
        _clients[key] = client
    return client


def _out(value: Any, as_object: bool) -> Any:
    """Default output is plain JSON-ready data; ``as_object=True`` keeps models."""
    if as_object:
        return value
    if isinstance(value, (list, tuple)):
        return [_out(item, False) for item in value]
    to_dict = getattr(value, "to_dict", None)
    return to_dict() if callable(to_dict) else value


def _protocol_payload(value: Any) -> Any:
    from .envelope import loads

    if isinstance(value, (bytes, bytearray)):
        return loads(bytes(value))
    if isinstance(value, str):
        return loads(value)
    return value


async def info(target: Any, *, as_object: bool = False, **options: Any) -> Any:
    """Information for one IP or a batch, merged from every loaded database.

    Returns **the standard JSON document** (a plain ``dict``) by default:

    * single IP-like value -> ``dict``
    * iterable / generator  -> ``list[dict]`` (input order preserved)
    * JSON envelope        -> ``dict`` (the protocol response document)

    Pass ``as_object=True`` to get the models instead: ``IPInfo``,
    ``list[IPInfo]``, or :class:`~detector.envelope.Response`.
    """
    if _is_envelope(target):
        payload = _protocol_payload(target)
        try:
            response = handle((await _client(**options)).sync, payload)
        except IPIntelError as exc:
            # Databases not ready yet (load timeout) or failed to open: keep the
            # envelope contract and answer with status="error" instead of raising.
            response = error_response(exc, payload)
        return _out(response, as_object)

    client = await _client(**options)
    if _is_single(target):
        return _out(await client.lookup(target), as_object)

    return _out([item async for item in client.stream(_as_iterable(target))], as_object)


async def distance(
    source: Any,
    targets: Any = None,
    *,
    as_object: bool = False,
    method: Optional[str] = None,
    **options: Any,
) -> Any:
    """Distance between IPs, computed from each side's merged geolocation.

    Returns the standard JSON document (a plain ``dict``) by default:

    * single -> single : ``dict``
    * single -> N      : ``list[dict]`` (N unbounded, generators accepted)
    * N      -> M      : ``list[dict]`` (full product)
    * single JSON envelope (as ``source``) -> ``dict`` (protocol response)

    Pass ``as_object=True`` for ``Distance`` / ``list[Distance]`` / ``Response``.
    ``method`` selects the maths: ``"haversine"`` (default) or ``"vincenty"``.
    """
    if _is_envelope(source):
        payload = _protocol_payload(source)
        try:
            response = handle((await _client(**options)).sync, payload)
        except IPIntelError as exc:
            response = error_response(exc, payload)
        return _out(response, as_object)

    client = await _client(**options)
    if _is_single(source) and _is_single(targets):
        return _out(await client.distance(source, targets, method=method), as_object)

    if _is_single(source):
        rows = await client.distance(source, _as_iterable(targets), method=method)
    else:
        rows = await client.distance_many(
            _as_iterable(source), _as_iterable(targets), method=method
        )
    results: List[Distance] = [rows] if isinstance(rows, Distance) else list(rows)
    return _out(results, as_object)


def _as_iterable(value: Any) -> Iterable[Any]:
    """Normalise a batch input to something iterable (generators pass through)."""
    if isinstance(value, (bytes, bytearray)):
        return [bytes(value)]
    return value
