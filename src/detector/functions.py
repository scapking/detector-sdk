"""The public API: two coroutines, each polymorphic over single and batch input.

    from detector import info, distance

    await info("8.8.8.8")                       # -> IPInfo
    await info(["8.8.8.8", "1.1.1.1"])          # -> [IPInfo, IPInfo]
    await info(generator_of_millions)           # -> [IPInfo, ...]  windowed, bounded memory
    await info("8.8.8.8", as_dict=True)         # -> dict (the standard JSON document)

    await distance("8.8.8.8", "1.1.1.1")                       # -> Distance
    await distance("8.8.8.8", ["1.1.1.1", "::1"])              # -> [Distance]
    await distance(["8.8.8.8", "1.1.1.1"], ["::1", "9.9.9.9"]) # -> [Distance] (N x M)

Both entry points also accept the JSON envelope, so the same two functions cover
the protocol case::

    await info({"type": "ipv4", "action": "info", "data": {"ip": "8.8.8.8"}})      # -> dict
    await distance({"type": "ipv4", "action": "distance",
                    "data": {"ip": "8.8.8.8", "list": ["1.1.1.1"]}})               # -> dict
    # as_object=True gives the Response / IPInfo / Distance models instead

Everything configurable is passed per call, or once through :func:`configure`.
Clients are pooled per option set, so repeated calls with the same options reuse
one memory-mapped client instead of reopening the databases.
"""

from __future__ import annotations

import ipaddress
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .aio import AsyncDetector
from .distance import Distance
from .models import IPInfo

__all__ = ["info", "distance", "configure", "close"]

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
    merged = {**_defaults, **options}
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
        from .envelope import handle

        response = handle((await _client(**options)).sync, _protocol_payload(target))
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
        from .envelope import handle

        response = handle((await _client(**options)).sync, _protocol_payload(source))
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
