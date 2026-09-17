"""detector - offline IP intelligence SDK.

One query returns every field of every bundled database, merged into a single
standardised English JSON document; distance between IPs is one call away.

Sync::

    from detector import lookup, distance

    lookup("8.8.8.8").to_dict()                       # all databases, one record
    distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])     # 1 to N -> list[Distance]

Async (same surface, ``a``-prefixed)::

    from detector import alookup, adistance, AsyncDetector

    info = await alookup("8.8.8.8")
    dist = await adistance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])

    async with await AsyncDetector.create(locales=("en",)) as detector:
        async for info in detector.stream(generator_of_millions):
            ...

JSON protocol (``{"type","action","data","status"}`` in, standard JSON out)::

    from detector import request_json
    request_json('{"type":"ipv4","action":"distance","data":{"ip":"8.8.8.8","list":["1.1.1.1"]}}')
"""

from __future__ import annotations

import threading
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Union

from ._version import __version__
from .aio import (
    AsyncDetector,
    adistance,
    adistance_many,
    aget_default_geo,
    alookup,
    alookup_many,
    anearest,
    arequest,
    arequest_json,
    aset_default_geo,
    astream,
    configure_async,
)
from .api import CROSS_CHECK_FIELDS, Detector, IPLike, parse_ip
from .databases import (
    BUILTIN_DATASETS,
    BUNDLED_KEYS,
    DATABASE_PRIORITY,
    DEFAULT_ATTRIBUTION,
    NON_REDISTRIBUTABLE_KEYS,
    OPTIONAL_KEYS,
    Database,
    DatabaseInfo,
    DatasetSpec,
    MemberSpec,
    SourceSpec,
)
from .distance import (
    EARTH_RADIUS_KM,
    KM_PER_MILE,
    METHODS,
    Distance,
    distance_between,
    haversine_km,
    vincenty_km,
)
from .envelope import ACTION_ALIASES, TYPE_ALIASES, Request, Response, loads
from .exceptions import (
    DatabaseError,
    DatabaseNotFoundError,
    DistanceUnavailableError,
    DownloadError,
    InvalidIPError,
    IPIntelError,
    NoDatabaseError,
    ProtocolError,
    UnsupportedActionError,
)
from .models import (
    ASN,
    DEFAULT_LOCALES,
    SCHEMA_VERSION,
    City,
    Continent,
    Country,
    IPInfo,
    JsonModel,
    Location,
    Place,
    Subdivision,
    pick_name,
)
from .update import (
    DEFAULT_UPDATE_KEYS,
    download_dataset,
    download_dataset_async,
    known_datasets,
    member_stem,
    read_manifest,
    update_datasets,
    update_datasets_async,
    write_manifest,
)

#: Alias kept for the "geoip2-style" mental model.
IPGeo = Detector
AsyncIPGeo = AsyncDetector

__all__ = [
    "__version__",
    # clients
    "Detector",
    "AsyncDetector",
    "IPGeo",
    "AsyncIPGeo",
    "IPLike",
    "parse_ip",
    # sync helpers
    "lookup",
    "lookup_many",
    "stream",
    "distance",
    "distance_many",
    "nearest",
    "request",
    "request_json",
    "configure",
    "get_default_geo",
    "set_default_geo",
    # async helpers
    "alookup",
    "alookup_many",
    "astream",
    "adistance",
    "adistance_many",
    "anearest",
    "arequest",
    "arequest_json",
    "aget_default_geo",
    "aset_default_geo",
    "configure_async",
    # models
    "IPInfo",
    "Place",
    "City",
    "Country",
    "Continent",
    "Subdivision",
    "Location",
    "ASN",
    "JsonModel",
    "Distance",
    "pick_name",
    "SCHEMA_VERSION",
    "DEFAULT_LOCALES",
    "CROSS_CHECK_FIELDS",
    # protocol
    "Request",
    "Response",
    "loads",
    "ACTION_ALIASES",
    "TYPE_ALIASES",
    # math
    "haversine_km",
    "vincenty_km",
    "distance_between",
    "EARTH_RADIUS_KM",
    "KM_PER_MILE",
    "METHODS",
    # datasets
    "BUILTIN_DATASETS",
    "BUNDLED_KEYS",
    "OPTIONAL_KEYS",
    "NON_REDISTRIBUTABLE_KEYS",
    "DATABASE_PRIORITY",
    "DatasetSpec",
    "MemberSpec",
    "SourceSpec",
    "member_stem",
    "DEFAULT_ATTRIBUTION",
    "DEFAULT_UPDATE_KEYS",
    "Database",
    "DatabaseInfo",
    "known_datasets",
    "update_datasets",
    "update_datasets_async",
    "download_dataset",
    "download_dataset_async",
    "write_manifest",
    "read_manifest",
    # errors
    "IPIntelError",
    "InvalidIPError",
    "DatabaseError",
    "DatabaseNotFoundError",
    "NoDatabaseError",
    "DistanceUnavailableError",
    "UnsupportedActionError",
    "ProtocolError",
    "DownloadError",
]

_default_geo: Optional[Detector] = None
_default_lock = threading.Lock()
_default_kwargs: Dict[str, Any] = {}


def configure(**kwargs: Any) -> Detector:
    """(Re)build the process-wide default client.

    ::

        import detector
        detector.configure(locales=("zh-CN",), distance_method="vincenty", cache_size=0)
    """
    global _default_geo, _default_kwargs
    with _default_lock:
        _default_kwargs = dict(kwargs)
        if _default_geo is not None:
            _default_geo.close()
        _default_geo = Detector(**_default_kwargs)
        return _default_geo


def get_default_geo() -> Detector:
    """Return (creating on first use) the default client."""
    global _default_geo
    if _default_geo is None:
        with _default_lock:
            if _default_geo is None:
                _default_geo = Detector(**_default_kwargs)
    return _default_geo


def set_default_geo(detector: Optional[Detector]) -> None:
    """Inject your own client (e.g. one holding licensed GeoIP2 files)."""
    global _default_geo
    with _default_lock:
        _default_geo = detector


# --------------------------------------------------------------------------- #
# Module level helpers: `from detector import lookup, distance` and go
# --------------------------------------------------------------------------- #


def lookup(
    ip: IPLike,
    *,
    locales: Optional[Sequence[str]] = None,
    raise_on_missing: Optional[bool] = None,
    use_cache: bool = True,
) -> IPInfo:
    return get_default_geo().lookup(
        ip, locales=locales, raise_on_missing=raise_on_missing, use_cache=use_cache
    )


def lookup_many(
    ips: Iterable[IPLike],
    *,
    locales: Optional[Sequence[str]] = None,
    ignore_errors: bool = True,
) -> List[IPInfo]:
    return get_default_geo().lookup_many(ips, locales=locales, ignore_errors=ignore_errors)


def stream(ips: Iterable[IPLike], *, locales: Optional[Sequence[str]] = None) -> Iterator[IPInfo]:
    return get_default_geo().stream(ips, locales=locales)


def distance(
    source: IPLike,
    targets: Union[IPLike, Iterable[IPLike]],
    *,
    method: Optional[str] = None,
    locales: Optional[Sequence[str]] = None,
) -> Union[Distance, List[Distance]]:
    return get_default_geo().distance(source, targets, method=method, locales=locales)


def distance_many(
    sources: Iterable[IPLike],
    targets: Iterable[IPLike],
    *,
    method: Optional[str] = None,
) -> List[Distance]:
    return get_default_geo().distance_many(sources, targets, method=method)


def nearest(
    source: IPLike,
    targets: Iterable[IPLike],
    *,
    limit: int = 5,
    max_km: Optional[float] = None,
    method: Optional[str] = None,
) -> List[Distance]:
    return get_default_geo().nearest(source, targets, limit=limit, max_km=max_km, method=method)


def request(payload: Any) -> Response:
    """Protocol entry: dict / JSON string / Request -> Response."""
    return get_default_geo().request(payload)


def request_json(payload: Union[str, bytes, Mapping[str, Any]]) -> str:
    """Protocol entry: JSON in, JSON out."""
    return get_default_geo().handle_json(payload)




