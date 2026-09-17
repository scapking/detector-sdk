"""detector - offline IP intelligence SDK.

Two coroutines do the work, and each one takes either a single IP or a batch::

    from detector import info, distance

    await info("8.8.8.8")                        # IPInfo
    await info(["8.8.8.8", "1.1.1.1"])           # [IPInfo, IPInfo]
    await info(generator)                        # [IPInfo, ...]  (bounded memory)
    await info("8.8.8.8", as_dict=True)          # the standard JSON document

    await distance("8.8.8.8", "1.1.1.1")                 # Distance
    await distance("8.8.8.8", ["1.1.1.1", "::1"])        # [Distance]
    await distance(["8.8.8.8"], ["::1", "9.9.9.9"])      # [Distance]  (N x M)

Both also accept the JSON envelope::

    await info({"type": "ipv4", "action": "info", "data": {"ip": "8.8.8.8"}})

Options are per call, or set once::

    from detector import configure
    configure(datasets=["dbip-city", "dbip-asn"], locales=("en",), include_raw=False)

Everything bundled: 11 datasets, no network, no API keys. Import name is
``detector``; the PyPI distribution is ``detector-sdk``.
"""

from __future__ import annotations

from ._version import __version__
from .aio import AsyncDetector
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
from .distance import EARTH_RADIUS_KM, KM_PER_MILE, METHODS, Distance
from .distance import distance_between as distance_between  # importable helper
from .distance import haversine_km as haversine_km  # importable helper
from .distance import vincenty_km as vincenty_km  # importable helper
from .envelope import Request, Response
from .envelope import loads as loads  # importable on purpose, not advertised in __all__
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
from .functions import close, configure, distance, info
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
)
from .models import pick_name as pick_name  # importable on purpose, not advertised in __all__
from .update import known_datasets
from .update import read_manifest as read_manifest  # importable on purpose, not advertised in __all__
from .update import (
    update_datasets_async as update_datasets,
)
from .update import write_manifest as write_manifest  # importable on purpose, not advertised in __all__

__all__ = [
    "__version__",
    # the two capabilities
    "info",
    "distance",
    # configuration and lifecycle
    "configure",
    "close",
    # clients (for explicit control)
    "Detector",
    "AsyncDetector",
    # results
    "IPInfo",
    "Distance",
    "Place",
    "City",
    "Country",
    "Continent",
    "Subdivision",
    "Location",
    "ASN",
    "JsonModel",
    "SCHEMA_VERSION",
    "DEFAULT_LOCALES",
    "CROSS_CHECK_FIELDS",
    # protocol objects
    "Request",
    "Response",
    # datasets
    "known_datasets",
    "update_datasets",
    "BUILTIN_DATASETS",
    "BUNDLED_KEYS",
    "OPTIONAL_KEYS",
    "NON_REDISTRIBUTABLE_KEYS",
    "DATABASE_PRIORITY",
    "DEFAULT_ATTRIBUTION",
    "Database",
    "DatabaseInfo",
    "DatasetSpec",
    "MemberSpec",
    "SourceSpec",
    # helpers
    "IPLike",
    "EARTH_RADIUS_KM",
    "KM_PER_MILE",
    "METHODS",
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
