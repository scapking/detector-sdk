# 02 - API reference

Everything below is importable straight from the package root.

```python
from detector import (
    Detector, AsyncDetector, IPGeo, AsyncIPGeo,     # clients (IPGeo = alias)
    lookup, lookup_many, stream, distance, distance_many, nearest,
    request, request_json, configure, get_default_geo, set_default_geo,
    alookup, alookup_many, astream, adistance, adistance_many, anearest,
    arequest, arequest_json, aget_default_geo, aset_default_geo, configure_async,
    IPInfo, Distance, Place, City, Country, Continent, Subdivision, Location, ASN,
    Request, Response, parse_ip, pick_name,
    known_datasets, update_datasets, update_datasets_async,
    download_dataset, download_dataset_async, write_manifest, read_manifest,
    haversine_km, vincenty_km, distance_between, METHODS,
    InvalidIPError, NoDatabaseError, DownloadError, ProtocolError, ...,
)
```

## Detector

```python
Detector(
    databases=None,            # {"my-db": "/path/x.mmdb"} -> use exactly these files
    db_dir=None,               # directory scanned for *.mmdb / *.mmdb.gz
    cache_dir=None,            # decompression + refreshed datasets
    datasets=None,             # ["dbip-city", "iptoasn-asn"] -> load only these keys
    exclude=None,              # keys to skip
    extra_dirs=None,           # additional directories scanned after db_dir
    locales=("en",),           # name priority; names maps always carry every language
    distance_method="haversine",
    cache_size=4096,           # LRU entries for lookup results; 0 disables
    load_mode="auto",          # auto | mmap | memory
    extract=True,              # decompress .gz into the cache
    strict=False,              # True -> batch helpers raise instead of embedding errors
    resolve_dns=False,         # opt-in reverse DNS per lookup
    workers=None,              # thread pool size for parallel=True
    include_raw=True,          # keep every database's full record in the result
    include_cross_check=True,  # build cross_check/agreement/conflicts
    include_all_names=True,    # False -> names maps are trimmed to `locales`
)
```

Methods:

| method | notes |
|---|---|
| `lookup(ip, *, locales=None, raise_on_missing=None, use_cache=True, resolve_dns=None, include_raw=None)` | one merged `IPInfo`; missing data is not an error |
| `lookup_many(ips, *, workers=None, locales=None, ignore_errors=True, parallel=False, processes=0, threshold=64)` | batch, order preserved |
| `stream(ips, *, locales=None)` | lazy generator, constant memory |
| `distance(source, targets, *, method=None, locales=None)` | `Distance` for one target, `list[Distance]` for many |
| `distance_many(sources, targets, *, method=None)` | N x M product |
| `nearest(source, targets, *, limit=5, max_km=None, method=None)` | ascending by distance |
| `request(payload)` / `handle(payload)` / `handle_json(payload)` | JSON protocol |
| `describe()` / `stats()` / `datasets` / `dataset_keys` | metadata and diagnostics |
| `available_datasets()` | static: the whole dataset registry |
| `close()` / context manager | releases mmaps and the thread pool |

Error contract: malformed addresses always raise `InvalidIPError`. *Missing data*
never raises unless you ask for it with `raise_on_missing=True`. Batch calls
report per-row problems in `info.meta["error"]`.

## AsyncDetector

Same surface, `await`ed, plus:

| member | notes |
|---|---|
| `await AsyncDetector.create(**kwargs)` | opens databases off the event loop |
| `await lookup_many(ips, *, concurrency=None, window=1024, processes=0)` | windowed concurrency, bounded memory |
| `stream(ips)` | async generator; accepts sync **and** async iterables |
| `await update_datasets(target_dir=None, *, datasets, concurrency=4)` | concurrent downloads |
| `async with` / `await aclose()` | lifecycle |
| `.sync` | the underlying synchronous `Detector` |

MMAP lookups run in a thread pool, so the event loop keeps running. Network
transfers are native asyncio sockets (see `detector/net.py`).

## Module-level helpers

Sync: `lookup`, `lookup_many`, `stream`, `distance`, `distance_many`, `nearest`,
`request`, `request_json`.
Async: `alookup`, `alookup_many`, `astream`, `adistance`, `adistance_many`,
`anearest`, `arequest`, `arequest_json`.

Both families share one lazily created client per process. Re-configure with
`configure(**kwargs)` / `configure_async(**kwargs)`, or inject your own with
`set_default_geo(detector)` / `aset_default_geo(async_detector)`.

## IPInfo

Attributes: `ip`, `version`, `found`, `network`, `networks`, `is_private`,
`is_global`, `is_loopback`, `is_reserved`, `is_multicast`, `is_unspecified`,
`continent`, `country`, `subdivisions`, `city`, `location`, `postal`,
`time_zone`, `asn`, `traits`, `hostname`, `sources`, `cross_check`, `agreement`,
`conflicts`, `raw`, `meta`.

Shortcuts: `country_code`, `continent_code`, `coordinates`, `asn_number`,
`asn_organization`, `city_name`, `country_name`, `display`,
`localized_name(locales)`.

Serialization: `to_dict(locales=None, all_names=None)`,
`to_json(indent=None, ensure_ascii=True, ...)`, `to_flat_dict()`,
`get("country.iso_code")`.

`Place`/`Country`/`Continent`/`Subdivision` expose `name(locales)`, `names`,
`geoname_id` (plus `iso_code`, `is_in_eu`, `is_eu`, `code` where applicable).
`Location` adds `valid` and `coordinates`. `ASN` adds `asn` (`"AS15169"`) and `found`.

## Distance

Fields: `source`, `target`, `km`, `mi`, `method`, `same_country`, `same_city`,
`same_continent`, `same_asn`, `source_location`, `target_location`, `reason`.
Helpers: `available`, `km_int`, `float(row)`, `str(row)`, `row.to_dict()`,
`Distance.between(info_a, info_b)`.

Math helpers: `haversine_km(lat1, lon1, lat2, lon2)`, `vincenty_km(...)`,
`distance_between(info_a, info_b, method=...)`, `METHODS`.

## Dataset helpers

```python
known_datasets()                 # registry: name, kind, licence, cadence, mirrors, bundled
update_datasets(dir, datasets=..., source="db-ip"|"sapics", progress=fn)  # blocking
await update_datasets_async(dir, datasets=..., concurrency=4)            # concurrent
download_dataset(key, dir) / await download_dataset_async(key, dir)
write_manifest(dir, keys) / read_manifest(dir)
```

## Protocol objects

`Request(type, action, data, status)` with `from_dict` / `from_json`,
`resolved_action`, `resolved_type`.
`Response(type, action, status, data, error, meta)` with `ok`,
`to_dict()`, `to_json()`.
