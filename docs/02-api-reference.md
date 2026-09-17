# 02 - API reference

Two coroutines do all the work. Everything else is configuration, data models or
error types.

```python
from detector import info, distance, configure, close
```

## info(target, *, as_dict=False, **options)

Information for one address or a batch, merged from every loaded database.
Dispatches on the input type:

| target | returns |
|---|---|
| single IP-like value (`str`, `bytes`, `int`, `IPv4Address`, `IPv6Address`, `IPInfo`) | `IPInfo` |
| iterable / generator of the above | `list[IPInfo]`, input order preserved |
| async iterable | `list[IPInfo]` |
| JSON envelope (`dict`, list of dicts, JSON string) | `Response` |

`as_dict=True` returns plain dictionaries instead of model objects.

## distance(source, targets=None, *, as_dict=False, method=None, **options)

| source | targets | returns |
|---|---|---|
| single | single | `Distance` |
| single | iterable / generator | `list[Distance]` (N unbounded) |
| iterable | iterable | `list[Distance]` (full N x M product) |
| single JSON envelope | - | `Response` |

`method` selects the maths: `"haversine"` (default, ~1 us) or `"vincenty"`
(WGS84 ellipsoid, ~30 us). `as_dict=True` returns dictionaries.

Rows that cannot be computed come back with `km=None` and a `reason`
(`no_location`, `invalid_ip`, `not_found`, `compute_failed`), so batch output
stays aligned with the input.

## configure(**options) / close()

`configure` sets defaults for every later call and drops pooled clients.
`close` (async) releases them. Options are `Detector` constructor arguments:

| option | meaning |
|---|---|
| `databases={"key": "/path/x.mmdb"}` | use exactly these files instead of the bundled set |
| `db_dir="/data"` | directory scanned for `*.mmdb` / `*.mmdb.gz` / `*.mmdb.xz` |
| `cache_dir="/tmp/detector"` | where archives are decompressed, where updates land |
| `datasets=["dbip-city", ...]` | load only these dataset keys |
| `exclude=["geolite2-city", ...]` | skip these keys |
| `extra_dirs=["/opt/extra"]` | additional directories scanned after `db_dir` |
| `locales=("en",)` | name priority for `name` / `display` |
| `distance_method="haversine"` | default maths for `distance()` |
| `cache_size=4096` | LRU entries for lookup results (`0` disables) |
| `load_mode="auto"` | `auto` / `mmap` / `memory` |
| `include_raw=True` | keep every database's full record in the result |
| `include_cross_check=True` | build `cross_check` / `agreement` / `conflicts` |
| `include_all_names=True` | `False` trims `names` maps to `locales` |
| `strict=False` | batch helpers raise instead of embedding errors |
| `resolve_dns=False` | opt-in reverse DNS per lookup |
| `max_concurrency=32`, `window=1024` | async batch tuning |

## Result objects

`IPInfo` (returned by `info`)

Attributes: `ip`, `version`, `found`, `network`, `networks`, `is_private`,
`is_global`, `is_loopback`, `is_reserved`, `is_multicast`, `is_unspecified`,
`continent`, `country`, `subdivisions`, `city`, `location`, `postal`,
`time_zone`, `asn`, `traits`, `hostname`, `sources`, `cross_check`,
`agreement`, `conflicts`, `raw`, `meta`.

Shortcuts: `country_code`, `continent_code`, `coordinates`, `asn_number`,
`asn_organization`, `city_name`, `country_name`, `display`,
`localized_name(locales)`.

Serialisation: `to_dict(locales=None, all_names=None)`, `to_json(indent=None)`,
`to_flat_dict()`, `get("country.iso_code")`, `info["country"]`.

`Distance` (returned by `distance`)

Fields: `source`, `target`, `km`, `mi`, `method`, `same_country`, `same_city`,
`same_continent`, `same_asn`, `source_location`, `target_location`, `reason`.
Helpers: `available`, `km_int`, `float(row)`, `str(row)`, `row.to_dict()`,
`row.to_json()`.

Nested models: `Place` / `City` / `Country` / `Continent` / `Subdivision`
(`name(locales)`, `names`, `geoname_id`, plus `iso_code`, `is_in_eu`, `code`),
`Location` (`valid`, `coordinates`), `ASN` (`asn`, `found`).

## Explicit clients

The pooled clients are fine for most work. Reach for a client object when you
need members that only exist there:

```python
from detector import Detector, AsyncDetector

async with await AsyncDetector.create(max_concurrency=64, window=1024) as client:
    await client.lookup(ip)                 # single, blocking-in-executor
    await client.lookup_many(ips)           # batch
    client.stream(ips)                      # async generator (sync or async source)
    await client.distance(a, b)             # Distance or list[Distance]
    await client.distance_many(a_list, b_list)
    await client.nearest(src, candidates, limit=5, max_km=None)
    await client.request(envelope)          # -> Response
    await client.describe()                 # database inventory + licences
    await client.stats()                    # counts + cache hit rate
    await client.update_datasets(["dbip-city"], concurrency=4)
```

`Detector` (synchronous) exposes the same members without `await`, for scripts
that prefer not to use asyncio.

## Data helpers

```python
from detector import known_datasets, update_datasets

known_datasets()                 # 11 datasets: name, kind, licence, files, cadence,
                                 # sources, bundled, redistributable
await update_datasets(datasets=["dbip-city"], concurrency=4)   # refresh into the cache
```

## Errors

All derive from `IPIntelError` with a stable `code`: `InvalidIPError`
(`invalid_ip`), `NoDatabaseError`, `DatabaseNotFoundError`,
`DistanceUnavailableError`, `ProtocolError`, `UnsupportedActionError`,
`DownloadError`.

Malformed addresses raise `InvalidIPError`; missing data does not (it returns
`found=False`); envelopes never raise - they return `status="error"` with the
code in `error`.
