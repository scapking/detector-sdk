# detector

Offline IP intelligence SDK for Python 3.9+.

One call returns **every field of every bundled database**, merged into a single
standardised English document — no API keys, no network, no extra downloads.
Distance between IPs is one call away, 1-to-1 or 1-to-N (N unbounded).

```python
from detector import lookup, distance

lookup("8.8.8.8").to_dict()                     # all databases, one merged record
distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])   # -> [Distance, Distance]
```

* **11 datasets bundled** (every dataset ip-location-db publishes), 76 MB of xz,
  read through memory-mapped MMDB files
* **Multi-source merge** with `cross_check` / `agreement` / `conflicts` so you can
  see which source said what before trusting a value
* **Nothing is dropped**: unmapped vendor fields land in `traits`, complete
  per-database records land in `raw`
* **Sync and async** APIs with the same surface (`lookup` / `alookup`,
  `Detector` / `AsyncDetector`)
* **English-only output**, stable field names, `null` for missing values
* Dependencies: `maxminddb` only (stdlib everywhere else)

---

## Install

```bash
pip install detector-sdk        # distribution name; the import name is `detector`
pip install .                   # from a checkout
```

The databases ship inside the wheel. First use decompresses them into
`~/.cache/detector/extracted` (override with `DETECTOR_CACHE_DIR`) — ~230 MB of
MMDB across 12 files. Reading is memory-mapped, so RAM stays low (~30 MB with
everything loaded).

> **Licence warning** : GeoLite2 data is bundled here because this build was asked
> to ship everything ip-location-db publishes. MaxMind's EULA forbids
> redistributing their data to third parties, so publishing this wheel (or an
> image containing it) to the public requires your own MaxMind entitlement.
> `Detector(datasets=[...])` / `exclude=[...]` let you drop those three datasets,
> and `NON_REDISTRIBUTABLE_KEYS` lists them.

## Quick start

```python
from detector import lookup, lookup_many, distance, nearest

info = lookup("2001:4860:4860::8888")

info.country_code            # 'CA'            (top-level merged value)
info.country.iso_code        # 'CA'
info.city.name()             # 'Montreal'
info.asn.organization        # 'Google LLC'
info.coordinates             # (45.5019, -73.5674)
info.cross_check["country"]  # {'DBIP-City-Lite': 'CA', 'User-Country': 'CA', ...}
info.conflicts               # [] - sources agree

info.to_dict()               # standard JSON document
info.to_json(indent=2)       # same, as a string
info.get("country.iso_code") # dotted access
info.to_flat_dict()          # {"country.iso_code": "CA", ...}

lookup_many(["8.8.8.8", "1.1.1.1", "not-an-ip"])   # batch, order preserved

distance("8.8.8.8", "1.1.1.1")                     # -> Distance
distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])      # -> list[Distance]
nearest("8.8.8.8", big_list, limit=3)              # closest N
```

### Distance

```python
result = distance("8.8.8.8", "1.1.1.1")
result.km           # 11917.42  (haversine, default)
result.mi           # 7404.34
result.same_country # False
result.same_asn     # False
float(result)       # 11917.42 - usable in arithmetic
str(result)         # '8.8.8.8 -> 1.1.1.1: 11,917.42 km'
```

Two methods: `haversine` (default, ~1 µs, great-circle on a sphere) and
`vincenty` (WGS84 ellipsoid, ~30 µs, millimetre accuracy). Geolocation error is
tens to thousands of kilometres, so the default is deliberate.

### Async

```python
import asyncio
from detector import alookup, adistance, AsyncDetector

async def main():
    info = await alookup("8.8.8.8")
    rows = await adistance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])

    async with await AsyncDetector.create(max_concurrency=64) as detector:
        results = await detector.lookup_many(ips)          # windowed, bounded memory
        async for info in detector.stream(open("ips.txt")):
            ...

asyncio.run(main())
```

`AsyncDetector` runs MMAP lookups in a thread pool (never on the event loop) and
uses native asyncio sockets for dataset downloads.

## The standard output document

```json
{
  "ip": "8.8.8.8",
  "version": 4,
  "found": true,
  "network": "8.8.8.0/24",
  "networks": {"dbip-city": "8.8.8.0/24", "dbip-country": "8.8.0.0/17"},
  "flags": {"is_private": false, "is_global": true, "is_loopback": false,
            "is_reserved": false, "is_multicast": false, "is_unspecified": false},
  "continent": {"name": "North America", "names": {"en": "North America", "...": "..."},
                "geoname_id": 6255149, "code": "NA"},
  "country": {"name": "United States", "names": {"en": "United States"},
              "geoname_id": 6252001, "iso_code": "US", "is_in_eu": false},
  "subdivisions": [{"name": "California", "names": {}, "geoname_id": null, "iso_code": null}],
  "city": {"name": "Mountain View", "names": {"en": "Mountain View"}, "geoname_id": null},
  "location": {"latitude": 37.422, "longitude": -122.085,
               "time_zone": null, "accuracy_radius": null},
  "postal": null,
  "time_zone": null,
  "asn": {"number": 15169, "asn": "AS15169", "organization": "Google LLC"},
  "traits": {},
  "hostname": null,
  "display": "Mountain View, California, United States",
  "sources": {"dbip-city": "DBIP-City-Lite", "dbip-asn": "DBIP-ASN-Lite",
              "iptoasn-asn": "IPtoASN-ASN", "...": "..."},
  "cross_check": {"country": {"DBIP-City-Lite": "US", "User-Country": "US"},
                  "asn": {"DBIP-ASN-Lite": 15169, "IPtoASN-ASN": 15169}},
  "agreement": {"country": true, "asn": true},
  "conflicts": [],
  "raw": {"dbip-city": {"city": {"names": {"en": "Mountain View"}}, "...": "..."}},
  "meta": {"schema_version": "1.0", "locales": ["en"],
           "databases": [{"key": "dbip-city", "name": "DBIP-City-Lite",
                          "build_date": "2026-09-01", "license": "CC BY 4.0"}],
           "attribution": "IP Geolocation by DB-IP (https://db-ip.com)"}
}
```

Want a different locale order? `lookup("8.8.8.8", locales=("zh-CN", "en"))` — the
`names` dictionaries always carry every language the source provides.

## JSON protocol

For callers that speak JSON rather than Python objects:

```python
from detector import request, request_json

response = request({"type": "ipv4", "action": "distance",
                    "data": {"ip": "8.8.8.8", "list": ["1.1.1.1", "223.5.5.5"]}})
response.ok        # True
response.data      # {"ip": "8.8.8.8", "source": {...}, "list": [...], "summary": {...}}

request_json('{"type":"ipv6","action":"info","data":{"ip":"2001:4860:4860::8888"}}')
```

`action` aliases (`lookup`, `query`, `dist`, ...) are accepted on input; the
response always echoes the canonical English action. Declaring `type: ipv4`
while sending an IPv6 literal is an error, not a silent mismatch.

## Datasets

| key | name | kind | licence | files | bundled | cadence |
|---|---|---|---|---|---|---|
| `dbip-city` | DBIP-City-Lite | city, subdivision, country, coords | CC BY 4.0 | 1 | yes | monthly |
| `dbip-asn` | DBIP-ASN-Lite | ASN | CC BY 4.0 | 1 | yes | monthly |
| `dbip-country` | DBIP-Country-Lite | country | CC BY 4.0 | 1 | yes | monthly |
| `geolite2-city` | GeoLite2-City | city, subdivision, country, coords | MaxMind EULA | 2 (v4/v6) | yes ⚠ | twice weekly |
| `geolite2-asn` | GeoLite2-ASN | ASN | MaxMind EULA | 1 | yes ⚠ | twice weekly |
| `geolite2-country` | GeoLite2-Country | country | MaxMind EULA | 1 | yes ⚠ | twice weekly |
| `iptoasn-asn` | IPtoASN-ASN | ASN | PDDL | 1 | yes | daily |
| `iptoasn-country` | IPtoASN-Country | country | PDDL | 1 | yes | daily |
| `origin-asn` | Origin-ASN | ASN | PDDL | 1 | yes | daily |
| `user-country` | User-Country | country | PDDL | 1 | yes | daily |
| `server-country` | Server-Country | country | PDDL | 1 | yes | daily |

⚠ = bundled but **not redistributable**: MaxMind's EULA does not permit passing
their data on to third parties. Drop them with
`Detector(exclude=NON_REDISTRIBUTABLE_KEYS)` if you are publishing.

Files are stored as `.mmdb.xz` (xz beats gzip by ~35% on MMDB data and the
standard-library `lzma` module unpacks it). Loaded files also carry their address
family, so a v6-only GeoLite2 half is never consulted for an IPv4 address.

You can always ignore the bundled copies and point the SDK at files you are
licensed to hold:

```python
from detector import Detector

detector = Detector(databases={
    "my-geolite2-city": "/data/GeoLite2-City.mmdb",
    "my-geoip2-isp":    "/data/GeoIP2-ISP.mmdb",
})
```

Any MMDB v2.0 file works — MaxMind, DB-IP, ip-location-db, or one you built with
`mmdb_writer`. Unknown schemas still contribute: unmapped keys end up in
`traits`, the whole record in `raw`.

### Selecting datasets

```python
Detector(datasets=["dbip-city", "iptoasn-asn"])          # only these
Detector(exclude=["user-country", "server-country"])     # all but these
Detector(datasets=["dbip-city"], include_raw=False)      # smaller payloads
```

### Updating

```python
import asyncio
from detector import update_datasets, update_datasets_async, known_datasets

known_datasets()                     # full registry: licences, mirrors, cadence

update_datasets("~/.cache/detector") # blocking
await update_datasets_async(         # concurrent asyncio downloads
    "~/.cache/detector",
    datasets=["dbip-city", "iptoasn-asn"],
    concurrency=4,
)
```

Fresh files in the cache directory override the bundled copies automatically.
`DETECTOR_DB_DIR` repoints the whole bundled directory instead.

## Extension points

| Need | How |
|---|---|
| Extra/vendor databases | `Detector(databases={...})`, or drop `.mmdb` into `DETECTOR_DB_DIR` |
| Custom distance metric | `Detector(distance_method="vincenty")` or compute from `info.coordinates` |
| Own JSON shape | `info.to_dict()` / `to_flat_dict()` and rebuild |
| Caching strategy | `Detector(cache_size=N)` (`0` disables) |
| Cleaner output | `include_raw=False`, `locales=("en",)` |
| Process-wide defaults | `detector.configure(...)`, `set_default_geo(my_detector)` |

## Performance notes

* Lookups are memory-mapped: 12 databases open cost ~30 MB RSS, and the 127 MB
  city database never lands in your heap.
* Warm startup is ~10 ms; the very first run decompresses the bundled data
  (~13 s).
* A default lookup is ~477 µs (2.1k IP/s) over 11 datasets / 12 files; ~40 µs
  when the address is already in the LRU cache.
* `stream()` and the async variant keep memory flat for arbitrarily long inputs;
  `include_raw=False` trims documents from ~9.8 KB to ~5.8 KB.
* Threads and multiprocessing inside the SDK were removed on purpose - both
  measured slower than sequential. Shard the input across processes instead
  (see `docs/07-performance.md`).
* Install the C extension of `maxminddb` (`libmaxminddb-dev` present at install
  time) for 3-5x faster raw lookups.

## Documentation

| file | contents |
|---|---|
| `docs/01-getting-started.md` | install, first lookup, first distance, first async call |
| `docs/02-api-reference.md` | every class, method and helper |
| `docs/03-output-schema.md` | the standard document field by field |
| `docs/04-distance.md` | maths, methods, accuracy reality check |
| `docs/05-json-protocol.md` | request/response envelopes and error codes |
| `docs/06-datasets.md` | datasets, licences, updates, custom MMDBs |
| `docs/07-performance.md` | measured numbers and tuning |
| `docs/08-troubleshooting.md` | when something looks wrong |
| `docs/09-architecture.md` | module map, data flow, design decisions |

Runnable examples live in `examples/` (`quickstart_sync.py`, `async_batch.py`,
`json_protocol.py`, `bulk_streaming.py`, `custom_database.py`).

## Licence

MIT for the code. Data licences and mandatory attribution are documented in
`NOTICE` and reproduced automatically in every response under `meta.attribution`.
