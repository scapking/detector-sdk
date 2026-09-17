# 01 - Getting started

## Install

```bash
pip install detector-sdk       # distribution name; import name is `detector`
pip install -e .               # from a checkout
```

Runtime dependency: `maxminddb` (pure Python with an optional C extension).
Everything else is the standard library.

All 11 datasets the upstream project publishes ship inside the package (76 MB of
`.mmdb.xz` across 12 files). The first call decompresses them into
`~/.cache/detector/extracted/` (~230 MB, ~13 s, once). After that reads are
memory-mapped, so a lookup never loads the 127 MB city database into your
process's private memory.

Override the locations:

| variable | meaning |
|---|---|
| `DETECTOR_CACHE_DIR` | where decompressed copies and refreshed datasets live |
| `DETECTOR_DB_DIR` | replaces the bundled data directory entirely |

## First lookup

```python
from detector import lookup

info = lookup("8.8.8.8")
print(info.ip, info.country.iso_code, info.city.name(), info.asn.organization)
print(info.to_json(indent=2)[:200])
```

`lookup()` uses a lazily created process-wide client. That is the right default
for scripts and small services. For control over datasets, caching and locale
order, build your own client:

```python
from detector import Detector

with Detector(locales=("en",), cache_size=8192) as detector:
    info = detector.lookup("2001:4860:4860::8888")
```

## First distance

```python
from detector import distance

distance("8.8.8.8", "1.1.1.1")                    # -> Distance
distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])     # -> list[Distance]
for row in distance("8.8.8.8", (ip for ip in open("ips.txt"))):   # unbounded
    print(row.target, row.km)
```

## First async call

```python
import asyncio
from detector import AsyncDetector

async def main():
    async with await AsyncDetector.create() as detector:
        info = await detector.lookup("8.8.8.8")
        rows = await detector.lookup_many(["8.8.8.8", "1.1.1.1", "114.114.114.114"])
        return info, rows

info, rows = asyncio.run(main())
```

## What you get per lookup

* merged fields (`country`, `city`, `location`, `asn`, ...) - first database wins, gaps get filled
* `sources` / `networks` - which dataset answered, and its CIDR for that address
* `cross_check` / `agreement` / `conflicts` - what every dataset said about the shared fields
* `raw` - the complete record from every dataset, untouched
* `traits` - vendor-specific extras that have no home in the fixed schema
* `meta` - schema version, database inventory, licences, attribution

Nothing is hidden and nothing is dropped. If two sources disagree, you see it.

## Next

* [02-api-reference.md](02-api-reference.md) - every class and function
* [03-output-schema.md](03-output-schema.md) - the standard document, field by field
* [04-distance.md](04-distance.md) - the distance maths and its limits
* [05-json-protocol.md](05-json-protocol.md) - `{"type","action","data","status"}` envelopes
* [06-datasets.md](06-datasets.md) - bundled vs optional datasets, licences, updates
* [07-performance.md](07-performance.md) - measured numbers and how to tune them
* [08-troubleshooting.md](08-troubleshooting.md) - when something looks wrong
