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

## The two functions

```python
import asyncio
from detector import info, distance

async def main():
    one = await info("8.8.8.8")                     # -> IPInfo
    many = await info(["8.8.8.8", "1.1.1.1"])       # -> [IPInfo, IPInfo]
    stream = await info(generator_of_millions)      # -> [IPInfo, ...] bounded memory
    document = await info("8.8.8.8", as_dict=True)  # -> dict (standard JSON)

    pair = await distance("8.8.8.8", "1.1.1.1")             # -> Distance
    rows = await distance("8.8.8.8", ["1.1.1.1", "::1"])    # -> [Distance]
    grid = await distance(["8.8.8.8"], ["::1"])             # -> [Distance]

asyncio.run(main())
```

That is the entire surface. No sync variants, no separate batch functions:
`info` and `distance` detect whether you passed one value or many.

Options are per call, or set once with `configure(...)`:

```python
await info("8.8.8.8", datasets=["dbip-city"], locales=("en",), include_raw=False)

from detector import configure
configure(cache_size=8192, distance_method="vincenty", max_concurrency=64)
```

Clients are pooled per option set, so repeated calls reuse one memory-mapped
client instead of reopening 12 databases. `await close()` releases them.

## What you get per lookup

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
