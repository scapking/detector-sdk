# 07 - Performance

All numbers measured on this machine (x86_64, Python 3.13, `maxminddb` 3.2 with
its C extension, **11 datasets / 12 files** loaded, page cache warm). Reproduce
with `python tools/benchmark.py`.

## Single lookups

| scenario | per IP | throughput |
|---|---|---|
| 11 datasets, `raw` + `cross_check` (defaults) | 477 µs | ~2 100 /s |
| 11 datasets, `include_raw=False, include_cross_check=False` | 350 µs | ~2 860 /s |
| `dbip-city` only | 170 µs | ~5 900 /s |
| same 11 datasets, result already in the LRU cache | 40 µs | ~25 000 /s |
| 3 000 random public addresses, sequential | 483 µs | ~2 070 /s |
| same, streamed without retaining results | 405 µs | ~2 470 /s |

Measurements are taken after a warm-up pass. The very first pass over a freshly
mapped database is I/O bound (page-cache fill across 185 MB of mmap): a cold
5 000-address run can measure 10 ms/IP. Any long-lived process pays that once;
`load_mode="memory"` removes it at the cost of the RAM.

Cost is dominated by the Python-level merge (one normalize per dataset per IP),
not by the MMDB lookups themselves: the raw lookups together take ~80 µs of the
477 µs. Addresses are also filtered by family before touching a file, so a
GeoLite2-City split never sees the wrong half.

## Startup and memory

| metric | value |
|---|---|
| first ever `Detector()` / `await warmup()` (90 MB of parts -> 251 MB of mmdb) | 12.6 s (2 cores, 25 MB/s disk) |
| same, warm cache (later processes) | 12-17 ms |
| `await warmup()` after the cache exists | 12 ms |
| warm `Detector()` (files already extracted) | 10 ms |
| resident set with 12 databases open | 30 MB |
| cache directory after extraction | 251 MB |
| one result document with `raw` | ~9.8 KB |
| same result with `include_raw=False, include_all_names=False` | ~5.8 KB |

Memory stays small because databases are memory-mapped: pages are shared with
the OS page cache and can be evicted. Holding *results* is what costs memory -
20 000 results with `raw` records is a few hundred MB. Use `stream()` when you
do not need to keep them.

## Batch work

```python
for info in detector.stream(open("huge.log")):     # flat memory
    ...
```

Streaming 200 000 addresses with a warm cache ran at 33 µs/address (6.6 s
total). Without cache reuse, expect the single-lookup numbers above (~483 µs for
unique addresses, dominated by the merge rather than by the lookups).

### Threads and processes are both a trap here

Measured, and the reason the API does not offer them:

* **threads**: 470 µs/ip vs 424 µs/ip sequential. The merge is pure Python and
  holds the GIL, so threads add handoff cost and buy nothing.
* **multiprocessing**: 0.13x - 0.23x of sequential speed. Workers must pickle
  their results back, and a result carrying `raw` records serializes to ~10 KB,
  so 50 000 addresses move ~500 MB through pipes.

Real parallelism comes from sharding the input across OS processes, where each
worker opens its own memory maps (shared through the page cache) and returns
whatever slim shape the parent actually needs:

```python
from concurrent.futures import ProcessPoolExecutor
from detector import Detector


def worker(chunk):
    with Detector(cache_size=0, include_raw=False) as local:
        return [(info.ip, info.country_code) for info in local.lookup_many(chunk)]


def sharded(ips, workers=4):
    chunks = [ips[i::workers] for i in range(workers)]
    with ProcessPoolExecutor(workers) as pool:
        for part in pool.map(worker, chunks):
            yield from part
```

## Tuning knobs

| knob | effect |
|---|---|
| `include_raw=False` | ~25% faster, ~2x smaller documents |
| `include_cross_check=False` | skips the per-source comparison work |
| `datasets=[...]` | fewer datasets, proportionally less merge work |
| `cache_size=N` | repeated addresses drop to ~30 µs; 0 disables |
| `locales=("en",)` | skips language fallback scans when picking names |
| `include_all_names=False` | trims `names` maps to the requested locales |
| `load_mode="memory"` | reads the whole database into RAM: faster on cold page cache, costs 185 MB |

## C extension

`maxminddb` builds an optional C extension (`libmaxminddb`) at install time. On
this machine it is present, and the reader class in use is
`maxminddb.extension.Reader`. Without it you get the pure-Python reader, roughly
3-5x slower per raw lookup. Check:

```python
type(detector.databases[0]._reader).__name__   # 'Reader' (C) or 'MmapReader'
```

## What not to optimise

Distance is one multiplication-heavy formula over two coordinates: ~1 µs for
haversine, ~30 µs for vincenty. The lookups that feed it dominate by two orders
of magnitude. Optimising the geometry is pointless; caching lookups is not.
