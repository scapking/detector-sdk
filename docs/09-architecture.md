# 09 - Architecture

```
detector/
├── src/detector/
│   ├── __init__.py      public surface: clients, helpers, models, errors
│   ├── api.py           Detector: open databases, merge, distance, protocol hooks
│   ├── aio.py           AsyncDetector + module-level coroutines
│   ├── databases.py     registry, discovery, decompression, MMDB reader, normalizer
│   ├── models.py        IPInfo / Place / Location / ASN - the standard schema
│   ├── distance.py      haversine + Vincenty, Distance result
│   ├── envelope.py      {"type","action","data","status"} protocol
│   ├── net.py           asyncio HTTP(S) client for dataset downloads
│   ├── update.py        dataset downloads, manifest, registry helpers
│   ├── cache.py         thread-safe LRU for lookup results
│   ├── exceptions.py    stable error codes
│   └── data/            8 bundled datasets + MANIFEST.json
├── tests/               75 tests: lookup, distance, protocol, async, databases
├── examples/            runnable scripts for each capability
├── tools/               build_data.py, benchmark.py (maintainers)
└── docs/                this documentation
```

## Data flow of one lookup

```
ip string ──parse_ip──▶ ipaddress object
                            │
                            ├─ flags (stdlib: private / global / loopback / ...)
                            │
                            ├─ for each loaded Database (sorted by priority):
                            │      MMAP lookup ─▶ raw record ─▶ normalize_record()
                            │                                        │
                            │        ┌───────────────────────────────┤
                            │        ▼                               ▼
                            │   merged fields (first wins)     cross_check / raw
                            ▼
                        IPInfo ──to_dict()──▶ standard JSON document
```

Four properties fall out of that design:

1. **Merging is deterministic.** Databases are sorted by kind priority
   (`city` > `enterprise` > `isp` > `asn` > `country`), ties broken by dataset
   key, so the same input always produces the same merged value.
2. **Nothing is lost.** Fields the mapper does not recognise go to `traits`, the
   complete record goes to `raw`, and the per-source view goes to `cross_check`.
3. **New databases need no code.** Any MMDB v2.0 file is read; the normalizer
   tolerates MaxMind's nested schema, DB-IP's schema and flat variants.
4. **Sync and async share one engine.** `AsyncDetector` runs the same code in a
   thread pool; only the network layer is genuinely async.

## Why these choices

| decision | reason |
|---|---|
| `maxminddb` only, no `geoip2` dependency | `geoip2` is a model layer over the same reader and drags in `requests` + `aiohttp`; we ship our own schema |
| MMAP by default | 185 MB of databases with 28 MB RSS; RAM is not the constraint, latency is |
| Data shipped in the wheel | "no network, no extra download" was a hard requirement; the cost is an 87 MB package |
| DB-IP Lite bundled, GeoLite2 opt-in | CC BY 4.0 and PDDL permit redistribution with attribution; MaxMind's EULA does not |
| Multi-source merge with cross-check | one database being wrong looks exactly like one database being right |
| English-only output | stable field names and values for machines; other languages stay available in `names` |
| No threads / no multiprocessing inside `lookup_many` | both measured slower (GIL for threads, pickling for processes) - see docs/07 |
| Stable `null`s instead of omitted keys | consumers never write defensive `.get()` chains |

## Extension points

| need | hook |
|---|---|
| extra or licensed databases | `Detector(databases={...})`, `db_dir=`, `DETECTOR_DB_DIR`, or drop files in the data dir |
| different distance maths | `distance_method="vincenty"`, or use `info.coordinates` yourself |
| other output shapes | `to_dict()`, `to_flat_dict()`, `get("a.b.c")`, or build from `raw` |
| custom caching | `cache_size`, plus your own layer around `lookup` |
| non-Python callers | the JSON envelope (`request` / `request_json`) |

## Failure model

* Malformed input: `InvalidIPError` with `code="invalid_ip"` and the offending value.
* Missing data: `found: false`, `None` fields, no exception (opt in with
  `raise_on_missing=True`).
* Missing databases: `NoDatabaseError` at construction, never a silent empty result.
* Unreadable database file: `DatabaseNotFoundError`, skipped or raised depending
  on `strict`.
* Protocol misuse: a `status="error"` response carrying a stable `code`; the
  protocol layer never raises into the caller.
