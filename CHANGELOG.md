# Changelog

## 0.4.0

* **Lazy block storage** (`.bz`): databases ship as independent zstd blocks plus
  an index, read by the bundled pure-Python MMDB reader. First use no longer
  materialises 250 MB on disk - a lookup decompresses only the blocks it touches.
  Cold start (import + open + first query) is **~0.39 s on a 2-core / 25 MB/s
  box** instead of ~10 s; steady state is ~26 µs/ip from the block LRU.
* The format decoder is the official MaxMind pure-Python reader, validated
  byte-for-byte against `maxminddb` across ~12k random lookups / 12 databases.
* **GeoLite2 is no longer bundled** (its EULA forbids redistribution; also the
  size blocker). The 8 redistributable datasets (all sapics + DB-IP) ship lazily.
  GeoLite2 stays registered and available via `update_datasets()` with a MaxMind
  key.
* Removed the `DETECTOR_INIT=import` auto-kick: initialisation is explicit again
  (`await warmup()` / `await ready()`). Lazy data makes this moot anyway - there
  is nothing to unpack.
* `ready()` / `progress()` report the lazy set as immediately ready.


## 0.3.0

* Progressive, priority-ordered unpacking: the bundled databases are written to
  disk cheapest-first in a background thread, so a country/ASN answer is ready
  in a fraction of a second while the big city database finishes.
* `DETECTOR_INIT=import|blocking|lazy|off` - optionally start unpacking at
  `import detector`, overlapping it with your own start-up work.
* Bounded first-query latency: `wait="all"|"any"|"none"`, `wait_timeout` and
  `on_timeout="partial"|"error"`. `LoadingTimeoutError` carries
  `ready`/`total`/`missing`/`retry_after`; the protocol returns
  `status:"error", error.code:"loading_timeout"`.
* `await ready(timeout=..., wait=...)` and `await progress()` to poll readiness;
  results carry `meta.preparation` (ready/total/loading/complete/failed).
* A broken dataset is reported, not fatal: other databases still answer and the
  failure appears in `progress()["failed"]` / `meta.datasets_failed`
  (`strict=True` upgrades it to an error).
* `Detector.refresh()` re-scans and re-opens after `update_datasets()`.


## 0.2.2

* Bundled databases are shipped **split into independently compressed parts**
  (``dbip-city-lite.mmdb.part001.zst`` …). ``extract_many`` decodes the parts on
  a thread pool and merges them into one MMDB file, so first-run unpacking uses
  every core instead of one - and the merged file is byte-identical to the
  original database.
* The largest databases ship as **zstd** (``zstandard`` is now a dependency):
  zstd decodes ~7x faster than the xz archives used before.
* New ``warmup()`` coroutine: unpack the bundled data up front (container build,
  startup hook, background task) instead of on the first query.
* ``AsyncDetector`` gained ``dataset_keys`` / ``database_uids``.


## 0.2.1

* **`info()` and `distance()` now return the standard JSON document (a plain
  `dict`) by default** instead of model objects. Pass `as_object=True` for
  `IPInfo` / `Distance` / `Response`.
* `AsyncDetector` gained `dataset_keys` and `database_uids`.


## 0.2.0

**Breaking: the public surface is now two coroutines.**

* ``info(target, *, as_dict=False, **options)`` - one IP or a batch (iterable,
  generator, async iterable); returns ``IPInfo``, ``list[IPInfo]``, or a
  ``Response`` for a JSON envelope.
* ``distance(source, targets=None, *, as_dict=False, method=None, **options)`` -
  1-to-1, 1-to-N (unbounded) or N-to-M; returns ``Distance`` / ``list[Distance]``.
* Both accept the ``{"type","action","data","status"}`` envelope, so the separate
  protocol helpers are gone.
* Removed from the public API: ``lookup``, ``lookup_many``, ``stream``,
  ``distance_many``, ``nearest``, ``request``, ``request_json``, every
  ``alookup``-style async twin, ``configure_async``, ``set_default_geo`` and the
  ``IPGeo``/``AsyncIPGeo`` aliases. The engine behind them is unchanged and still
  reachable through ``Detector`` / ``AsyncDetector`` for callers who need
  ``nearest``, ``describe``, ``stats`` or ``update_datasets`` on a client.
* ``configure(**options)`` sets call defaults; clients are pooled per option set;
  ``await close()`` releases them.

## 0.1.0

Initial release.

**Lookup**

* One `lookup()` merges every bundled dataset into a single standardised,
  English-only document: merged fields, `sources`, `networks`, `cross_check`,
  `agreement`, `conflicts`, `traits`, `raw`, `meta`.
* IPv4 and IPv6, private/reserved/loopback/multicast flags, optional reverse DNS.
* Missing data returns `found: false` with `null` fields instead of raising.
* `lookup_many()` (order preserved) and `stream()` (lazy, constant memory).

**Distance**

* `distance(source, targets)` for 1-to-1 and 1-to-N (N unbounded, generators
  accepted), `distance_many()` for N x M, `nearest()` for ranking.
* `haversine` (default) and `vincenty` (WGS84 ellipsoid) methods.
* Per-row `reason` when no distance can be computed.

**Async**

* `AsyncDetector` mirroring the sync client, with windowed concurrency, async
  streaming (sync and async iterables), and native asyncio downloads.
* Module-level `alookup`, `adistance`, `arequest`, ... sharing one lazy client.

**Datasets**

* All 11 datasets ip-location-db publishes are bundled: `dbip-city`, `dbip-asn`,
  `dbip-country` (CC BY 4.0), `geolite2-city` / `geolite2-asn` /
  `geolite2-country` (MaxMind EULA - bundled, **not** redistributable; see
  NOTICE) and `iptoasn-asn`, `iptoasn-country`, `origin-asn`, `user-country`,
  `server-country` (PDDL).
* Data ships as `.mmdb.xz` (35% smaller than gzip, unpacked by stdlib `lzma`);
  gzip and plain mmdb are still accepted for downloads.
* Split datasets share one key: `geolite2-city` is two files with `variant`
  `ipv4` / `ipv6`, tracked per file in `sources` / `networks` / `raw` and gated
  by address family.
* Any MMDB v2.0 file can be loaded; unknown fields survive in `traits` / `raw`.
* `update_datasets()` / `update_datasets_async()` with mirror fallback,
  concurrent transfers, atomic writes and a refreshed `MANIFEST.json`.

**Protocol**

* `{"type","action","data","status"}` envelopes with `info` / `distance`
  actions, batch payloads, canonical English action echo, and stable error codes.

**Output**

* `to_dict()`, `to_json()`, `to_flat_dict()`, dotted `get()`.
* `locales` for name selection; `include_all_names=False` for strictly English
  payloads; `include_raw` / `include_cross_check` to trade completeness for size.

**Notable engineering decisions**

* GeoLite2 is bundled on request, with the licence consequence documented in
  NOTICE and machine-readable in `known_datasets()[key]["redistributable"]`.
* `maxminddb` is the only dependency (`geoip2` is a model layer over the same
  reader and pulls in `requests` + `aiohttp`).
* Threads and multiprocessing were removed from `lookup_many`: both measured
  slower than sequential execution (GIL on the merge, pickling of `raw` records).
  Sharding across processes is documented instead.
