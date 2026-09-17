# 08 - Troubleshooting

## `NoDatabaseError` on import or first use

The package data is missing or unreadable. Check:

```python
from detector.databases import package_data_dir, BUNDLED_KEYS, BUILTIN_DATASETS
from detector import Detector
print(package_data_dir(), Detector.available_datasets().keys())
```

`package_data_dir()` must contain `dbip-*.mmdb.gz` / `iptoasn-*.mmdb.gz` plus
`MANIFEST.json`. If you installed from a source tree, run the vendoring step:

```bash
python tools/build_data.py
```

## First call takes ~7 seconds

That is the one-time decompression of 87 MB into `~/.cache/detector/extracted`
(185 MB of `.mmdb`). Subsequent process starts take ~7 ms because the files are
already there. Point `DETECTOR_CACHE_DIR` at a writable, reasonably fast volume
if `$HOME` is on a slow or read-only filesystem (a temp directory is used as a
fallback).

## Lookups return `found: false` for a public address

* The address really has no record in any loaded dataset (rare but possible for
  freshly announced blocks).
* The dataset set is too small: `Detector(datasets=["dbip-country"])` has no
  city/coordinates. Run `Detector().describe()` to see what is loaded.
* It is a private, reserved or documentation range: `192.168.x.x`, `10.x.x.x`,
  `100.64.0.0/10`, `2001:db8::/32` - these are correctly absent from public
  databases. The `flags` block still tells you what the address is.

## `conflicts` is never empty

Expected for operator names: `GOOGLE` (IPtoASN) vs `Google LLC` (DB-IP) vs
`Google LLC` (Origin-ASN) are three spellings of one fact. Country and city
conflicts are also normal for mobile and anycast networks. Use `agreement` for
a quick gate:

```python
if info.agreement.get("country") and info.country_code == "CN":
    ...
```

Multi-source disagreement is information, not a bug. Read `cross_check` before
deciding which value you trust.

## Distances are `null`

`reason` says why (see [04-distance.md](04-distance.md)). The two usual cases:
private addresses (no coordinates) and datasets loaded without a location
provider (`dbip-country` alone has no coordinates; load `dbip-city` or
`geolite2-city`).

## Distances look wrong for CDN / DNS / anycast addresses

They usually are, and no fix exists at this layer: `1.1.1.1` reports Cloudflare's
registered location, `8.8.8.8` Google's. The database is answering honestly about
the *network*, not about the user behind it.

## `include_all_names=False` and I still see non-English text

Only `names` maps are trimmed. `display` is built from the requested `locales`
and `raw` records are passed through untouched - set `include_raw=False` as well
if you need an ASCII-only payload:

```python
Detector(locales=("en",), include_all_names=False, include_raw=False).lookup("8.8.8.8").to_json()
```

## Slow batch processing

See [07-performance.md](07-performance.md). Short version: `include_raw=False`
plus sharding across processes. Threads and multiprocessing inside `lookup_many`
were removed because they measured *slower*; do not re-add them.

## `DETECTOR_DB_DIR` seems to be ignored

Relative paths resolve against the process working directory. Use absolute
paths, and remember that a directory passed as `db_dir=` wins over the
environment variable.

## Where did a value come from?

```python
info.sources              # dataset key -> product name that answered
info.networks             # dataset key -> CIDR that matched
info.raw["dbip-city"]     # the untouched record
detector.describe()       # inventory with licence, build date, sha256, source URL
```

The attribution string in `meta.attribution` satisfies the CC BY 4.0 obligation
for DB-IP and the PDDL notice for the ip-location-db datasets.

## Reporting a data problem

1. `Detector().describe()` for the dataset build dates
2. `info.to_dict()` for the exact record
3. Upstream issue: [DB-IP](https://db-ip.com/) or
   [ip-location-db](https://github.com/sapics/ip-location-db)

Refreshing first often fixes it - `update_datasets()` pulls the newest release,
and newer files in the cache directory override the bundled copies.
