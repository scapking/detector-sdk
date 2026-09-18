# 06 - Datasets, licences and updates

## Everything ships (11 datasets, 12 databases, ~90 MB of parts)

| key | name | provides | licence | files | cadence |
|---|---|---|---|---|---|
| `dbip-city` | DBIP-City-Lite | city, subdivision, country, continent, coordinates | CC BY 4.0 | 1 | monthly |
| `dbip-asn` | DBIP-ASN-Lite | ASN number + organisation | CC BY 4.0 | 1 | monthly |
| `dbip-country` | DBIP-Country-Lite | country, continent | CC BY 4.0 | 1 | monthly |
| `geolite2-city` | GeoLite2-City | city, subdivision, country, coordinates | MaxMind EULA ⚠ | 2 (IPv4 + IPv6) | twice weekly |
| `geolite2-asn` | GeoLite2-ASN | ASN number + organisation | MaxMind EULA ⚠ | 1 | twice weekly |
| `geolite2-country` | GeoLite2-Country | country, continent | MaxMind EULA ⚠ | 1 | twice weekly |
| `iptoasn-asn` | IPtoASN-ASN | ASN number + organisation | PDDL | 1 | daily |
| `iptoasn-country` | IPtoASN-Country | country | PDDL | 1 | daily |
| `origin-asn` | Origin-ASN | ASN number + organisation | PDDL | 1 | daily |
| `user-country` | User-Country | country | PDDL | 1 | daily |
| `server-country` | Server-Country | country | PDDL | 1 | daily |

Everything comes from the upstream [ip-location-db](https://github.com/sapics/ip-location-db)
project, plus DB-IP's own endpoints for the DB-IP Lite trio. Data is stored as
Either a whole `.mmdb.xz`/`.mmdb.zst`, or a set of `.mmdb.partNNN.xz|zst` parts
for the big ones. `lzma`/`zstandard` unpack them into `<cache>/extracted/` on
first use: parts decode in parallel and are merged into one MMDB file. `.mmdb.gz` and plain `.mmdb` files (what
upstream serves) are understood as well.

## The GeoLite2 licence problem

MaxMind's EULA forbids disclosing their data to third parties. Bundling
GeoLite2 in a package you hand out *is* disclosure and needs a commercial
redistribution licence (GeoLite2-City is roughly $10 000/year).

This build bundles GeoLite2 anyway, because it was specified to ship everything
ip-location-db publishes. Consequences, stated plainly:

* **Internal use**: fine, no extra entitlement needed.
* **Publishing a wheel / image containing it**: your problem to license.
* **Not your risk to take**: drop those datasets.

```python
from detector import Detector, NON_REDISTRIBUTABLE_KEYS, known_datasets

NON_REDISTRIBUTABLE_KEYS            # ('geolite2-city', 'geolite2-asn', 'geolite2-country')

safe = Detector(exclude=NON_REDISTRIBUTABLE_KEYS)                  # everything else
minimal = Detector(datasets=["dbip-city", "dbip-asn", "dbip-country"])

# compliance check before publishing
[publishable := {k: v for k, v in known_datasets().items() if v["redistributable"]}]
```

Every manifest entry and every `meta.databases` record carries the licence, the
attribution text and a `redistributable` flag, so a release pipeline can assert
on it instead of trusting a README.

## Address families

Split datasets keep their family: `ip_versions` is `(4,)`, `(6,)` or `(4, 6)`.
An IPv4 lookup never consults a v6-only file (and vice versa), and doing so can
never raise. `Detector().describe()["databases"]` shows the coverage per file.

## Why several country and ASN databases?

Because one database lying to you looks exactly like one database telling the
truth. Five country sources and three ASN sources cross-check each other:

```python
info = Detector().lookup("8.8.8.8")
info.cross_check["country"]        # {'DBIP-City-Lite': 'US', 'User-Country': 'US', ...}
info.agreement["country"]          # True - all five agree
info.conflicts                     # ['asn_organization'] - operators spell names differently
```

Typical conflicts: operator naming (`GOOGLE` vs `Google LLC`), country for
mobile/anycast networks, and coordinates (each source picks a different
centroid).

## Selecting datasets

```python
Detector(datasets=["dbip-city"])                    # only this one
Detector(datasets=["dbip-city", "iptoasn-asn"])
Detector(exclude=["user-country", "server-country"])
Detector(databases={"geoip2-enterprise": "/data/GeoIP2-Enterprise.mmdb"})  # exactly these
```

Fewer datasets means faster merges: each loaded database costs one lookup and
one normalization per query (~35-40 µs total for the full set of eight).

## Downloading / refreshing

```python
from detector import known_datasets, update_datasets, update_datasets_async

known_datasets()                                  # full registry

update_datasets()                                 # blocking, into the cache dir
update_datasets("~/data/ip", datasets=["dbip-city"], source="db-ip")
await update_datasets_async(datasets=["iptoasn-asn", "origin-asn"], concurrency=4)
await update_datasets_async(datasets=["geolite2-city"])      # opt-in only
```

* Files land in the target directory (default `~/.cache/detector`), with a
  refreshed `MANIFEST.json` carrying sizes, sha256 and build dates.
* The cache directory is scanned **before** the bundled data, so a refreshed
  dataset silently overrides its bundled twin.
* Mirrors are tried in order: DB-IP's own endpoint first for the DB-IP trio (a
  `.mmdb.gz`), then the ip-location-db GitHub release (a plain `.mmdb`).
* Downloads are streamed with a progress callback and are atomic: a crashed
  download never leaves a half-written database behind.
* `DETECTOR_DB_DIR` points the SDK at a completely different data directory if
  you would rather not touch the cache.

Maintainers vendoring data into the package:

```bash
python tools/build_data.py                     # every bundled dataset
python tools/build_data.py dbip-city --source sapics
python tools/build_data.py --period 2026-08    # pin a DB-IP release month
```

## Adding a database nobody has seen before

Any MaxMind DB v2.0 file works, whatever its schema. Fields the mapper does not
recognise are preserved in `traits`, and the complete record in `raw`:

```python
Detector(databases={
    "city": "/data/GeoLite2-City.mmdb",
    "asn": "/data/GeoLite2-ASN.mmdb",
    "internal": "/data/acme-ip-reputation.mmdb",
})
```

Drop `.mmdb` / `.mmdb.gz` files into `DETECTOR_DB_DIR` and they are discovered
automatically. Filenames matching a registry entry (for example
`dbip-city-lite-2026-09.mmdb.gz`) inherit that entry's name, kind, licence and
`providers` metadata.

## Manifest and auditing

```python
from detector import read_manifest, write_manifest

manifest = read_manifest("src/detector/data")
manifest["datasets"][0]["sha256"]
manifest["datasets"][0]["build_date"]
manifest["attribution"]
```

`Detector.describe()` gives the same information for the *loaded* set, plus
licence and `source_url` per database - the honest answer to "what data produced
this output, and under which licence?".
