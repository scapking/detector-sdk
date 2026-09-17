# 03 - Output schema

`IPInfo.to_dict()` returns a fixed set of keys. Missing values are `null`
instead of absent, so downstream code never needs defensive lookups. Breaking
schema changes bump `meta.schema_version`.

```json
{
  "ip": "8.8.8.8",
  "version": 4,
  "found": true,
  "network": "8.8.8.0/24",
  "networks": {"dbip-city": "8.8.8.0/24", "dbip-country": "8.8.0.0/17"},
  "flags": {
    "is_private": false, "is_global": true, "is_loopback": false,
    "is_reserved": false, "is_multicast": false, "is_unspecified": false
  },
  "continent": {"name": "North America", "names": {"en": "North America", "zh-CN": "北美洲"},
                "geoname_id": 6255149, "code": "NA"},
  "country": {"name": "United States", "names": {"en": "United States"},
              "geoname_id": 6252001, "iso_code": "US", "is_in_eu": false},
  "subdivisions": [{"name": "California", "names": {"en": "California"},
                    "geoname_id": null, "iso_code": null}],
  "city": {"name": "Mountain View", "names": {"en": "Mountain View"}, "geoname_id": null},
  "location": {"latitude": 37.422, "longitude": -122.085,
               "time_zone": null, "accuracy_radius": null},
  "postal": null,
  "time_zone": null,
  "asn": {"number": 15169, "asn": "AS15169", "organization": "Google LLC"},
  "traits": {},
  "hostname": null,
  "display": "Mountain View, California, United States",
  "sources": {"dbip-city": "DBIP-City-Lite", "dbip-asn": "DBIP-ASN-Lite"},
  "cross_check": {"country": {"DBIP-City-Lite": "US", "User-Country": "US"},
                  "asn": {"DBIP-ASN-Lite": 15169, "IPtoASN-ASN": 15169}},
  "agreement": {"country": true, "asn": true},
  "conflicts": [],
  "raw": {"dbip-city": {"city": {"names": {"en": "Mountain View"}}, "...": "..."}},
  "meta": {
    "schema_version": "1.0",
    "locales": ["en"],
    "databases": [{"key": "dbip-city", "name": "DBIP-City-Lite", "kind": "city",
                   "database_type": "DBIP-City-Lite", "file": "dbip-city-lite.mmdb",
                   "build_date": "2026-09-01", "license": "CC BY 4.0",
                   "attribution": "IP Geolocation by DB-IP (https://db-ip.com)",
                   "source_url": "https://db-ip.com/db/download/ip-to-city-lite",
                   "languages": ["en", "ja", "zh-CN"],
                   "sha256": "c5d05b35...", "size": 127339927,
                   "providers": ["city", "subdivision", "country", "continent", "location"]}],
    "attribution": "IP Geolocation by DB-IP (https://db-ip.com) | ..."
  }
}
```

### Keys are per *file*, not per dataset

Most datasets are one file, so the key is the dataset key (`dbip-city`). A
dataset published as separate IPv4/IPv6 files contributes two entries, suffixed
with the variant: `geolite2-city-ipv4` and `geolite2-city-ipv6`. Only the
applicable one answers for a given address.

```python
v4 = lookup("8.8.8.8")
sorted(v4.raw)          # [..., 'geolite2-city-ipv4', ...]   (no -ipv6 entry)
v6 = lookup("2001:4860:4860::8888")
sorted(v6.raw)          # [..., 'geolite2-city-ipv6', ...]
```

`info.meta["databases"][i]["variant"]` and `["ip_versions"]` expose the same
information for auditing.

## Field notes

| field | meaning |
|---|---|
| `found` | at least one database had a record for this address |
| `network` / `networks` | most specific CIDR from all datasets / per-dataset CIDR |
| `flags` | computed with the standard library, never from a database |
| `location` | `latitude`/`longitude` are database centroids; `accuracy_radius` is only present when the source provides it (GeoLite2 does, DB-IP Lite does not) |
| `postal`, `time_zone` | `null` unless the loaded datasets provide them |
| `asn.organization` | operator name as spelled by the winning database |
| `traits` | vendor extras (connection type, anycast flags, domain, user type, ...) merged from every dataset |
| `display` | `"City, Subdivision, Country"` in the requested locale order |
| `cross_check` | every source's answer for the comparable fields: `country`, `country_name`, `continent`, `subdivision`, `city`, `postal`, `asn`, `asn_organization`, `location` |
| `agreement` | per field: did every source that answered agree? Single-source fields count as agreed |
| `conflicts` | sorted list of fields where sources disagree - read these before trusting a value |
| `raw` | untouched per-file records |

## Language handling

* `locales` (default `("en",)`) decides which value fills `name` / `display`.
* `names` maps keep every language the source ships by default - set
  `include_all_names=False` (or pass `all_names=False` to `to_dict`) when you
  need a strictly English payload of a fixed size.
* Language fallback: `zh-CN` request on a database that only has `zh` still matches.

## Trimming for size

```python
Detector(include_raw=False, include_cross_check=False, include_all_names=False)
```

A default lookup of `8.8.8.8` serializes to ~10 KB, almost all of it `raw` and
the multilingual `names` maps. The trimmed version is a few hundred bytes.

## Flattening

```python
info.to_flat_dict()
# {'ip': '8.8.8.8', 'flags.is_private': False, 'country.iso_code': 'US',
#  'location.latitude': 37.422, 'subdivisions.0.name': 'California', ...}
```

Nested lists become indexed keys, so the whole document fits a columnar store
(Parquet, ClickHouse, BigQuery) without hand-written unpacking.
