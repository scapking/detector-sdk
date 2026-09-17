# 04 - Distance

Distance is computed from the coordinates each database reports. That is the
whole trick, and also the whole limitation.

## Two methods

| method | aliases | maths | cost | when |
|---|---|---|---|---|
| `haversine` | `great-circle`, `spherical` | great circle on a sphere (R = 6371.0088 km) | ~1 µs | default; right answer for IP geolocation error bars |
| `vincenty` | `geodesic`, `ellipsoid` | geodesic on the WGS84 ellipsoid | ~30 µs | when you need sub-percent accuracy and can pay for it |

Measured difference: Beijing -> Shanghai is 1067.31 km (haversine) vs
1069.43 km (vincenty) - 0.2%. IP centroids are off by tens to hundreds of
kilometres, so the default wastes nothing.

```python
from detector import distance, haversine_km, vincenty_km

distance("8.8.8.8", "1.1.1.1")                          # 11953.88 km
distance("8.8.8.8", "1.1.1.1", method="vincenty")        # 11 991-ish km
haversine_km(39.9042, 116.4074, 31.2304, 121.4737)       # 1067.31
```

## 1-to-N, N-to-N

```python
detector.distance("8.8.8.8", "1.1.1.1")                 # Distance
detector.distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])   # list[Distance]
detector.distance("8.8.8.8", (ip for ip in generator))   # unbounded, streamed
detector.distance_many(china_ips, global_ips)            # len(A) * len(B) rows
detector.nearest("223.5.5.5", candidates, limit=3)       # sorted, closest first
```

Every IP is resolved exactly once per call, so `distance_many` over 1 000 x 1 000
addresses performs 2 000 lookups, not a million.

## The result

```python
row = distance("8.8.8.8", "114.114.114.114")
row.km            # 9774.63
row.mi            # 6073.67
row.same_country  # False
row.same_city     # False
row.same_asn      # False
row.same_continent# False
row.method        # 'haversine'
row.source_location  # {'latitude': 37.422, 'longitude': -122.085, ...}
float(row)        # usable in arithmetic
row.to_dict()
```

`same_*` flags exist because they are usually what you actually want to gate on -
"same country" is far more reliable than "less than 500 km" given the error bars.

## When a distance cannot be computed

`km` is `null` and `reason` explains why. No exception is raised, so batch rows
stay aligned with the input:

| reason | meaning |
|---|---|
| `no_location` | one side has no coordinates (private address, no data, or a database without location fields) |
| `invalid_ip` | the address could not be parsed |
| `not_found` | parsed, but no dataset had a record and the address was never resolved |
| `compute_failed: ...` | `vincenty` did not converge (near-antipodal points) |

```python
rows = distance("8.8.8.8", ["192.168.1.1", "nonsense", "1.1.1.1"])
[(row.target, row.km, row.reason) for row in rows]
# [('192.168.1.1', None, 'no_location'), ('nonsense', None, 'invalid_ip'),
#  ('1.1.1.1', 11953.88, None)]
```

## Accuracy reality check

* Coordinates are population centroids per network block, not device positions.
* DB-IP Lite is a free subset: city-level hits are right most of the time,
  and country-level is far more trustworthy than the city name.
* Mobile and anycast networks (Cloudflare, Google DNS, content CDNs) report the
  operator's registered location, which can be thousands of kilometres from the
  user.
* An address with no location data is better than a guessed one; that is why
  `reason` is explicit.

Never use these numbers for identifying a household, a person or a street
address. Neither the licence nor the data supports it.

## Consistency across sources

The distance uses the merged record, which follows the priority order
(`city` > `enterprise` > `asn` > `country`). When you want to know how much the
answer depends on the source, look at the cross-check:

```python
info = detector.lookup("8.8.8.8")
info.cross_check["location"]     # every dataset's idea of where this address is
info.conflicts                   # fields where those datasets disagree
```

Coordinates disagree often - that is exactly the honesty you want before
shipping a "distance < 500 km" rule.
