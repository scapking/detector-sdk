"""The whole API in one file: two coroutines, single or batch, JSON by default.

    python examples/quickstart.py
"""

from __future__ import annotations

import asyncio
import json

from detector import close, configure, distance, info

IPS = ["8.8.8.8", "1.1.1.1", "114.114.114.114", "2001:4860:4860::8888"]


async def main() -> None:
    # --- 1. one IP: the return value is the standard JSON document -------------
    row = await info("8.8.8.8")
    print(type(row).__name__, "-", len(row), "top-level keys")
    print(f"  ip       : {row['ip']}  IPv{row['version']}  found={row['found']}")
    print(f"  location : {row['display']}")
    print(f"  country  : {row['country']['iso_code']} ({row['country']['name']})")
    print(f"  asn      : {row['asn']['asn']} {row['asn']['organization']}")
    print(f"  coords   : {row['location']['latitude']}, {row['location']['longitude']}")
    print(f"  sources  : {len(row['sources'])} databases")

    # --- 2. same function, a batch --------------------------------------------
    for item in await info(IPS):
        country = item["country"]["iso_code"] if item["country"] else "-"
        city = item["city"]["name"] if item["city"] else "-"
        asn = item["asn"]["asn"] if item["asn"] else "-"
        print(f"  {item['ip']:<26} {country:<4} {city:<16} {asn}")

    # --- 3. cross-check: what every database says about the same address ------
    print(f"\n  country votes : {json.dumps(row['cross_check']['country'])}")
    print(f"  agent names   : {json.dumps(row['cross_check']['asn_organization'])}")
    print(f"  agreement     : {json.dumps(row['agreement'])}")
    print(f"  conflicts     : {row['conflicts']}")

    # --- 4. distance: one pair -------------------------------------------------
    pair = await distance("8.8.8.8", "114.114.114.114")
    print(f"\n  {pair['source']} -> {pair['target']}: {pair['distance_km']:,.2f} km "
          f"({pair['distance_mi']:,.1f} mi)")

    # --- 5. distance: one to N, N to M, alternative maths ---------------------
    for item in await distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"]):
        print(f"  {item['target']:<20} {item['distance_km']:>10,.1f} km  same_asn={item['same_asn']}")
    for item in await distance(["8.8.8.8", "1.1.1.1"], ["223.5.5.5"], method="vincenty"):
        print(f"  {item['source']:<12} -> {item['target']:<12} {item['distance_km']:>10,.1f} km "
              f"({item['method']})")

    # --- 6. model objects when you prefer attributes --------------------------
    model = await info("8.8.8.8", as_object=True)
    print(f"\n  as_object : {type(model).__name__} {model.country.iso_code} "
          f"{model.city.name()} {model.asn.asn} {model.coordinates}")
    far = await distance("8.8.8.8", "1.1.1.1", as_object=True)
    print(f"  as_object : {type(far).__name__} {far.km:,.2f} km ({far.mi:,.2f} mi) {far!s}")

    # --- 7. options: per call, or once ---------------------------------------
    lean = await info("8.8.8.8", include_raw=False, include_all_names=False)
    print(f"\n  lean document: {len(json.dumps(lean))} bytes (vs {len(json.dumps(row))})")

    configure(locales=("en",), include_raw=False)
    try:
        print(f"  configured   : {(await info('8.8.8.8'))['city']['name']}")
    finally:
        configure()

    await close()


if __name__ == "__main__":
    asyncio.run(main())
