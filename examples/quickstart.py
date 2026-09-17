"""The whole API in one file: two coroutines, single or batch.

    python examples/quickstart.py
"""

from __future__ import annotations

import asyncio
import json

from detector import close, configure, distance, info


async def main() -> None:
    # --- 1. information: one IP -------------------------------------------------
    result = await info("8.8.8.8")
    print(f"{result.ip}  IPv{result.version}  found={result.found}")
    print(f"  location : {result.display}")
    print(f"  country  : {result.country.iso_code} ({result.country.name()})")
    print(f"  asn      : {result.asn.asn} {result.asn.organization}")
    print(f"  coords   : {result.coordinates}")
    print(f"  sources  : {len(result.sources)} databases")

    # --- 2. information: many IPs (same function) -------------------------------
    rows = await info(["8.8.8.8", "1.1.1.1", "114.114.114.114", "2001:4860:4860::8888"])
    for row in rows:
        print(f"  {row.ip:<26} {row.country_code} {row.city_name:<15} {row.asn.asn}")

    # --- 3. the standard JSON document -----------------------------------------
    document = await info("8.8.8.8", as_dict=True)
    print(f"\njson keys   : {len(document)} top-level fields")
    print(f"cross-check : {json.dumps(document['cross_check']['country'])}")
    print(f"conflicts   : {document['conflicts']}")
    print(f"document    : {len(json.dumps(document))} bytes (raw + every language)")

    # --- 4. distance: one pair --------------------------------------------------
    row = await distance("8.8.8.8", "114.114.114.114")
    print(f"\n{row}  ({row.mi:,.1f} mi, same_country={row.same_country})")

    # --- 5. distance: one to N, or N to M --------------------------------------
    for item in await distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"]):
        print(f"  {item.target:<20} {item.km:>10,.1f} km  same_asn={item.same_asn}")

    for item in await distance(["8.8.8.8", "1.1.1.1"], ["223.5.5.5"]):
        print(f"  {item.source:<12} -> {item.target:<12} {item.km:>10,.1f} km")

    # --- 6. options: per call, or once ----------------------------------------
    lean = await info("8.8.8.8", include_raw=False, include_all_names=False, as_dict=True)
    print(f"\nlean document: {len(json.dumps(lean))} bytes")

    configure(locales=("en",), include_raw=False)
    try:
        print(f"configured   : {(await info('8.8.8.8')).city_name}")
    finally:
        configure()

    await close()


if __name__ == "__main__":
    asyncio.run(main())
