"""Every bundled dataset in one lookup - the 60-second tour.

    python examples/quickstart_sync.py
"""

from __future__ import annotations

import json

from detector import Detector, distance, lookup


def main() -> None:
    # 1. Module-level helpers: a shared client is created on first use.
    info = lookup("8.8.8.8")
    print(f"{info.ip}  IPv{info.version}  found={info.found}")
    print(f"  location : {info.display}")
    print(f"  country  : {info.country.iso_code} ({info.country.name()})")
    print(f"  asn      : {info.asn.asn} {info.asn.organization}")
    print(f"  coords   : {info.coordinates}")
    print(f"  network  : {info.network}")

    # 2. Which database said what - conflicts surface instead of being hidden.
    print("\nsources:")
    for key, name in info.sources.items():
        print(f"  {key:<18} {name}")
    print("cross-check (country):")
    for source, value in info.cross_check["country"].items():
        print(f"  {source:<20} {value}")
    print(f"agreement: {json.dumps(info.agreement)}")
    print(f"conflicts: {info.conflicts}")

    # 3. Full standard document, including every raw per-database record.
    document = info.to_dict()
    print(f"\ntop-level keys: {list(document)}")
    print(f"raw records  : {list(document['raw'])}")
    print(f"document size: {len(info.to_json())} bytes")

    # 4. A tuned client: English only, no raw payloads, no cross-check.
    with Detector(include_all_names=False, include_raw=False, include_cross_check=False) as lean:
        small = lean.lookup("114.114.114.114")
        print(f"\nlean lookup: {small.to_json()}")

    # 5. Distance, sync flavour.
    row = distance("8.8.8.8", "114.114.114.114")
    print(f"\n{row}  ({row.mi:,.1f} mi, same_country={row.same_country})")


if __name__ == "__main__":
    main()
