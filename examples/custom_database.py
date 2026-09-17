"""Feed your own databases: GeoLite2, GeoIP2 or a hand-built MMDB.

    python examples/custom_database.py
"""

from __future__ import annotations

import shutil
from pathlib import Path

from detector import Detector, known_datasets


def main() -> None:
    registry = known_datasets()
    print("bundled by default:")
    for key, entry in registry.items():
        if entry["bundled"]:
            print(f"  {key:<18} {entry['name']:<18} {entry['license']:<10} {entry['update']}")
    print("bundled but NOT redistributable (do not republish these):")
    for key, entry in registry.items():
        if entry["bundled"] and not entry["redistributable"]:
            print(f"  {key:<18} {entry['name']:<18} {entry['license']}")

    # Drop them for a wheel you intend to publish:
    #   from detector import NON_REDISTRIBUTABLE_KEYS, Detector
    #   publishable = Detector(exclude=NON_REDISTRIBUTABLE_KEYS)

    # --- Option 1: point the client at files you are licensed to hold ---------
    # geoip2 = Detector(databases={
    #     "city": "/data/GeoLite2-City.mmdb",
    #     "asn":  "/data/GeoLite2-ASN.mmdb",
    # })

    # --- Option 2: add an extra database next to the bundled ones -------------
    # Any MMDB v2.0 file works, even one with a schema nobody has seen before:
    # unmapped keys are preserved in `traits`, the whole record in `raw`.
    extra = Path("build/example-asn.mmdb")
    extra.parent.mkdir(exist_ok=True)
    bundled = Path(__file__).resolve().parents[1] / "src/detector/data/dbip-asn-lite.mmdb.gz"
    if not extra.exists():
        import gzip

        with gzip.open(bundled, "rb") as src, extra.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    with Detector(databases={"my-asn": extra}, cache_size=0, include_cross_check=False) as custom:
        info = custom.lookup("8.8.8.8")
        print(f"\ncustom database -> asn={info.asn.asn} {info.asn.organization}")
        print(f"  sources : {info.sources}")
        print(f"  raw keys: {list(info.raw['my-asn'])}")
        print(f"  describe: {custom.describe()['databases'][0]['name']}")

    # --- Option 3: rebuild the dataset set by selection ----------------------
    with Detector(datasets=["dbip-city", "dbip-asn"], cache_size=0) as partial:
        info = partial.lookup("8.8.8.8")
        print(f"\nselected datasets: {partial.dataset_keys}")
        print(f"  country={info.country.iso_code} asn={info.asn.asn} cross_check={list(info.cross_check)}")


if __name__ == "__main__":
    main()
