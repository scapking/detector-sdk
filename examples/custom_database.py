"""Which datasets exist, and how to plug in your own database.

    python examples/custom_database.py
"""

from __future__ import annotations

import asyncio
import lzma
import shutil
from pathlib import Path

from detector import AsyncDetector, info, known_datasets


async def main() -> None:
    registry = known_datasets()
    print(f"{'key':<18}{'name':<20}{'licence':<28}{'files':<6}redistributable")
    for key, entry in registry.items():
        print(f"{key:<18}{entry['name']:<20}{entry['license']:<28}"
              f"{len(entry['files']):<6}{entry['redistributable']}")

    # Drop the licence-restricted datasets for a build you intend to publish:
    #   from detector import NON_REDISTRIBUTABLE_KEYS
    #   client = Detector(exclude=NON_REDISTRIBUTABLE_KEYS)

    # Your own MMDB (any MaxMind DB v2.0 file) can be used instead of, or next
    # to, the bundled ones. Unknown fields survive in traits/raw.
    extra = Path("build/example-asn.mmdb")
    extra.parent.mkdir(exist_ok=True)
    source = Path(__file__).resolve().parents[1] / "src/detector/data/dbip-asn-lite.mmdb.xz"
    if not extra.exists():
        with lzma.open(source, "rb") as src, extra.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    async with await AsyncDetector.create(databases={"my-asn": extra}, cache_size=0) as client:
        row = await client.lookup("8.8.8.8")
        print(f"\ncustom database -> {row.asn.asn} {row.asn.organization}")
        print(f"  raw keys: {list(row.raw['my-asn'])}")

    # Selection also works through the two public functions.
    partial = await info("8.8.8.8", datasets=["dbip-city"], include_raw=False)
    print(f"\ncity-only lookup -> country={partial.country_code} asn={partial.asn} "
          f"cross_check={list(partial.cross_check)}")


if __name__ == "__main__":
    asyncio.run(main())
