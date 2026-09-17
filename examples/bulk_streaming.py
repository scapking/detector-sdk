"""Bulk work with the two functions: generators, ranking, matrices.

    python examples/bulk_streaming.py
"""

from __future__ import annotations

import asyncio
import itertools
import time

from detector import AsyncDetector, info

CHINA = ["114.114.114.114", "223.5.5.5", "2400:3200::1", "1.2.4.8", "101.226.4.6"]
GLOBAL = ["8.8.8.8", "1.1.1.1", "9.9.9.9", "208.67.222.222", "2001:4860:4860::8888"]


def addresses(count: int):
    pool = CHINA + GLOBAL
    return (pool[index % len(pool)] for index in range(count))


async def main() -> None:
    # 1. 200k addresses through one call: a generator, so memory stays flat and
    #    the repeat addresses hit the LRU cache.
    started = time.perf_counter()
    rows = await info(itertools.islice(addresses(200_000), 200_000), cache_size=8192)
    elapsed = time.perf_counter() - started
    print(f"info(200k) -> {len(rows)} records in {elapsed:.2f}s "
          f"({elapsed / len(rows) * 1e6:.0f} us/ip)")

    # 2. Ranking needs the client object (nearest is a client method, not a
    #    separate public function).
    async with await AsyncDetector.create(cache_size=8192) as client:
        ranked = await client.nearest("223.5.5.5", GLOBAL + CHINA, limit=3)
        print("\nclosest to 223.5.5.5:")
        for rank, row in enumerate(ranked, 1):
            print(f"  {rank}. {row.target:<20} {row.km:>10,.1f} km  same_asn={row.same_asn}")

        # 3. Cross-check disagreements across sources, in bulk.
        disagreements: dict = {}
        for row in await client.lookup_many(CHINA + GLOBAL):
            for field in row.conflicts:
                disagreements.setdefault(field, []).append(row.ip)
        print("\nfields where sources disagree:")
        for field, ips in disagreements.items():
            print(f"  {field:<18} {len(ips)} of {len(CHINA + GLOBAL)} addresses")


if __name__ == "__main__":
    asyncio.run(main())
