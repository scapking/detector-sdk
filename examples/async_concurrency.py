"""Large inputs: the same two functions, bounded memory, real concurrency.

    python examples/async_concurrency.py
"""

from __future__ import annotations

import asyncio
import time

from detector import AsyncDetector, info

IPS = [
    "8.8.8.8",
    "1.1.1.1",
    "114.114.114.114",
    "223.5.5.5",
    "2001:4860:4860::8888",
    "2400:3200::1",
    "2a00:1450:4001:80e::200e",
    "not-an-ip",
]


def addresses(count: int):
    """Stands in for a 10-million-line log file."""
    pool = IPS[:6]
    return (pool[index % len(pool)] for index in range(count))


async def main() -> None:
    # 1. info() accepts a generator: work happens in windows, memory stays flat.
    started = time.perf_counter()
    rows = await info(addresses(20_000), window=512)
    elapsed = time.perf_counter() - started
    print(f"info(20k addresses) -> {len(rows)} records in {elapsed:.2f}s "
          f"({elapsed / len(rows) * 1e6:.0f} us/ip), {sum(r.found for r in rows)} resolved")

    # 2. AsyncDetector when you want explicit control over the client.
    async with await AsyncDetector.create(max_concurrency=32, window=1024, cache_size=8192) as client:
        rows = await client.lookup_many(IPS)
        for row in rows:
            state = f"{row.country_code} {row.city_name}" if row.found else "error"
            print(f"  {row.ip:<26} {state}")

        # 3. Distances from one address to many, then the closest ones.
        ranked = await client.nearest("223.5.5.5", IPS[:6], limit=3)
        print("\nclosest to 223.5.5.5:")
        for rank, row in enumerate(ranked, 1):
            print(f"  {rank}. {row.target:<24} {row.km:>10,.1f} km")

        # 4. Shard across OS processes for CPU-bound bulk work: every worker opens
        #    its own memory maps, the OS page cache is shared.
        print(f"\nclient stats: {await client.stats()}")


if __name__ == "__main__":
    asyncio.run(main())
