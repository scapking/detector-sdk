"""Async usage: concurrent batches, streaming, and non-blocking lookups.

    python examples/async_batch.py
"""

from __future__ import annotations

import asyncio
import time

from detector import AsyncDetector, adistance, alookup

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


async def main() -> None:
    # 1. One-off coroutine helpers (shared client under the hood).
    info = await alookup("8.8.8.8")
    print(f"alookup -> {info.ip} {info.country.iso_code} {info.city.name()}")

    # 2. Own client, tuned concurrency and window size.
    async with await AsyncDetector.create(max_concurrency=32, window=256) as detector:
        started = time.perf_counter()
        results = await detector.lookup_many(IPS)
        elapsed = time.perf_counter() - started
        print(f"lookup_many({len(IPS)}) in {elapsed*1000:.1f} ms")
        for item in results:
            if item.found:
                status = f"{item.country.iso_code} {item.display}"
            else:
                status = f"error={item.meta.get('error', {}).get('code')}"
            print(f"  {item.ip:<26} {status}")

        # 3. Stream millions of addresses with flat memory usage.
        async def source():
            for _ in range(3):
                for ip in IPS[:3]:
                    yield ip

        streamed = [item.ip async for item in detector.stream(source())]
        print(f"\nstreamed {len(streamed)} records: {streamed}")

        # 4. Distances, including a generator of targets.
        rows = await detector.distance("8.8.8.8", (ip for ip in IPS[:5]))
        for row in rows:
            distance_text = f"{row.km:>10,.1f} km" if row.available else f"n/a ({row.reason})"
            print(f"  -> {row.target:<26} {distance_text}")

        ranked = await detector.nearest("223.5.5.5", IPS[:6], limit=3)
        print("\nclosest to 223.5.5.5:")
        for rank, row in enumerate(ranked, 1):
            print(f"  {rank}. {row.target:<26} {row.km:,.1f} km")

        # 5. Multiprocessing for big batches: real parallelism, same results.
        bulk = IPS[:6] * 200
        started = time.perf_counter()
        parallel = await detector.lookup_many(bulk, processes=2)
        elapsed = time.perf_counter() - started
        print(f"\nprocesses=2: {len(parallel)} records in {elapsed*1000:.0f} ms")

    one = await adistance("8.8.8.8", "1.1.1.1")
    print(f"\nadistance -> {one}")


if __name__ == "__main__":
    asyncio.run(main())
