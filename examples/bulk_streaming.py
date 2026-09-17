"""Bulk work: unbounded streaming, ranking, and matrix distances.

    python examples/bulk_streaming.py
"""

from __future__ import annotations

import itertools
import time

from detector import Detector

CHINA_IPS = ["114.114.114.114", "223.5.5.5", "2400:3200::1", "1.2.4.8", "101.226.4.6"]
GLOBAL_IPS = ["8.8.8.8", "1.1.1.1", "9.9.9.9", "208.67.222.222", "2001:4860:4860::8888"]


def addresses(count: int):
    """A generator standing in for a 10-million-row log file."""
    pool = CHINA_IPS + GLOBAL_IPS
    return (pool[index % len(pool)] for index in range(count))


def main() -> None:
    with Detector(cache_size=8192) as detector:
        # 1. Stream 200k addresses: memory stays flat, cache absorbs the repeats.
        started = time.perf_counter()
        found = 0
        for info in detector.stream(itertools.islice(addresses(200_000), 200_000)):
            found += info.found
        elapsed = time.perf_counter() - started
        print(f"streamed 200k addresses in {elapsed:.2f}s ({found} resolved)")
        print(f"cache: {detector.stats()['cache']}")

        # 2. Closest N out of a big candidate list.
        ranked = detector.nearest("223.5.5.5", GLOBAL_IPS + CHINA_IPS, limit=3)
        print("\nclosest to 223.5.5.5 (Hangzhou):")
        for rank, row in enumerate(ranked, 1):
            print(f"  {rank}. {row.target:<20} {row.km:>10,.1f} km  same_asn={row.same_asn}")

        # 3. Full matrix between two groups.
        rows = detector.distance_many(CHINA_IPS[:3], GLOBAL_IPS[:3])
        print(f"\nmatrix {len(rows)} pairs:")
        for row in rows:
            print(f"  {row.source:<18} -> {row.target:<18} {row.km:>10,.1f} km")

        # 4. Cross-check disagreements across sources, in bulk.
        disagreements = {}
        for ip in CHINA_IPS + GLOBAL_IPS:
            info = detector.lookup(ip)
            for field in info.conflicts:
                disagreements.setdefault(field, []).append(ip)
        print("\nfields where sources disagree:")
        for field, ips in disagreements.items():
            print(f"  {field:<18} {len(ips)} of {len(CHINA_IPS + GLOBAL_IPS)} addresses")


if __name__ == "__main__":
    main()
