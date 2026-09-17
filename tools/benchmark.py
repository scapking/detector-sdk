"""Benchmark harness - reproduces the numbers quoted in docs/07-performance.md.

    python tools/benchmark.py                # full run
    python tools/benchmark.py --quick        # skip the heavy batches
    python tools/benchmark.py --unique 5000  # size of the synthetic workload
"""

from __future__ import annotations

import random
import resource
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from detector import Detector  # noqa: E402

KNOWN = [
    "8.8.8.8",
    "1.1.1.1",
    "223.5.5.5",
    "114.114.114.114",
    "2001:4860:4860::8888",
    "2400:3200::1",
    "2a00:1450:4001:80e::200e",
    "101.226.4.6",
]


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def synthetic(count: int, seed: int = 7) -> list:
    random.seed(seed)
    addresses = []
    for _ in range(count):
        octets = [random.randint(1, 223)] + [random.randint(0, 255) for _ in range(2)]
        octets.append(random.randint(1, 254))
        addresses.append(".".join(str(part) for part in octets))
    return addresses


def bench(label: str, func, count: int) -> float:
    started = time.perf_counter()
    func()
    elapsed = (time.perf_counter() - started) / count
    print(f"{label:<52} {elapsed * 1e6:9.1f} us/ip {1 / elapsed:9.0f} ip/s")
    return elapsed


def main(argv: list) -> int:
    quick = "--quick" in argv
    unique_count = 2000
    if "--unique" in argv:
        unique_count = int(argv[argv.index("--unique") + 1])
    unique = synthetic(unique_count)

    print(f"python {sys.version.split()[0]}  maxminddb reader: ", end="")
    baseline_rss = rss_mb()
    started = time.perf_counter()
    full = Detector(cache_size=0)
    open_ms = (time.perf_counter() - started) * 1000
    reader = type(full.databases[0]._reader).__name__
    print(f"{reader}")
    print(
        f"open {len(full.databases)} database files: {open_ms:.0f} ms   "
        f"RSS {baseline_rss:.0f} -> {rss_mb():.0f} MB\n"
    )

    lean = Detector(cache_size=0, include_raw=False, include_cross_check=False)
    city = Detector(datasets=["dbip-city"], cache_size=0)
    cached = Detector(cache_size=8192)

    # Warm-up: the first pass over a freshly mapped database is I/O bound
    # (page-cache fill on 185 MB of mmap), which would otherwise dominate every
    # number below. Production processes that run for more than a few seconds
    # never see that cost - and if they do, load_mode="memory" removes it.
    print("warming up (page cache)...")
    for detector_instance in (full, lean, city):
        for _ in detector_instance.stream(list(KNOWN)):
            pass
    for _ in full.lookup_many(list(unique)):
        pass
    print()

    files = len(full.databases)
    datasets = len(set(full.dataset_keys))
    bench(
        f"lookup, {datasets} datasets/{files} files, raw + cross_check",
        lambda: [full.lookup(ip) for ip in KNOWN],
        len(KNOWN),
    )
    bench(
        f"lookup, {datasets} datasets, no raw/cross_check",
        lambda: [lean.lookup(ip) for ip in KNOWN],
        len(KNOWN),
    )
    bench("lookup, dbip-city only", lambda: [city.lookup(ip) for ip in KNOWN], len(KNOWN))
    rounds = 50
    bench(
        "lookup, cached (repeat addresses)",
        lambda: [[cached.lookup(ip) for ip in KNOWN] for _ in range(rounds)],
        rounds * len(KNOWN),
    )
    print()
    bench("unique IPs, sequential", lambda: full.lookup_many(list(unique)), len(unique))
    bench("unique IPs, no raw/cross_check", lambda: lean.lookup_many(list(unique)), len(unique))
    bench(
        "unique IPs, streamed (not retained)",
        lambda: [None for _ in full.stream(list(unique))],
        len(unique),
    )
    print()
    bench("distance 1 -> 1", lambda: full.distance("8.8.8.8", "1.1.1.1"), 1)
    targets = list(unique) if quick else list(unique) * 5
    bench(f"distance 1 -> {len(targets)}", lambda: full.distance("8.8.8.8", targets), len(targets))

    document = full.lookup("8.8.8.8")
    print(
        f"\nresult size: {len(document.to_json())} bytes (full) / "
        f"{len(lean.lookup('8.8.8.8').to_json())} bytes (lean)"
    )
    print(f"peak RSS: {rss_mb():.0f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
