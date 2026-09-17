"""Startup / steady-state benchmark: import cost, cold vs warm start, memory.

    python tools/startup_bench.py            # all phases
    python tools/startup_bench.py import     # a single phase

Phases (each one is a fresh process, so the numbers mean something):

    import   cost of `from detector import info, distance` - data untouched
    cold     first call with an empty cache directory (decompresses 76 MB of xz)
    warm     first call with the cache already populated (open + mmap)
    steady   per-call cost once the client is pooled
    memory   RSS with mmap vs load_mode="memory"
"""

from __future__ import annotations

import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

IPS = ["8.8.8.8", "1.1.1.1", "223.5.5.5", "114.114.114.114",
       "2001:4860:4860::8888", "2400:3200::1"]

CACHE = Path(tempfile.gettempdir()) / "detector-bench-cache"


def rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def child(code: str) -> None:
    """Run a snippet in a fresh interpreter and stream its output."""
    env = {**os.environ, "DETECTOR_CACHE_DIR": str(CACHE), "PYTHONPATH": str(ROOT / "src")}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


IMPORT_SNIPPET = """
import time, os, json
started = time.perf_counter()
from detector import info, distance
import detector
elapsed = (time.perf_counter() - started) * 1000
cache = os.environ["DETECTOR_CACHE_DIR"]
print(json.dumps({
    "import_ms": round(elapsed, 1),
    "cache_dir_exists": os.path.isdir(cache),
    "cache_entries": len(os.listdir(cache)) if os.path.isdir(cache) else 0,
    "rss_mb": round(__import__("resource").getrusage(__import__("resource").RUSAGE_SELF).ru_maxrss / 1024, 1),
}))
"""

FIRST_CALL_SNIPPET = """
import asyncio, json, time, os, resource
from detector import info
started = time.perf_counter()
row = asyncio.run(info("8.8.8.8"))
first_ms = (time.perf_counter() - started) * 1000
print(json.dumps({
    "first_call_ms": round(first_ms, 1),
    "rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
    "result": f"{row.country_code} {row.city_name}",
}))
"""

STEADY_SNIPPET = """
import asyncio, json, time, resource
from detector import info, close
from detector.functions import _client

async def main():
    # warm the client
    await info("8.8.8.8")

    rounds = 20
    per_round = 50
    started = time.perf_counter()
    for _ in range(rounds):
        await info(["8.8.8.8", "1.1.1.1", "223.5.5.5", "114.114.114.114"])
    batch_us = (time.perf_counter() - started) / (rounds * 4) * 1e6

    first = await _client()
    second = await _client()
    third = await _client(datasets=["dbip-city"])

    started = time.perf_counter()
    for _ in range(2000):
        await info("8.8.8.8")
    cached_us = (time.perf_counter() - started) / 2000 * 1e6

    await close()
    print(json.dumps({
        "batch_fresh_us_per_ip": round(batch_us, 1),
        "single_cached_us": round(cached_us, 1),
        "client_pooled": first is second,
        "distinct_option_sets_get_own_client": first is not third,
        "rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
    }))

asyncio.run(main())
"""

MEMORY_SNIPPET = """
import asyncio, json, resource
from detector import info, close
from detector.functions import _client

async def main():
    await info("8.8.8.8")
    mmap_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    await close()

    client = await _client(load_mode="memory", include_raw=False)
    await client.lookup("8.8.8.8")
    memory_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    ips = ["8.8.8.8", "1.1.1.1", "223.5.5.5", "114.114.114.114",
           "2001:4860:4860::8888", "2400:3200::1"]
    info_rows = await info(ips * 100, include_raw=False)
    heavy_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    await close()
    print(json.dumps({
        "rss_mmap_mb": round(mmap_rss, 1),
        "rss_memory_mode_mb": round(memory_rss, 1),
        "rss_after_600_results_mb": round(heavy_rss, 1),
        "rows": len(info_rows),
    }))

asyncio.run(main())
"""


def phase_import() -> None:
    print("\n[1] import only - does `from detector import info, distance` touch the data?")
    for _ in range(3):
        child(IMPORT_SNIPPET)


def phase_cold() -> None:
    print("\n[2] cold start - empty cache directory, first call decompresses the datasets")
    shutil.rmtree(CACHE, ignore_errors=True)
    started = time.perf_counter()
    child(FIRST_CALL_SNIPPET)
    print(f"    (wall clock for the whole process: {time.perf_counter() - started:.1f}s)")
    print(f"    cache now holds {len(list((CACHE / 'extracted').glob('*.mmdb')))} mmdb files, "
          f"{sum(p.stat().st_size for p in (CACHE / 'extracted').glob('*.mmdb')) / 1e6:.0f} MB")


def phase_warm() -> None:
    print("\n[3] warm start - cache populated, so only open + mmap")
    for _ in range(3):
        child(FIRST_CALL_SNIPPET)


def phase_steady() -> None:
    print("\n[4] steady state - client pooled, repeated calls")
    child(STEADY_SNIPPET)


def phase_memory() -> None:
    print("\n[5] memory - mmap (default) vs load_mode='memory'")
    child(MEMORY_SNIPPET)


PHASES = {
    "import": phase_import,
    "cold": phase_cold,
    "warm": phase_warm,
    "steady": phase_steady,
    "memory": phase_memory,
}


def main(argv: list) -> int:
    selected = [name for name in argv if name in PHASES] or list(PHASES)
    print(f"python {sys.version.split()[0]} | cache: {CACHE}")
    for name in selected:
        PHASES[name]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
