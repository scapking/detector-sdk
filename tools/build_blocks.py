"""Build the block containers (``.bz``) shipped with the SDK.

    python tools/build_blocks.py                          # every bundled dataset
    python tools/build_blocks.py --block-kb 256 --only dbip-city-lite

Input is any raw ``.mmdb`` in the data directory or the extraction cache. A
``.bz`` container stores the MMDB in independent zstd blocks plus an index; the
lazy reader (:func:`detector.mmdb_lazy.open_lazy`) decompresses only the blocks a
lookup touches, so the SDK never has to materialise the full 250 MB on disk.

The build verifies block-level round-trips (raw block bytes == decompressed
block) and records the whole-file sha256 at ``DET_BLOCK`` level so the runtime
can cross-check provenance.
"""

from __future__ import annotations

import hashlib
import json
import struct
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import zstandard

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from detector.databases import (  # noqa: E402
    BUILTIN_DATASETS,
    package_data_dir,
    user_cache_dir,
)

DATA_DIR = ROOT / "src" / "detector" / "data"
DEFAULT_BLOCK_KB = 256
MAGIC = b"DETBLK01"
HEAD_FMT = struct.Struct(">8sIII")
INDEX_FMT = struct.Struct(">I")


def build_blocks(mmdb: Path, out: Path, block_bytes: int) -> dict:
    """Cut ``mmdb`` into independently compressed blocks; return the manifest row."""
    data = mmdb.read_bytes()  # the biggest bundled DB is 127 MB - fits in memory
    raw_digest = hashlib.sha256(data).hexdigest()

    # One compressor per thread: sharing a single zstandard object across worker
    # threads crashes the interpreter (segfault in backend_c).
    import threading as _threading

    _local = _threading.local()

    def _compress(chunk: bytes) -> bytes:
        comp = getattr(_local, "compressor", None)
        if comp is None:
            comp = _local.compressor = zstandard.ZstdCompressor(level=19)
        return comp.compress(chunk)

    chunks = [data[i:i + block_bytes] for i in range(0, len(data), block_bytes)]
    workers = max(2, (Path("/proc/cpuinfo").read_text().count("processor") or 2))
    with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as pool:
        blocks = list(pool.map(_compress, chunks))

    header = HEAD_FMT.pack(MAGIC, block_bytes, len(blocks), len(data))
    lengths = b"".join(INDEX_FMT.pack(len(blob)) for blob in blocks)

    # Verify each block round-trips before trusting the file.
    for index, (chunk, blob) in enumerate(zip(chunks, blocks)):
        if zstandard.ZstdDecompressor().decompress(blob) != chunk:
            raise SystemExit(f"block {index} failed round-trip for {mmdb.name}")

    out.write_bytes(header + lengths + b"".join(blocks))
    return {
        "file": out.name,
        "codec": "bz",
        "size": len(data),
        "packaged_size": out.stat().st_size,
        "block_size": block_bytes,
        "blocks": len(blocks),
        "sha256": raw_digest,
    }


def parse_args(argv: list) -> dict:
    options = {"block_kb": DEFAULT_BLOCK_KB, "only": None, "out": DATA_DIR}
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--block-kb":
            index += 1
            options["block_kb"] = int(argv[index])
        elif item == "--only":
            index += 1
            options["only"] = argv[index]
        elif item == "--out":
            index += 1
            options["out"] = Path(argv[index])
        else:
            raise SystemExit(f"unknown option: {item}")
        index += 1
    return options


def raw_for(member_name: str) -> Path:
    """The raw mmdb behind a bundled member filename (cache/extracted first)."""
    for where in (user_cache_dir() / "extracted", package_data_dir()):
        stem = member_name
        for suffix in (".mmdb.zst", ".mmdb.gz", ".mmdb.xz", ".mmdb"):
            if stem.endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        candidate = where / f"{stem}.mmdb"
        if candidate.is_file():
            return candidate
    raise SystemExit(f"raw mmdb not found for {member_name} (run the data build first)")


def main(argv: list) -> int:
    options = parse_args(argv)
    options["out"].mkdir(parents=True, exist_ok=True)
    wanted = options["only"]
    manifest_read = {}
    man = options["out"] / "MANIFEST.json"
    if man.is_file():
        try:
            manifest_read = json.loads(man.read_text())
        except Exception:
            manifest_read = {}
    entries = manifest_read.get("datasets", [])

    started = time.perf_counter()
    count = 0
    for key, spec in BUILTIN_DATASETS.items():
        if wanted and wanted not in key:
            continue
        for member in spec.members:
            raw = raw_for(member.filename)
            out = options["out"] / f"{member_stem(member)}.mmdb.bz"
            report = build_blocks(raw, out, int(options["block_kb"] * 1024))
            print(f"{key:<16}{out.name:<34}{report['size']/1e6:6.1f} MB raw -> "
                  f"{report['packaged_size']/1e6:5.1f} MB in {report['blocks']} blocks "
                  f"({out.stat().st_size/raw.stat().st_size*100:.0f}%) sha256={report['sha256'][:10]}…")
            # 更新 manifest 里该 key 的行为记录
            for entry in entries:
                if entry.get("key") == key and entry.get("variant") == member.variant:
                    entry.update(report)
            count += 1

    manifest_read["datasets"] = [e for e in entries if "file" in e]
    for entry in manifest_read["datasets"]:
        entry.pop("part_sizes", None)
        entry.pop("parts", None)
        entry["codec"] = entry.get("codec", "bz")
    (options["out"] / "MANIFEST.json").write_text(
        json.dumps(manifest_read, ensure_ascii=False, indent=2)
    )
    print(f"\n{count} databases -> {options['out'].resolve()} in {time.perf_counter()-started:.1f}s")
    return 0


def member_stem(member: object) -> str:
    import re as _re

    name = getattr(member, "filename", str(member))
    return _re.sub(r"\.(mmdb\.(zst|gz|xz)|mmdb)$", "", name)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))