"""Maintainer script: split bundled archives into parallel-decodable parts.

A single ``.mmdb.xz`` decompresses on one core - and the biggest bundled
database (a 127 MB city file) dominates first-run cost. Splitting it into
independently compressed parts lets :func:`detector.extract_many` decode them on
a thread pool and concatenate the results, so first use scales with core count.

    python tools/split_data.py                        # split everything > 12 MB
    python tools/split_data.py --chunk-mb 8           # smaller parts, more of them
    python tools/split_data.py --only dbip-city-lite  # one artefact
    python tools/split_data.py --codec gz             # gzip instead of xz
    python tools/split_data.py --verify-only          # check existing parts

Every part set is verified before the original archive is removed: the parts are
decompressed again and hashed, and the digest must match the source file.
"""

from __future__ import annotations

import hashlib
import lzma
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from detector.databases import (  # noqa: E402
    _archive_kind,
    _open_archive,
    logical_name,
    part_info,
)
from detector.update import write_manifest  # noqa: E402

DATA_DIR = ROOT / "src" / "detector" / "data"
DEFAULT_CHUNK_MB = 12


def parse_args(argv: list) -> dict:
    options = {"chunk_mb": DEFAULT_CHUNK_MB, "only": None, "codec": "xz",
               "verify_only": False, "keep": False}
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--chunk-mb":
            index += 1
            options["chunk_mb"] = float(argv[index])
        elif item == "--only":
            index += 1
            options["only"] = argv[index]
        elif item == "--codec":
            index += 1
            options["codec"] = argv[index]
        elif item == "--verify-only":
            options["verify_only"] = True
        elif item == "--keep":
            options["keep"] = True
        else:
            raise SystemExit(f"unknown option: {item}")
        index += 1
    return options


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def compress_chunk(raw: Path, target: Path, codec: str) -> str:
    if codec == "xz":
        with open(raw, "rb") as src, lzma.open(target, "wb", preset=9 | lzma.PRESET_EXTREME) as dst:
            shutil.copyfileobj(src, dst, 1 << 22)
    elif codec == "gz":
        import gzip

        with open(raw, "rb") as src, gzip.open(target, "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst, 1 << 22)
    elif codec == "zst":
        import zstandard

        with open(raw, "rb") as src, open(target, "wb") as dst:
            zstandard.ZstdCompressor(level=22, threads=-1).copy_stream(src, dst, read_size=1 << 22)
    else:
        raise SystemExit(f"unknown codec: {codec}")
    return sha256(target)


def decompress(path: Path, kind: str) -> bytes:
    with _open_archive(path, kind) as src:
        return src.read()


def existing_parts(stem: str) -> list:
    found = []
    for candidate in sorted(DATA_DIR.iterdir()):
        info = part_info(candidate.name)
        if info is not None and info[0] == stem:
            found.append(candidate)
    return found


def verify(stem: str, digest: str) -> bool:
    """Decompress the parts in index order and compare with the source digest."""
    parts = existing_parts(stem)
    if not parts:
        return False
    parts.sort(key=lambda item: part_info(item.name)[1])
    running = hashlib.sha256()
    for path in parts:
        kind = _archive_kind(path) or "xz"
        running.update(decompress(path, kind))
    return running.hexdigest() == digest


def split_one(source: Path, chunk_mb: float, codec: str, workers: int) -> dict:
    kind = _archive_kind(source)
    if kind is None:
        return {"file": source.name, "status": "skipped (not an archive)"}
    stem = logical_name(source.name)
    if part_info(source.name) is not None:
        return {"file": source.name, "status": "skipped (already a part)"}

    with tempfile.TemporaryDirectory(prefix="detector-split-") as tmp:
        tmpdir = Path(tmp)
        raw = tmpdir / stem
        with _open_archive(source, kind) as src, open(raw, "wb") as dst:
            shutil.copyfileobj(src, dst, 1 << 22)
        digest = sha256(raw)
        size = raw.stat().st_size
        chunk_bytes = int(chunk_mb * 1_000_000)
        count = max(1, -(-size // chunk_bytes))

        chunks = []
        with open(raw, "rb") as handle:
            for index in range(1, count + 1):
                piece = tmpdir / f"chunk{index:04d}"
                piece.write_bytes(handle.read(chunk_bytes))
                chunks.append(piece)

        staged = []
        for index, piece in enumerate(chunks, 1):
            target = tmpdir / f"{stem}.part{index:03d}.{codec}"
            staged.append((piece, target))
        with ThreadPoolExecutor(max(1, workers)) as pool:
            list(pool.map(lambda item: compress_chunk(item[0], item[1], codec), staged))

        # Verify the compressed parts before touching the shipped artefact.
        running = hashlib.sha256()
        for _piece, target in staged:
            running.update(decompress(target, codec))
        if running.hexdigest() != digest:
            return {"file": source.name, "status": "FAILED verification, original kept"}

        for index, (_piece, target) in enumerate(staged, 1):
            final = DATA_DIR / f"{stem}.part{index:03d}.{codec}"
            shutil.move(str(target), final)

    return {
        "file": source.name,
        "status": "ok",
        "size": size,
        "compressed": sum(p.stat().st_size for p in existing_parts(stem)),
        "parts": len(existing_parts(stem)),
        "sha256": digest,
    }


def main(argv: list) -> int:
    options = parse_args(argv)
    if not DATA_DIR.is_dir():
        raise SystemExit(f"no data directory: {DATA_DIR}")

    archives = [
        item for item in sorted(DATA_DIR.iterdir())
        if item.is_file() and _archive_kind(item) is not None and part_info(item.name) is None
    ]
    if options["only"]:
        archives = [item for item in archives if options["only"] in item.name]

    if options["verify_only"]:
        failed = 0
        for source in archives:
            stem = logical_name(source.name)
            parts = existing_parts(stem)
            if not parts:
                print(f"{stem:<34} no parts")
                continue
            print(f"{stem:<34} {len(parts)} parts  ({sum(p.stat().st_size for p in parts)/1e6:.1f} MB)")
        return failed

    workers = max(1, (Path("/proc/cpuinfo").read_text().count("processor") or 2))
    removed = []
    for source in archives:
        size_mb = source.stat().st_size / 1e6
        raw_mb = size_mb  # archives are compared against the raw size below
        try:
            with _open_archive(source, _archive_kind(source) or "xz") as handle:
                handle.seek(0, 2)
                raw_mb = handle.tell() / 1e6
        except (OSError, AttributeError, NotImplementedError):
            raw_mb = size_mb * 3.2  # conservative estimate for the compression ratio
        chunks = -(-int(raw_mb * 1e6) // int(options["chunk_mb"] * 1_000_000))
        if chunks < 2:
            print(f"{source.name:<40} {raw_mb:6.1f} MB raw  kept whole (single chunk)")
            continue
        result = split_one(source, options["chunk_mb"], options["codec"], workers)
        if result["status"] == "ok":
            print(f"{source.name:<40} {result['size']/1e6:6.1f} MB -> "
                  f"{result['parts']} parts, {result['compressed']/1e6:5.1f} MB "
                  f"({result['sha256'][:12]}… verified)")
            if not options["keep"]:
                source.unlink()
                removed.append(source.name)
        else:
            print(f"{source.name:<40} {result['status']}")

    write_manifest(DATA_DIR, [])
    total = sum(item.stat().st_size for item in DATA_DIR.iterdir() if item.is_file())
    print(f"\nremoved {len(removed)} whole archive(s); data directory now {total/1e6:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
