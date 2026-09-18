"""Test-only fixtures: materialise real database bytes from the bundled `.bz`.

The bundled data ships as lazy block containers, not archives, so archive-level
tests (extraction, parts, manifest, load-timeout) need synthetic archives. These
helpers unpack a bundled `.bz` back to raw MMDB bytes and re-compress them in
whatever form a test needs.
"""

from __future__ import annotations

import gzip
import lzma
import struct
from pathlib import Path

from detector.databases import package_data_dir
from detector.mmdb_lazy import LazyBuffer

# The smallest bundled databases (fast to duplicate for fixture purposes).
SMALL = [
    "iptoasn-country",
    "server-country",
    "dbip-asn-lite",
    "user-country",
]


def raw_bytes(stem: str) -> bytes:
    """The uncompressed MMDB behind a bundled ``<stem>.mmdb.bz`` (streamed in blocks)."""
    buffer = LazyBuffer(package_data_dir() / f"{stem}.mmdb.bz", cache_blocks=1 << 10)
    try:
        return bytes(buffer[:buffer.size()])
    finally:
        buffer.close()


def write_raw(stem: str, directory: Path) -> Path:
    """Write ``<stem>.mmdb`` into ``directory``; return its path."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{stem}.mmdb"
    target.write_bytes(raw_bytes(stem))
    return target


def write_archive(stem: str, directory: Path, codec: str = "xz") -> Path:
    """Write ``<stem>.mmdb.<codec>`` (an archive) into ``directory``."""
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{stem}.mmdb.{codec}"
    data = raw_bytes(stem)
    if codec == "xz":
        compressed = lzma.compress(data, preset=9 | lzma.PRESET_EXTREME)
    elif codec == "gz":
        compressed = gzip.compress(data, compresslevel=6)
    else:  # pragma: no cover
        raise ValueError(f"unsupported test codec: {codec}")
    target.write_bytes(compressed)
    return target


def blocks_of(stem: str) -> tuple[int, int]:
    """``(block_size, block_count)`` from a bundled ``.bz`` container header."""
    with open(package_data_dir() / f"{stem}.mmdb.bz", "rb") as handle:
        head = handle.read(20)
    _magic, block_size, block_count, _raw = struct.unpack(">8sIII", head)
    return block_size, block_count