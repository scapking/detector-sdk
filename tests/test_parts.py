"""Archive extraction / split-parts machinery, exercised on synthetic archives.

The bundled data ships as lazy ``.bz`` containers (no archives), but the
extraction path still exists for updated/custom data. These tests build small
archives from the bundled block data and verify original-bytes extraction,
parallel decode and merging.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from detector.databases import (
    concat_parts,
    ensure_extracted,
    extract_many,
    logical_name,
    part_info,
)

from ._data import write_archive


def _source_dir(tmp_path: Path, codec: str) -> Path:
    directory = tmp_path / "src"
    write_archive("iptoasn-country", directory, codec=codec)
    return directory


def _split_into_parts(raw: Path, tmp_path: Path, codec: str, chunks: int = 3) -> list:
    """Cut + compress ``raw`` into ``chunks`` independent part files."""
    import gzip
    import lzma

    data = raw.read_bytes()
    step = -(-len(data) // chunks)
    parts = []
    for index in range(chunks):
        piece = data[index * step:(index + 1) * step]
        if not piece:
            break
        target = tmp_path / f"mydata.mmdb.part{index + 1:03d}.{codec}"
        target.parent.mkdir(parents=True, exist_ok=True)
        if codec == "gz":
            with gzip.open(target, "wb", compresslevel=6) as handle:
                handle.write(piece)
        else:
            with lzma.open(target, "wb", preset=1) as handle:
                handle.write(piece)
        parts.append(target)
    return parts


def test_part_info_and_logical_name() -> None:
    assert part_info("dbip-city-lite.mmdb.part003.xz") == ("dbip-city-lite.mmdb", 3, "xz")
    assert part_info("x.mmdb.part2.gz") == ("x.mmdb", 2, "gz")
    assert part_info("dbip-city-lite.mmdb.xz") is None
    assert logical_name("dbip-city-lite.mmdb.part003.xz") == "dbip-city-lite.mmdb"
    assert logical_name("dbip-city-lite.mmdb.xz") == "dbip-city-lite.mmdb"
    assert logical_name("dbip-city-lite.mmdb") == "dbip-city-lite.mmdb"


@pytest.mark.parametrize("codec", ["xz", "gz"])
def test_split_parts_merge_back_byte_identical(tmp_path: Path, codec: str) -> None:
    import gzip
    import lzma

    reader = lzma.open if codec == "xz" else gzip.open
    directory = _source_dir(tmp_path, codec)
    suffix = ".xz" if codec == "xz" else ".gz"
    archive = directory / f"iptoasn-country.mmdb{suffix}"
    raw = Path(tmp_path) / "raw.mmdb"
    with reader(archive, "rb") as src, open(raw, "wb") as dst:
        while True:
            block = src.read(1 << 20)
            if not block:
                break
            dst.write(block)
    digest = hashlib.sha256(raw.read_bytes()).hexdigest()

    parts = _split_into_parts(raw, tmp_path / "parts", codec)
    assert len(parts) >= 2
    resolved = ensure_extracted(parts[1], tmp_path / "cache")
    assert Path(resolved).name == "mydata.mmdb"
    assert hashlib.sha256(Path(resolved).read_bytes()).hexdigest() == digest
    # idempotent, and stable across part input
    assert Path(extract_many([parts[0]], tmp_path / "cache")[0]) == Path(resolved)


def test_concat_parts_matches_single_file(tmp_path: Path) -> None:
    directory = _source_dir(tmp_path, "xz")
    archive = directory / "iptoasn-country.mmdb.xz"
    with __import__("lzma").open(archive, "rb") as src:
        raw = src.read()
    parts = _split_into_parts(Path(tmp_path) / "r.mmdb", tmp_path, "xz", chunks=4)         if False else None
    # write raw then split
    Path(tmp_path, "r.mmdb").write_bytes(raw)
    parts = _split_into_parts(Path(tmp_path) / "r.mmdb", tmp_path / "c", "xz", chunks=4)
    target = tmp_path / "merged.mmdb"
    concat_parts(parts, target)
    assert target.read_bytes() == raw


def test_real_bundled_bz_data_is_fully_queryable() -> None:
    from detector import Detector

    detector = Detector(cache_size=0)
    try:
        assert len(detector.databases) >= 8
        row = detector.lookup("8.8.8.8")
        assert row.found and row.country.iso_code == "US"
        assert "dbip-city" in detector.dataset_keys
    finally:
        detector.close()
