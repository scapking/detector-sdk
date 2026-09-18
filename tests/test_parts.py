"""Split databases: parsing, parallel decoding, merging, warmup."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import lzma
from pathlib import Path

import pytest

from detector import Detector, info, warmup
from detector.databases import (
    BUILTIN_DATASETS,
    concat_parts,
    extract_many,
    logical_name,
    open_databases,
    package_data_dir,
    part_info,
)

SOURCE_KEY = "iptoasn-country"


def _source_archive() -> Path:
    """A small real dataset, either whole or as its first part."""
    data_dir = package_data_dir()
    member = BUILTIN_DATASETS[SOURCE_KEY].members[0]
    whole = data_dir / member.filename
    if whole.is_file():
        return whole
    parts = sorted(
        (item for item in data_dir.iterdir() if part_info(item.name)),
        key=lambda item: part_info(item.name)[1],
    )
    candidates = [
        item for item in parts
        if part_info(item.name)[0] == logical_name(member.filename)
    ]
    assert candidates, f"no bundled data for {SOURCE_KEY}"
    return candidates[0]


def _decode(path: Path) -> bytes:
    kind = path.name.rsplit(".", 1)[-1]
    opener = gzip.open if kind == "gz" else lzma.open
    with opener(path, "rb") as handle:
        return handle.read()


def _split(raw: bytes, tmp_path: Path, codec: str, chunks: int = 3) -> list:
    """Compress ``raw`` into ``chunks`` independent parts."""
    step = -(-len(raw) // chunks)
    parts = []
    for index in range(chunks):
        piece = raw[index * step:(index + 1) * step]
        if not piece:
            break
        target = tmp_path / f"mydata.mmdb.part{index + 1:03d}.{codec}"
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
    assert part_info("x.mmdb.part002.zst") == ("x.mmdb", 2, "zst")
    assert part_info("dbip-city-lite.mmdb.xz") is None
    assert part_info("dbip-city-lite.mmdb") is None

    assert logical_name("dbip-city-lite.mmdb.part003.xz") == "dbip-city-lite.mmdb"
    assert logical_name("dbip-city-lite.mmdb.xz") == "dbip-city-lite.mmdb"
    assert logical_name("dbip-city-lite.mmdb") == "dbip-city-lite.mmdb"


@pytest.mark.parametrize("codec", ["xz", "gz"])
def test_split_parts_merge_back_byte_identical(tmp_path: Path, codec: str) -> None:
    raw = _decode(_source_archive())
    parts = _split(raw, tmp_path, codec)
    assert len(parts) >= 2

    resolved = extract_many([parts[1]], tmp_path / "cache")
    merged = Path(resolved[0])
    assert merged.name == "mydata.mmdb"
    assert hashlib.sha256(merged.read_bytes()).hexdigest() == hashlib.sha256(raw).hexdigest()

    # idempotent second call, and asking with another part gives the same file
    assert Path(extract_many([parts[-1]], tmp_path / "cache")[0]) == merged
    assert not (tmp_path / "cache" / ".staging").exists() or not any(
        (tmp_path / "cache" / ".staging").iterdir()
    )


def test_concat_parts_matches_single_file(tmp_path: Path) -> None:
    source = _source_archive()
    raw = _decode(source)
    parts = _split(raw, tmp_path, "xz", chunks=4)
    target = tmp_path / "merged.mmdb"
    concat_parts(parts, target)
    assert target.read_bytes() == raw


def test_extract_many_returns_input_order(tmp_path: Path) -> None:
    data_dir = package_data_dir()
    archives = []
    for item in sorted(data_dir.iterdir()):
        if part_info(item.name) is not None:
            archives.append(item)
        if len(archives) == 3:
            break
    assert len(archives) >= 2
    resolved = extract_many(archives, tmp_path / "cache")
    assert len(resolved) == len(archives)
    assert all(Path(item).is_file() for item in resolved)
    # same logical database resolves to one file, whatever part you pass in
    again = extract_many(list(reversed(archives)), tmp_path / "cache")
    assert sorted(str(item) for item in again) == sorted(str(item) for item in resolved)


def test_split_dataset_loads_and_answers(tmp_path: Path) -> None:
    """A directory holding only parts is a usable database directory."""
    source = _source_archive()
    raw = _decode(source)
    parts = _split(raw, tmp_path, "xz", chunks=2)
    for index, part in enumerate(parts, 1):
        part.rename(tmp_path / f"dbip-asn-lite.mmdb.part{index:03d}.xz")

    databases = open_databases(db_dir=tmp_path, cache_dir=tmp_path / "cache", strict=True)
    keys = [db.info.key for db in databases]
    assert keys.count("dbip-asn") == 1, f"parts should collapse to one database: {keys}"

    client = Detector(db_dir=tmp_path, cache_dir=tmp_path / "cache", cache_size=0)
    try:
        row = client.lookup("8.8.8.8")
        assert row.found
    finally:
        client.close()


def test_warmup_is_idempotent_and_reports() -> None:
    first = asyncio.run(warmup())
    assert first["files"] >= 8
    assert first["bytes"] > 100_000_000
    assert first["cache_dir"]

    second = asyncio.run(warmup())
    assert second["files"] == first["files"]
    assert second["already_ready"] is True, "a warm cache must not decompress again"
    assert second["seconds"] <= first["seconds"] + 1.0


def test_info_still_works_after_split_data() -> None:
    document = asyncio.run(info("8.8.8.8"))
    assert document["found"] is True
    assert document["country"]["iso_code"] == "US"
