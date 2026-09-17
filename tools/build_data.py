"""Maintainer script: re-vendor the bundled datasets and refresh MANIFEST.json.

    python tools/build_data.py                 # all bundled datasets
    python tools/build_data.py dbip-city       # one of them
    python tools/build_data.py --source sapics # use the GitHub mirror only
    python tools/build_data.py --extract       # also leave plain .mmdb files behind
    python tools/build_data.py --keep-raw      # do not recompress downloads to .xz

The script is async: datasets download concurrently over asyncio sockets, then
recompresses each artefact to ``.mmdb.xz`` (35% smaller than gzip, unpacked by
the standard-library ``lzma`` module at runtime).
"""

from __future__ import annotations

import asyncio
import lzma
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from detector.databases import (  # noqa: E402
    BUILTIN_DATASETS,
    BUNDLED_KEYS,
    MemberSpec,
    ensure_extracted,
)
from detector.update import member_stem, update_datasets_async, write_manifest  # noqa: E402

DATA_DIR = ROOT / "src" / "detector" / "data"


def parse_args(argv: list) -> tuple:
    datasets = []
    source = None
    period = None
    concurrency = 4
    extract = False
    keep_raw = False
    index = 0
    while index < len(argv):
        item = argv[index]
        if item == "--source":
            index += 1
            source = argv[index]
        elif item == "--period":
            index += 1
            period = argv[index]
        elif item == "--concurrency":
            index += 1
            concurrency = int(argv[index])
        elif item == "--extract":
            extract = True
        elif item == "--keep-raw":
            keep_raw = True
        elif item.startswith("-"):
            raise SystemExit(f"unknown option: {item}")
        else:
            datasets.append(item)
        index += 1
    return (datasets or list(BUNDLED_KEYS), source, period, concurrency, extract, keep_raw)


def report(name: str, done: int, total: int | None) -> None:
    if total:
        percent = done * 100 // total
        sys.stderr.write(f"\r  {name:<28} {percent:3d}%  {done / 1e6:7.1f}/{total / 1e6:.1f} MB")
    else:
        sys.stderr.write(f"\r  {name:<28} {done / 1e6:7.1f} MB")
    if total and done >= total:
        sys.stderr.write("\n")
    sys.stderr.flush()


def recompress(path: Path) -> Path:
    """Store a downloaded database as .mmdb.xz, dropping the original artefact."""
    if path.name.endswith(".mmdb.xz"):
        return path
    target = path.with_name(f"{member_stem(MemberSpec(path.name))}.mmdb.xz")
    if path.name.endswith(".gz"):
        import gzip

        data = gzip.decompress(path.read_bytes())
    else:
        data = path.read_bytes()
    target.write_bytes(lzma.compress(data, preset=9 | lzma.PRESET_EXTREME))
    if target != path:
        path.unlink()
    return target


async def main(argv: list) -> int:
    datasets, source, period, concurrency, extract, keep_raw = parse_args(argv)
    unknown = [key for key in datasets if key not in BUILTIN_DATASETS]
    if unknown:
        return print(f"unknown dataset(s): {unknown}; known: {sorted(BUILTIN_DATASETS)}") or 2

    print(f"vendoring {len(datasets)} dataset(s) into {DATA_DIR}")
    if source:
        print(f"mirror: {source}")
    result = await update_datasets_async(
        DATA_DIR,
        datasets=datasets,
        source=source,
        period=period,
        concurrency=concurrency,
        progress=report,
        extract=False,     # keep the shipped artefacts compressed only
        manifest=False,
    )
    stored: dict = {}
    for uid, path in sorted(result.items()):
        final = path if keep_raw else recompress(path)
        stored[uid] = final
        print(f"  {uid:<26} -> {final.name}  {final.stat().st_size / 1e6:.2f} MB")
        if extract:
            print(f"    extracted: {ensure_extracted(final, DATA_DIR)}")

    manifest = write_manifest(DATA_DIR)
    print(f"manifest -> {manifest}")

    from detector.databases import NON_REDISTRIBUTABLE_KEYS

    if NON_REDISTRIBUTABLE_KEYS:
        print(
            "\nWARNING: bundled data includes licence-restricted datasets "
            f"({', '.join(NON_REDISTRIBUTABLE_KEYS)}). MaxMind's EULA does not permit "
            "redistributing GeoLite2 data in a public package - see NOTICE before "
            "publishing a wheel or image that contains it."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
