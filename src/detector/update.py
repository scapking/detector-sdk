"""Dataset updates: fetch the newest databases from the upstream mirrors.

The same registry serves everyone, so ``detector db update`` picks up whatever
the ip-location-db / DB-IP projects published today:

* bundled keys (``dbip-*``, ``iptoasn-*``, ``origin-asn``, ``user-country``,
  ``server-country``) -> refreshed daily or monthly, dropped into a directory
* optional keys (``geolite2-*``) -> downloaded on explicit request only, because
  MaxMind's EULA forbids redistributing their data inside a public package

Sync and async entry points share all the logic; the async one downloads several
datasets concurrently over native asyncio sockets.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import lzma
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from ._version import __version__
from .databases import (
    BUILTIN_DATASETS,
    BUNDLED_KEYS,
    NON_REDISTRIBUTABLE_KEYS,
    DatasetSpec,
    MemberSpec,
    _archive_kind,
    _open_archive,
    _part_siblings,
    concat_parts,
    ensure_extracted,
    part_info,
    sha256_of,
)
from .exceptions import DownloadError
from .net import download, download_sync

__all__ = [
    "MANIFEST_NAME",
    "DEFAULT_UPDATE_KEYS",
    "member_stem",
    "ProgressFn",
    "download_dataset",
    "download_dataset_async",
    "update_datasets",
    "update_datasets_async",
    "write_manifest",
    "read_manifest",
    "known_datasets",
]

MANIFEST_NAME = "MANIFEST.json"
DEFAULT_UPDATE_KEYS: Tuple[str, ...] = BUNDLED_KEYS

ProgressFn = Optional[Callable[[str, int, Optional[int]], None]]


def known_datasets() -> Dict[str, Dict[str, Any]]:
    """Registry view: key -> metadata (licence, kind, bundled, mirrors, ...)."""
    return {
        key: {
            "name": spec.name,
            "kind": spec.kind,
            "license": spec.license,
            "bundled": spec.bundled,
            "redistributable": spec.redistributable,
            "update": spec.update,
            "providers": list(spec.providers),
            "sources": list(spec.available_sources),
            "homepage": spec.homepage,
            "files": list(spec.filenames),
        }
        for key, spec in BUILTIN_DATASETS.items()
    }


def member_stem(member: MemberSpec) -> str:
    """``geolite2-city-ipv4.mmdb.xz`` -> ``geolite2-city-ipv4``."""
    for suffix in (".mmdb.xz", ".mmdb.gz", ".mmdb"):
        if member.filename.endswith(suffix):
            return member.filename[: -len(suffix)]
    return member.filename


def _target_path(member: MemberSpec, target_dir: Path, compressed: bool) -> Path:
    """Local name for a downloaded member.

    Downloads keep their upstream shape (``.mmdb.gz`` from DB-IP, plain
    ``.mmdb`` from GitHub); discovery maps either back onto the dataset by name,
    so nothing needs recompressing on the fly.
    """
    suffix = ".mmdb.gz" if compressed else ".mmdb"
    return target_dir / f"{member_stem(member)}{suffix}"


def _spec(key: str) -> DatasetSpec:
    try:
        return BUILTIN_DATASETS[key]
    except KeyError:
        known = sorted(BUILTIN_DATASETS)
        raise KeyError(f"unknown dataset {key!r}; known: {known}") from None


def _candidate_urls(
    member: MemberSpec, source: Optional[str], period: Optional[str]
) -> List[Tuple[str, str, bool]]:
    """``[(mirror_name, url, compressed)]`` for one member, in priority order."""
    today = datetime.now(timezone.utc)
    period = period or f"{today.year:04d}-{today.month:02d}"
    out: List[Tuple[str, str, bool]] = []
    for mirror in member.sources:
        if source and mirror.name != source:
            continue
        out.append(
            (mirror.name, mirror.url_template.format(key=member.variant or "", period=period),
             mirror.compressed)
        )
    return out


# --------------------------------------------------------------------------- #
# Async (preferred)
# --------------------------------------------------------------------------- #


async def _download_member_async(
    key: str,
    member: MemberSpec,
    target_dir: Path,
    *,
    source: Optional[str],
    period: Optional[str],
    timeout: float,
    progress: ProgressFn,
    retries: int,
) -> Path:
    errors: List[str] = []
    for mirror_name, url, compressed in _candidate_urls(member, source, period):
        target = _target_path(member, target_dir, compressed)
        label = f"{key}[{mirror_name}]" + (f" {member.variant}" if member.variant else "")
        try:
            await download(url, target, timeout=timeout, progress=progress, name=label,
                           retries=retries)
            return target
        except DownloadError as exc:
            errors.append(f"{mirror_name}: {exc.message}")
    raise DownloadError(
        f"dataset {key}{'/' + member.variant if member.variant else ''} "
        f"failed on every mirror: " + "; ".join(errors),
        detail=[mirror.name for mirror in member.sources],
    )


async def download_dataset_async(
    key: str,
    target_dir: "str | os.PathLike",
    *,
    source: Optional[str] = None,
    period: Optional[str] = None,
    timeout: float = 300,
    progress: ProgressFn = None,
    retries: int = 1,
) -> Dict[str, Path]:
    """Download every file of one dataset, trying mirrors in turn.

    Returns ``{uid: local path}`` - a single entry for combined datasets,
    ``{"geolite2-city-ipv4": ..., "geolite2-city-ipv6": ...}`` for split ones.
    """
    spec = _spec(key)
    target_dir = Path(target_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    out: Dict[str, Path] = {}
    for member in spec.members:
        path = await _download_member_async(
            key, member, target_dir, source=source, period=period, timeout=timeout,
            progress=progress, retries=retries,
        )
        uid = f"{key}-{member.variant}" if member.variant else key
        out[uid] = path
    return out


async def update_datasets_async(
    target_dir: "str | os.PathLike",
    *,
    datasets: Iterable[str] = DEFAULT_UPDATE_KEYS,
    source: Optional[str] = None,
    concurrency: int = 4,
    period: Optional[str] = None,
    timeout: float = 300,
    progress: ProgressFn = None,
    extract: bool = True,
    manifest: bool = True,
    retries: int = 1,
) -> Dict[str, Path]:
    """Download several datasets concurrently. Returns ``{key: local path}``."""
    keys = list(datasets)
    target_dir = Path(target_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(max(1, concurrency))
    results: Dict[str, Path] = {}
    failures: List[str] = []

    async def one(key: str) -> None:
        async with semaphore:
            try:
                paths = await download_dataset_async(
                    key, target_dir, source=source, period=period, timeout=timeout,
                    progress=progress, retries=retries,
                )
            except DownloadError as exc:
                failures.append(f"{key}: {exc.message}")
                return
            results.update(paths)
            if progress:
                for uid in paths:
                    progress(uid, 1, 1)

    await asyncio.gather(*(one(key) for key in keys))

    if extract:
        loop = asyncio.get_running_loop()
        for path in results.values():
            if path.name.endswith((".gz", ".xz")):
                await loop.run_in_executor(None, ensure_extracted, path, target_dir)
    if manifest:
        await asyncio.get_running_loop().run_in_executor(
            None, write_manifest, target_dir, list(results)
        )
    if failures and not results:
        raise DownloadError("every dataset failed: " + "; ".join(failures), detail=failures)
    return results


# --------------------------------------------------------------------------- #
# Sync wrappers (blocking, no event loop required)
# --------------------------------------------------------------------------- #


def download_dataset(
    key: str,
    target_dir: "str | os.PathLike",
    *,
    source: Optional[str] = None,
    period: Optional[str] = None,
    timeout: float = 300,
    progress: ProgressFn = None,
) -> Dict[str, Path]:
    """Blocking variant of :func:`download_dataset_async`; returns ``{uid: path}``."""
    spec = _spec(key)
    target_dir = Path(target_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    out: Dict[str, Path] = {}
    for member in spec.members:
        errors: List[str] = []
        label_variant = f" {member.variant}" if member.variant else ""
        for mirror_name, url, compressed in _candidate_urls(member, source, period):
            target = _target_path(member, target_dir, compressed)
            try:
                download_sync(url, target, timeout=timeout, progress=progress,
                              name=f"{key}[{mirror_name}]{label_variant}")
                out[f"{key}-{member.variant}" if member.variant else key] = target
                break
            except DownloadError as exc:
                errors.append(f"{mirror_name}: {exc.message}")
        else:
            raise DownloadError(
                f"dataset {key}{label_variant} failed on every mirror: " + "; ".join(errors),
                detail=errors,
            )
    return out


def update_datasets(
    target_dir: "str | os.PathLike",
    *,
    datasets: Iterable[str] = DEFAULT_UPDATE_KEYS,
    source: Optional[str] = None,
    period: Optional[str] = None,
    timeout: float = 300,
    progress: ProgressFn = None,
    extract: bool = True,
    manifest: bool = True,
) -> Dict[str, Path]:
    """Blocking variant of :func:`update_datasets_async`."""
    results: Dict[str, Path] = {}
    failures: List[str] = []
    for key in datasets:
        try:
            paths = download_dataset(
                key, target_dir, source=source, period=period, timeout=timeout, progress=progress
            )
        except DownloadError as exc:
            failures.append(f"{key}: {exc.message}")
            continue
        for uid, path in paths.items():
            results[uid] = path
            if extract and path.name.endswith((".gz", ".xz")):
                ensure_extracted(path, Path(target_dir).expanduser())
            if progress:
                progress(uid, 1, 1)
    if manifest and results:
        write_manifest(target_dir, list(results))
    if failures and not results:
        raise DownloadError("every dataset failed: " + "; ".join(failures), detail=failures)
    return results


# --------------------------------------------------------------------------- #
# Manifest
# --------------------------------------------------------------------------- #


def _database_build_date(path: Path) -> Optional[str]:
    """Read the real build date from MMDB metadata (gz/xz transparently unpacked)."""
    import tempfile

    import maxminddb

    candidate = path
    temporary: Optional[str] = None
    try:
        if path.name.endswith((".gz", ".xz")):
            handle = tempfile.NamedTemporaryFile(suffix=".mmdb", delete=False)
            temporary = handle.name
            opener = gzip.open if path.name.endswith(".gz") else lzma.open
            with opener(path, "rb") as src:
                shutil.copyfileobj(src, handle, length=1 << 20)
            handle.close()
            candidate = Path(temporary)
        reader = maxminddb.open_database(str(candidate))
        try:
            epoch = getattr(reader.metadata(), "build_epoch", None)
        finally:
            reader.close()
        if epoch:
            return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:  # pragma: no cover - unreadable file: fall back to mtime
        return None
    finally:
        if temporary:
            Path(temporary).unlink(missing_ok=True)
    return None


def _local_candidates(target_dir: Path, member: MemberSpec) -> List[Path]:
    """Every local file that represents this member, whatever its compression.

    Split artefacts count too: ``<stem>.mmdb.part001.xz`` … ``partNNN.xz``.
    """
    stem = member_stem(member)
    names = [
        member.filename,
        f"{stem}.mmdb",
        f"{stem}.mmdb.gz",
        f"{stem}.mmdb.xz",
        f"{stem}.mmdb.zst",
    ]
    candidates = [target_dir / name for name in dict.fromkeys(names)]
    if target_dir.is_dir():
        candidates.extend(sorted(target_dir.glob(f"{stem}.mmdb.part*.??")))
    return [item for item in candidates if item.is_file() and part_info(item.name) is None] or candidates


def write_manifest(target_dir: "str | os.PathLike", datasets: Sequence[str] = ()) -> Path:
    """Write ``MANIFEST.json``: one entry per file, with sha256, licence, build date."""
    target_dir = Path(target_dir).expanduser()
    entries: List[Dict[str, Any]] = []
    wanted = set(datasets) if datasets else set(BUILTIN_DATASETS)

    for key, spec in BUILTIN_DATASETS.items():
        if key not in wanted:
            continue
        for member in spec.members:
            existing = [item for item in _local_candidates(target_dir, member) if item.is_file()]
            parts = _part_siblings(target_dir, member)
            if not existing and not parts:
                continue
            parts.sort(key=lambda item: part_info(item.name)[1])  # type: ignore[index]
            packaged = parts[0] if parts else max(existing, key=lambda item: item.stat().st_mtime)
            part_sizes: List[int] = []
            with tempfile.TemporaryDirectory(prefix="detector-manifest-") as tmp:
                if parts:
                    # Sha256 / size / build date describe the database, not the
                    # packaging, so the split artefact is assembled once here - and
                    # the uncompressed size of every part is recorded so the runtime
                    # can write the parts straight to their offsets.
                    assembled = Path(tmp) / member_stem(member)
                    try:
                        part_sizes = []
                        with open(assembled, "wb") as dst:
                            for part in parts:
                                size = 0
                                with _open_archive(part, _archive_kind(part)) as src:
                                    while True:
                                        block = src.read(1 << 22)
                                        if not block:
                                            break
                                        dst.write(block)
                                        size += len(block)
                                part_sizes.append(size)
                    except Exception:
                        assembled = None  # type: ignore[assignment]
                else:
                    if _archive_kind(packaged) is None:
                        assembled = packaged
                    else:
                        assembled = Path(tmp) / member_stem(member)
                        try:
                            concat_parts([packaged], assembled)
                        except Exception:
                            assembled = None  # type: ignore[assignment]

                if assembled is not None and Path(assembled).is_file():
                    size = Path(assembled).stat().st_size
                    digest = sha256_of(Path(assembled))
                    build_date = _database_build_date(Path(assembled))
                else:  # pragma: no cover - fall back to the packaged bytes
                    size = packaged.stat().st_size
                    digest = sha256_of(packaged)
                    build_date = None
            fallback_time = datetime.fromtimestamp(
                packaged.stat().st_mtime, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            entries.append(
                {
                    "key": key,
                    "variant": member.variant,
                    "name": spec.name,
                    "file": member_stem(member),
                    "parts": [item.name for item in parts],
                    "part_sizes": part_sizes,
                    "codec": (_archive_kind(parts[0]) if parts else None),
                    "kind": spec.kind,
                    "database_type": spec.name,
                    "license": spec.license,
                    "attribution": spec.attribution,
                    "homepage": spec.homepage,
                    "providers": list(spec.providers),
                    "redistributable": spec.redistributable,
                    "size": size,
                    "part_bytes": part_sizes,
                    "packaged_size": sum(
                        item.stat().st_size
                        for item in (parts or [max(existing, key=lambda i: i.stat().st_mtime)])
                    ),
                    "build_date": build_date or fallback_time,
                    "sha256": digest,
                    "packaged_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )

    attributions = sorted({entry["attribution"] for entry in entries if entry["attribution"]})
    payload = {
        "generator": f"detector/{__version__}",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "attribution": " | ".join(attributions),
        "bundled": list(BUNDLED_KEYS),
        "non_redistributable": list(NON_REDISTRIBUTABLE_KEYS),
        "sha256": {entry["file"]: entry["sha256"] for entry in entries},
        "datasets": entries,
    }
    path = target_dir / MANIFEST_NAME
    tmp = target_dir / f".{MANIFEST_NAME}.tmp{os.getpid()}"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return path


def read_manifest(target_dir: "str | os.PathLike") -> Dict[str, Any]:
    """Read ``MANIFEST.json``; empty dict when missing."""
    path = Path(target_dir).expanduser() / MANIFEST_NAME
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))
