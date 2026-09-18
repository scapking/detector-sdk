"""Progressive, priority-ordered preparation of the bundled databases.

First use has to turn ~90 MB of compressed parts into 251 MB of memory-mappable
MMDB files. That work is unavoidable *once*, but it does not have to happen all
at once, nor on the critical path:

* units are unpacked **cheapest first**, so a small country/ASN database is
  queryable within a fraction of a second while the big city database is still
  being written;
* the unpacking runs in a **background thread**, so ``import detector`` - or an
  application's own start-up work - overlaps with it;
* readiness, per-unit failures and elapsed time are observable
  (:meth:`Preparation.progress`), and waiting is bounded
  (:meth:`Preparation.wait`), so a caller can decide between "answer with
  what is ready" and "raise a timeout".

Everything here is transport-agnostic: :class:`~detector.api.Detector` consumes
it, the public ``warmup()``/``ready()`` coroutines expose it, and a refresh
(``update_datasets()``) re-uses the same machinery.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .databases import (
    DATABASE_PRIORITY,
    DatasetSpec,
    _archive_kind,
    discover_database_files,
    extract_many,
    logical_name,
    member_for_filename,
    part_info,
    resolve_data_dir,
    user_cache_dir,
)
from .exceptions import DatabaseError, LoadingTimeoutError

__all__ = ["Preparation", "Unit", "shared_preparation"]


@dataclass
class Unit:
    """One database file (or one split database) to unpack."""

    key: str
    variant: str
    logical: str
    paths: List[Path]
    packaged_bytes: int
    kind: str = "unknown"
    priority: int = 90
    resolved: Optional[Path] = None
    seconds: float = 0.0
    error: Optional[str] = None

    @property
    def label(self) -> str:
        return f"{self.key}:{self.variant}" if self.variant else self.key


class Preparation:
    """Unpack the bundled databases in the cheapest-first order, in background.

    ::

        prep = Preparation(cache_dir="/tmp/x")
        prep.start()                       # returns immediately
        prep.wait(timeout=1.0)             # -> False: not everything is ready yet
        prep.ready_paths()                 # -> {logical name: unpacked path}
        prep.progress()                    # -> counters, failures, elapsed

    Calling :meth:`start` twice is a no-op, and a second process that finds the
    cache populated finishes in milliseconds because extraction is cached.
    """

    def __init__(
        self,
        *,
        db_dir: Optional[os.PathLike] = None,
        cache_dir: Optional[os.PathLike] = None,
        datasets: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        extra_dirs: Optional[Sequence[os.PathLike]] = None,
        extract: bool = True,
        executor: Optional[Callable[[Callable[..., Any], Tuple[Any, ...]], Any]] = None,
    ) -> None:
        self.db_dir = resolve_data_dir(db_dir)
        self.cache_dir = Path(cache_dir).expanduser() if cache_dir else user_cache_dir()
        self.datasets = tuple(datasets) if datasets else None
        self.exclude = tuple(exclude or ())
        self.extra_dirs = [Path(item).expanduser() for item in (extra_dirs or ())]
        self.extract = extract
        self._units = self._scan()
        self._condition = threading.Condition()
        self._thread: Optional[threading.Thread] = None
        self._started = False
        self._finished = False
        self._started_at: Optional[float] = None
        self._finished_at: Optional[float] = None
        self._failed_all = False

    # ------------------------------------------------------------------ #
    # Discovery
    # ------------------------------------------------------------------ #

    def _roots(self) -> List[Path]:
        roots = [self.db_dir] if self.db_dir.is_dir() else []
        for extra in self.extra_dirs:
            if extra.is_dir() and extra not in roots:
                roots.append(extra)
        return roots

    def _scan(self) -> List[Unit]:
        """Every archive to unpack, cheapest (smallest packaged) first.

        Later directories override earlier ones for the same dataset, exactly
        like :func:`~detector.databases.open_databases`.
        """
        chosen: Dict[Tuple[str, str], Unit] = {}
        include = set(self.datasets) if self.datasets else None
        for root in self._roots():
            for path in discover_database_files(root):
                if _archive_kind(path) is None:
                    continue
                spec, member = member_for_filename(path.name)
                key = spec.key if spec else logical_name(path.name).rsplit(".", 1)[0]
                variant = member.variant if member else ""
                if include is not None and key not in include:
                    continue
                if key in self.exclude:
                    continue
                current = chosen.get((key, variant))
                if current is not None and path.name in {item.name for item in current.paths}:
                    continue
                logical = logical_name(path.name)
                info = part_info(path.name)
                if info is not None:
                    siblings = sorted(
                        [
                            item for item in root.iterdir()
                            if item.is_file() and part_info(item.name) is not None
                            and part_info(item.name)[0] == logical
                            and part_info(item.name)[2] == info[2]
                        ],
                        key=lambda item: part_info(item.name)[1],  # type: ignore[index]
                    )
                else:
                    siblings = [path]
                previous = chosen.get((key, variant))
                if previous is not None and info is not None:
                    if any(part_info(item.name) for item in previous.paths):
                        continue
                chosen[(key, variant)] = Unit(
                    key=key,
                    variant=variant,
                    logical=logical,
                    paths=siblings,
                    packaged_bytes=sum(item.stat().st_size for item in siblings),
                    kind=(spec.kind if isinstance(spec, DatasetSpec) else "unknown"),
                    priority=_priority(spec),
                )
        return sorted(chosen.values(), key=lambda unit: (unit.packaged_bytes, unit.label))

    # ------------------------------------------------------------------ #
    # Running
    # ------------------------------------------------------------------ #

    @property
    def total(self) -> int:
        return len(self._units)

    @property
    def started(self) -> bool:
        return self._started

    @property
    def finished(self) -> bool:
        return self._finished

    def start(self) -> "Preparation":
        """Begin unpacking in a daemon thread (idempotent, non-blocking)."""
        with self._condition:
            if self._started:
                return self
            self._started = True
            self._started_at = time.perf_counter()
            self._thread = threading.Thread(
                target=self._run, name="detector-prepare", daemon=True
            )
            self._thread.start()
        return self

    def _run(self) -> None:
        for unit in self._units:
            if unit.resolved is not None or unit.error is not None:
                continue
            started = time.perf_counter()
            try:
                if self.extract:
                    resolved = extract_many(unit.paths, self.cache_dir)[0]
                else:
                    resolved = unit.paths[0]
                unit.resolved = Path(resolved)
            except DatabaseError as exc:
                unit.error = str(exc)
            except Exception as exc:
                unit.error = f"{type(exc).__name__}: {exc}"
            unit.seconds = time.perf_counter() - started
            with self._condition:
                self._condition.notify_all()
        with self._condition:
            self._finished = True
            self._finished_at = time.perf_counter()
            self._failed_all = all(unit.resolved is None for unit in self._units)
            self._condition.notify_all()

    def join(self, timeout: Optional[float] = None) -> None:
        """Wait for the background thread to end (does not raise)."""
        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    # ------------------------------------------------------------------ #
    # Readiness
    # ------------------------------------------------------------------ #

    @property
    def ready_count(self) -> int:
        return sum(1 for unit in self._units if unit.resolved is not None)

    @property
    def failed_count(self) -> int:
        return sum(1 for unit in self._units if unit.error is not None)

    @property
    def failed(self) -> Dict[str, str]:
        """``{dataset label: message}`` for every unit that could not be unpacked."""
        return {unit.label: unit.error for unit in self._units if unit.error}

    def is_ready(self) -> bool:
        """True when every unit has been processed (successfully or not)."""
        return self._finished

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until every unit is processed. ``False`` when the timeout hit."""
        self.start()
        if not self._units:
            return True
        with self._condition:
            if self._finished:
                return True
            if timeout is None:
                self._condition.wait_for(lambda: self._finished)
                return True
            return self._condition.wait_for(lambda: self._finished, timeout=timeout)

    def wait_for_any(self, timeout: Optional[float] = None) -> bool:
        """Block until at least one unit is unpacked (successfully)."""
        self.start()
        with self._condition:
            if self.ready_count:
                return True
            if timeout is None:
                self._condition.wait_for(lambda: self.ready_count or self._finished)
                return self.ready_count > 0
            self._condition.wait_for(
                lambda: self.ready_count or self._finished, timeout=timeout
            )
            return self.ready_count > 0

    def ready_paths(self) -> Dict[str, Path]:
        """``{logical name: unpacked path}`` for everything unpacked so far."""
        return {
            unit.logical: unit.resolved
            for unit in self._units
            if unit.resolved is not None
        }

    def readiness(self) -> Dict[str, Any]:
        """Readiness snapshot, cheap enough to put in every result's ``meta``."""
        ready = self.ready_count
        total = self.total
        return {
            "ready": ready,
            "total": total,
            "loading": (not self._finished) if self._started else False,
            "complete": self._finished and ready == total,
            "seconds": round(self.elapsed, 3),
        }

    @property
    def elapsed(self) -> float:
        if self._started_at is None:
            return 0.0
        end = self._finished_at if self._finished_at is not None else time.perf_counter()
        return end - self._started_at

    def progress(self) -> Dict[str, Any]:
        """Full status: counters, per-unit timings, failures and elapsed time."""
        ready = [unit for unit in self._units if unit.resolved is not None]
        failed = [unit for unit in self._units if unit.error is not None]
        return {
            "ready": len(ready),
            "total": self.total,
            "loading": (not self._finished) if self._started else False,
            "complete": self._finished and len(ready) == self.total,
            "failed": {unit.label: unit.error for unit in failed},
            "pending": [unit.label for unit in self._units if unit.resolved is None and unit.error is None],
            "timings": {unit.label: round(unit.seconds, 3) for unit in ready},
            "seconds": round(self.elapsed, 3),
            "cache_dir": str(self.cache_dir) if self.cache_dir else None,
        }

    # ------------------------------------------------------------------ #
    # Consumers
    # ------------------------------------------------------------------ #

    def fail_if_empty(self) -> None:
        """Raise the recorded failure when nothing could be unpacked at all."""
        if self._units and self._failed_all:
            first = next((unit for unit in self._units if unit.error), None)
            raise DatabaseError(
                "no bundled database could be unpacked"
                + (f": {first.error}" if first else ""),
                detail=str(self.cache_dir or ""),
            )

    def timeout_error(self, timeout: Optional[float]) -> LoadingTimeoutError:
        """Build the error raised when waiting timed out."""
        pending = [unit.label for unit in self._units if unit.resolved is None and unit.error is None]
        return LoadingTimeoutError(
            f"databases are still being prepared ({self.ready_count}/{self.total} ready "
            f"after {self.elapsed:.2f}s)",
            detail=", ".join(pending[:8]),
            ready=self.ready_count,
            total=self.total,
            seconds=round(self.elapsed, 3),
            missing=pending,
            retry_after=1.0,
        )


def _priority(spec: Optional[Any]) -> int:
    if spec is None:
        return DATABASE_PRIORITY.get("unknown", 90)
    return DATABASE_PRIORITY.get(getattr(spec, "kind", "unknown"), 90)


_SHARED: Dict[str, "Preparation"] = {}
_SHARED_LOCK = threading.Lock()


def shared_preparation(
    *,
    db_dir: Optional[os.PathLike] = None,
    cache_dir: Optional[os.PathLike] = None,
    datasets: Optional[Sequence[str]] = None,
    exclude: Optional[Sequence[str]] = None,
    extra_dirs: Optional[Sequence[os.PathLike]] = None,
    extract: bool = True,
) -> "Preparation":
    """One :class:`Preparation` per (cache directory, dataset filter).

    ``Detector``, ``warmup()``, ``ready()`` and ``progress()`` all go through
    this, so a client and the module-level helpers observe the *same* background
    unpacking - calling ``warmup()`` before the first query really does make the
    first query instant, and ``progress()`` reports the client's own state.
    """
    resolved_cache = Path(cache_dir).expanduser() if cache_dir else user_cache_dir()
    key = "|".join(
        [
            str(resolve_data_dir(db_dir)),
            str(resolved_cache),
            ",".join(sorted(datasets)) if datasets else "",
            ",".join(sorted(exclude or ())),
            ",".join(sorted(str(Path(item).expanduser()) for item in (extra_dirs or ()))),
            "1" if extract else "0",
        ]
    )
    with _SHARED_LOCK:
        prep = _SHARED.get(key)
        if prep is None:
            prep = Preparation(
                db_dir=db_dir,
                cache_dir=cache_dir,
                datasets=datasets,
                exclude=exclude,
                extra_dirs=extra_dirs,
                extract=extract,
            )
            _SHARED[key] = prep
        return prep
