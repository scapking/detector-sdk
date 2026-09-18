"""Async API: :class:`AsyncDetector` plus module-level coroutine helpers.

Design:

* CPU-bound MMAP lookups are dispatched to a thread pool (``run_in_executor``),
  never executed on the event loop.
* Network-bound work (dataset downloads) is native asyncio - see :mod:`detector.net`.
* Batching is windowed, so a million-IP input costs a bounded amount of memory
  while still overlapping work across threads.

Two equivalent styles::

    # 1. explicit client
    async with await AsyncDetector.create() as geo:
        info = await geo.lookup("8.8.8.8")
        results = await geo.distance("8.8.8.8", ["1.1.1.1", "223.5.5.5"])
        async for row in geo.stream(huge_iterable):
            ...

    # 2. the two public coroutines (they build and pool a client for you)
    from detector import info, distance
    info = await info("8.8.8.8")
    dist = await distance("8.8.8.8", "1.1.1.1")
"""

from __future__ import annotations

import asyncio
import functools
from pathlib import Path
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Union,
)

from .api import Detector, IPLike
from .distance import Distance
from .envelope import Response, handle
from .exceptions import InvalidIPError
from .models import IPInfo
from .update import update_datasets_async

__all__ = ["AsyncDetector"]

DEFAULT_CONCURRENCY = 32
DEFAULT_WINDOW = 1024


class AsyncDetector:
    """Async facade over :class:`~detector.api.Detector`."""

    def __init__(
        self,
        geo: Optional[Detector] = None,
        *,
        max_concurrency: int = DEFAULT_CONCURRENCY,
        window: int = DEFAULT_WINDOW,
        executor: Optional[Any] = None,
        **geo_kwargs: Any,
    ) -> None:
        # ``geo`` is a blocking open (mmap + manifest reads). For a truly
        # non-blocking startup use ``await AsyncDetector.create(...)``.
        self._geo = geo if geo is not None else Detector(**geo_kwargs)
        self.max_concurrency = max(1, int(max_concurrency))
        self.window = max(1, int(window))
        self._executor = executor
        self._closed = False

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #

    @classmethod
    async def create(
        cls,
        *,
        max_concurrency: int = DEFAULT_CONCURRENCY,
        window: int = DEFAULT_WINDOW,
        executor: Optional[Any] = None,
        **geo_kwargs: Any,
    ) -> "AsyncDetector":
        """Open the databases in a worker thread so the event loop stays responsive."""
        loop = asyncio.get_running_loop()
        geo = await loop.run_in_executor(executor, functools.partial(Detector, **geo_kwargs))
        return cls(geo, max_concurrency=max_concurrency, window=window, executor=executor)

    @property
    def sync(self) -> Detector:
        """Escape hatch to the underlying synchronous client."""
        return self._geo

    @property
    def databases(self) -> List[Any]:
        return self._geo.databases

    async def ready(self, timeout: Optional[float] = None, *, wait: str = "all") -> bool:
        """Is the database set ready? Never raises: use :meth:`progress` for detail.

        ``wait="all"`` (default) waits for every dataset; ``wait="any"`` returns
        as soon as the first one is unpacked, which is what a latency-sensitive
        health check wants.
        """
        prep = self._geo.preparation
        if prep is None:
            return True
        loop = asyncio.get_running_loop()
        call = prep.wait_for_any if wait == "any" else prep.wait
        return bool(await loop.run_in_executor(self._executor, functools.partial(call, timeout)))

    async def progress(self) -> Dict[str, Any]:
        """Preparation snapshot: ready/total, loading flag, failures, timings."""
        prep = self._geo.preparation
        if prep is None:
            return {"ready": len(self._geo.databases), "total": len(self._geo.databases),
                    "loading": False, "complete": True, "failed": {}, "pending": [],
                    "timings": {}, "seconds": 0.0, "cache_dir": str(self._geo.cache_dir)}
        return prep.progress()

    @property
    def dataset_keys(self) -> List[str]:
        """Loaded dataset keys, de-duplicated."""
        return self._geo.dataset_keys

    @property
    def database_uids(self) -> List[str]:
        """One uid per loaded file (split datasets contribute several)."""
        return self._geo.database_uids

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    async def _run(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("AsyncDetector is closed")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, functools.partial(func, *args, **kwargs))

    # ------------------------------------------------------------------ #
    # Lookup
    # ------------------------------------------------------------------ #

    async def lookup(self, ip: IPLike, **kwargs: Any) -> IPInfo:
        return await self._run(self._geo.lookup, ip, **kwargs)

    async def lookup_many(
        self,
        ips: Iterable[IPLike],
        *,
        concurrency: Optional[int] = None,
        window: Optional[int] = None,
        locales: Optional[Sequence[str]] = None,
        ignore_errors: bool = True,
    ) -> List[IPInfo]:
        """Batch lookup with bounded memory; order is preserved.

        Each window is handed to the synchronous client in one worker call, so
        the event loop stays responsive without paying per-IP thread handoffs
        (the merge step is CPU-bound under the GIL).
        """
        items = list(ips)
        if not items:
            return []
        size = window or self.window
        limit = asyncio.Semaphore(concurrency or self.max_concurrency)
        results: List[IPInfo] = []

        async def one_chunk(chunk: List[Any]) -> List[IPInfo]:
            async with limit:
                return await self._run(
                    self._geo.lookup_many, chunk, locales=locales, ignore_errors=ignore_errors
                )

        for start in range(0, len(items), size):
            chunk = items[start : start + size]
            results.extend(await one_chunk(chunk))
        return results

    async def stream(
        self,
        ips: Union[Iterable[IPLike], AsyncIterable[IPLike]],
        *,
        concurrency: Optional[int] = None,
        window: Optional[int] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> AsyncIterator[IPInfo]:
        """Async generator over unbounded input, yielding in input order.

        Accepts both synchronous iterables (lists, files, generators) and async
        iterables (database cursors, other coroutines' output).
        """
        limit = asyncio.Semaphore(concurrency or self.max_concurrency)
        size = window or self.window
        pending: List[Any] = []

        async def one(value: IPLike) -> IPInfo:
            async with limit:
                try:
                    return await self._run(self._geo.lookup, value, locales=locales)
                except InvalidIPError as exc:
                    return IPInfo(ip=str(value), found=False, meta={"error": exc.to_dict()})

        if hasattr(ips, "__aiter__"):
            async for value in ips:  # type: ignore[union-attr]
                pending.append(asyncio.ensure_future(one(value)))
                if len(pending) >= size:
                    for info in await asyncio.gather(*pending):
                        yield info
                    pending = []
        else:
            for value in ips:  # type: ignore[union-attr]
                pending.append(asyncio.ensure_future(one(value)))
                if len(pending) >= size:
                    for info in await asyncio.gather(*pending):
                        yield info
                    pending = []
        if pending:
            for info in await asyncio.gather(*pending):
                yield info

    # ------------------------------------------------------------------ #
    # Distance
    # ------------------------------------------------------------------ #

    async def distance(
        self,
        source: IPLike,
        targets: Union[IPLike, Iterable[IPLike]],
        *,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> Union[Distance, List[Distance]]:
        return await self._run(self._geo.distance, source, targets, method=method, locales=locales)

    distance_1toN = distance

    async def distance_many(
        self,
        sources: Iterable[IPLike],
        targets: Iterable[IPLike],
        *,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> List[Distance]:
        return await self._run(
            self._geo.distance_many, sources, targets, method=method, locales=locales
        )

    async def nearest(
        self,
        source: IPLike,
        targets: Iterable[IPLike],
        *,
        limit: int = 5,
        max_km: Optional[float] = None,
        method: Optional[str] = None,
        locales: Optional[Sequence[str]] = None,
    ) -> List[Distance]:
        return await self._run(
            self._geo.nearest, source, targets, limit=limit, max_km=max_km, method=method,
            locales=locales,
        )

    # ------------------------------------------------------------------ #
    # Protocol and metadata
    # ------------------------------------------------------------------ #

    async def request(self, payload: Any) -> Response:
        return await self._run(handle, self._geo, payload)

    async def request_json(self, payload: Any) -> str:
        response = await self.request(payload)
        return response.to_json()

    async def describe(self) -> Dict[str, Any]:
        return await self._run(self._geo.describe)

    async def stats(self) -> Dict[str, Any]:
        return await self._run(self._geo.stats)

    # ------------------------------------------------------------------ #
    # Datasets
    # ------------------------------------------------------------------ #

    async def update_datasets(
        self,
        target_dir: "Optional[str | Path]" = None,
        *,
        datasets: Iterable[str] = ("dbip-city", "dbip-asn", "dbip-country"),
        source: Optional[str] = None,
        concurrency: int = 4,
        period: Optional[str] = None,
        progress: Optional[Any] = None,
        extract: bool = True,
        manifest: bool = True,
    ) -> Dict[str, Path]:
        """Download datasets concurrently into ``target_dir`` (default: cache dir)."""
        return await update_datasets_async(
            target_dir or self._geo.cache_dir,
            datasets=datasets,
            source=source,
            concurrency=concurrency,
            period=period,
            progress=progress,
            extract=extract,
            manifest=manifest,
        )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def aclose(self) -> None:
        if not self._closed:
            await self._run(self._geo.close)
            self._closed = True

    def close(self) -> None:
        """Blocking close for callers that are not in an event loop."""
        self._geo.close()
        self._closed = True

    async def __aenter__(self) -> "AsyncDetector":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def __repr__(self) -> str:  # pragma: no cover
        keys = ",".join(self._geo.dataset_keys)
        return f"<AsyncDetector datasets={keys} concurrency={self.max_concurrency}>"
