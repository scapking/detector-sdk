"""Thread-safe LRU cache used to short-circuit repeated lookups."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any, Optional, Tuple

__all__ = ["LRUCache"]


class LRUCache:
    """Minimal implementation: capacity bound, hit counters, thread safe."""

    __slots__ = ("_data", "_hits", "_lock", "_maxsize", "_misses")

    def __init__(self, maxsize: int = 0) -> None:
        self._data: "OrderedDict[str, Any]" = OrderedDict()
        self._maxsize = max(0, int(maxsize))
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def enabled(self) -> bool:
        return self._maxsize > 0

    def get(self, key: str) -> Tuple[bool, Optional[Any]]:
        if not self._maxsize:
            return False, None
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                self._hits += 1
                return True, self._data[key]
            self._misses += 1
            return False, None

    def set(self, key: str, value: Any) -> None:
        if not self._maxsize:
            return
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._hits = 0
            self._misses = 0

    def stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._data),
                "maxsize": self._maxsize,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(self._hits / total, 4) if total else None,
            }

    def __len__(self) -> int:
        return len(self._data)
