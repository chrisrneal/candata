"""Thread-safe in-memory TTL cache."""

from __future__ import annotations

import threading
import time
from typing import Any


class TTLCache:
    """Simple dict-based cache with per-key TTL expiry."""

    def __init__(self, default_ttl: float = 300.0) -> None:
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        with self._lock:
            expires_at = time.monotonic() + (ttl if ttl is not None else self._default_ttl)
            self._store[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def evict_expired(self) -> int:
        """Remove all expired entries. Returns count of evicted keys."""
        now = time.monotonic()
        with self._lock:
            expired = [k for k, (_, exp) in self._store.items() if now > exp]
            for k in expired:
                del self._store[k]
            return len(expired)


# Domain-specific caches
indicator_cache = TTLCache(default_ttl=300)      # 5 min
geography_cache = TTLCache(default_ttl=86400)     # 24 hr
procurement_cache = TTLCache(default_ttl=900)     # 15 min
search_cache = TTLCache(default_ttl=60)           # 1 min
housing_cache = TTLCache(default_ttl=300)         # 5 min
trade_cache = TTLCache(default_ttl=300)           # 5 min
