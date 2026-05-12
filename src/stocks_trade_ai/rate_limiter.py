"""Async token bucket with both per-second and per-minute caps."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

log = logging.getLogger(__name__)


class RateLimiter:
    """Combined per-second and per-minute sliding-window limiter.

    Callers `await limiter.acquire()` before issuing a call. If both caps
    have headroom the call returns immediately; otherwise it sleeps until
    capacity frees up.
    """

    def __init__(
        self,
        per_sec: int,
        per_min: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        name: str = "rl",
    ) -> None:
        if per_sec <= 0 or per_min <= 0:
            raise ValueError("limits must be positive")
        self._per_sec = per_sec
        self._per_min = per_min
        self._clock = clock
        self._name = name
        self._stamps_sec: deque[float] = deque()
        self._stamps_min: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        # Single-flight under the lock: callers serialize to compute a fair
        # wait, then sleep released. Bursts are still possible but bounded.
        async with self._lock:
            wait = self._wait_needed()
        if wait > 0:
            log.debug("[%s] rate-limited, sleeping %.3fs", self._name, wait)
            await asyncio.sleep(wait)
        async with self._lock:
            now = self._clock()
            self._evict(now)
            self._stamps_sec.append(now)
            self._stamps_min.append(now)

    def _wait_needed(self) -> float:
        now = self._clock()
        self._evict(now)
        waits = [0.0]
        if len(self._stamps_sec) >= self._per_sec:
            waits.append(self._stamps_sec[0] + 1.0 - now)
        if len(self._stamps_min) >= self._per_min:
            waits.append(self._stamps_min[0] + 60.0 - now)
        return max(waits)

    def _evict(self, now: float) -> None:
        while self._stamps_sec and now - self._stamps_sec[0] >= 1.0:
            self._stamps_sec.popleft()
        while self._stamps_min and now - self._stamps_min[0] >= 60.0:
            self._stamps_min.popleft()
