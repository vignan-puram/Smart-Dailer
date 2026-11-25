"""Virtual clock and event scheduler.

Every service reads time from an injected ``Clock`` instead of calling
``time.time()``. The same production code therefore runs:

* inside the discrete-event simulator (a 2-hour scenario runs in seconds and
  is fully reproducible for a given seed), and
* behind the API, where an engineer advances time explicitly.

Mock telecom providers deliver their webhook-style events through the
``Scheduler``. That is how latency, delays and out-of-order delivery are modelled.
"""
from __future__ import annotations

import heapq
import itertools
import threading
from dataclasses import dataclass, field
from typing import Callable, Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SimClock:
    def __init__(self, start: float = 0.0) -> None:
        self._t = float(start)
        self._lock = threading.Lock()

    def now(self) -> float:
        return self._t

    def advance_to(self, t: float) -> None:
        with self._lock:
            if t > self._t:
                self._t = float(t)


@dataclass(order=True)
class _Scheduled:
    at: float
    seq: int
    callback: Callable[[], None] = field(compare=False)


class Scheduler:
    """Min-heap of callbacks ordered by virtual time (FIFO for equal times)."""

    def __init__(self, clock: SimClock) -> None:
        self.clock = clock
        self._heap: list[_Scheduled] = []
        self._seq = itertools.count()
        self._lock = threading.Lock()

    def schedule_at(self, at: float, callback: Callable[[], None]) -> None:
        with self._lock:
            heapq.heappush(self._heap, _Scheduled(max(at, self.clock.now()), next(self._seq), callback))

    def schedule_in(self, delay: float, callback: Callable[[], None]) -> None:
        self.schedule_at(self.clock.now() + max(0.0, delay), callback)

    def run_until(self, t: float) -> int:
        """Run every callback due at or before ``t``; returns how many ran."""
        ran = 0
        while True:
            with self._lock:
                if not self._heap or self._heap[0].at > t:
                    break
                item = heapq.heappop(self._heap)
            self.clock.advance_to(item.at)
            item.callback()
            ran += 1
        self.clock.advance_to(t)
        return ran

    def pending(self) -> int:
        with self._lock:
            return len(self._heap)
