"""Bounded reorder buffer: orders events from many producers by event time.

Events wait until the newest event time seen has moved `window_ms` past them, then are
released in (event time, arrival) order. Ties break by arrival, so the order is fully
deterministic. When more than `max_pending` events are waiting, the oldest are released
early instead of dropped: memory stays bounded and the forced count is reported.

The buffer never owns durability. A buffered event is answered as `pending` and is not
acknowledged; the producer keeps it and re-sends until it gets a durable receipt.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field


@dataclass(order=True)
class _Entry[T]:
    ts_ms: int
    arrival: int
    item: T = field(compare=False)


class Sequencer[T]:
    def __init__(self, *, window_ms: int, max_pending: int = 10_000) -> None:
        if window_ms < 0 or max_pending < 1:
            raise ValueError("window_ms must be >= 0 and max_pending >= 1")
        self.window_ms = window_ms
        self.max_pending = max_pending
        self.forced_releases = 0
        self._heap: list[_Entry[T]] = []
        self._arrival = 0
        self._max_ts: int | None = None

    @property
    def pending(self) -> int:
        return len(self._heap)

    def add(self, ts_ms: int, item: T) -> list[T]:
        """Buffer `item` and return everything that is now safe to release, in order."""
        self._arrival += 1
        heapq.heappush(self._heap, _Entry(ts_ms, self._arrival, item))
        if self._max_ts is None or ts_ms > self._max_ts:
            self._max_ts = ts_ms
        released = self._release_through(self._max_ts - self.window_ms)
        while len(self._heap) > self.max_pending:
            released.append(heapq.heappop(self._heap).item)
            self.forced_releases += 1
        return released

    def flush(self) -> list[T]:
        return self._release_through(None)

    def _release_through(self, watermark: int | None) -> list[T]:
        released: list[T] = []
        while self._heap and (watermark is None or self._heap[0].ts_ms <= watermark):
            released.append(heapq.heappop(self._heap).item)
        return released
