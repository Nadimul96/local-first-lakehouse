"""Many workers, one queue: every item is leased by exactly one worker at a time."""

from __future__ import annotations

import multiprocessing
import threading
import time
from collections import Counter
from pathlib import Path

from lakehouse.queue import UploadQueue
from tests.test_queue import item, receipt_for

ITEMS = 120


def _fill(path: Path) -> UploadQueue:
    queue = UploadQueue(path, max_pending_bytes=10**9)
    for n in range(ITEMS):
        queue.enqueue(item(n))
    return queue


def test_threads_never_share_a_lease(tmp_path: Path) -> None:
    queue = _fill(tmp_path / "queue.sqlite")
    active: set[str] = set()
    claims: Counter[str] = Counter()
    guard = threading.Lock()
    overlaps: list[str] = []

    def worker(name: str) -> None:
        while (lease := queue.claim(name)) is not None:
            key = lease.item.idempotency_key
            with guard:
                if key in active:
                    overlaps.append(key)
                active.add(key)
                claims[key] += 1
            time.sleep(0.001)
            queue.record_receipt(lease, receipt_for(item(int(key, 16))))
            queue.complete(lease)
            with guard:
                active.discard(key)

    threads = [threading.Thread(target=worker, args=(f"t{n}",)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert overlaps == []
    assert len(claims) == ITEMS and set(claims.values()) == {1}
    assert queue.stats()["by_status"]["completed"] == ITEMS


def _process_worker(path: str, name: str, results: multiprocessing.Queue) -> None:  # type: ignore[type-arg]
    queue = UploadQueue(Path(path), max_pending_bytes=10**9)
    claimed = []
    while (lease := queue.claim(name)) is not None:
        claimed.append(lease.item.idempotency_key)
        queue.record_receipt(lease, receipt_for(item(int(lease.item.idempotency_key, 16))))
        queue.complete(lease)
    results.put(claimed)


def test_processes_never_double_claim(tmp_path: Path) -> None:
    path = tmp_path / "queue.sqlite"
    _fill(path)
    context = multiprocessing.get_context("spawn")
    results: multiprocessing.Queue = context.Queue()  # type: ignore[type-arg]
    workers = [
        context.Process(target=_process_worker, args=(str(path), f"p{n}", results))
        for n in range(4)
    ]
    for process in workers:
        process.start()
    claimed = [key for _ in workers for key in results.get(timeout=60)]
    for process in workers:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert len(claimed) == ITEMS
    assert len(set(claimed)) == ITEMS
