from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lakehouse.errors import CapacityError, ConflictError, LeaseLostError
from lakehouse.queue import NewItem, UploadQueue, UploadReceipt
from tests.conftest import FakeClock


def item(n: int, size: int = 100) -> NewItem:
    return NewItem(
        idempotency_key=f"{n:064x}",
        object_key=f"bronze/part-{n}.enc",
        source_path=f"bronze/part-{n}.parquet",
        spool_path=f"aa/{n}.enc",
        plaintext_sha256="p" * 64,
        ciphertext_sha256="c" * 64,
        key_id="k1",
        plaintext_bytes=size - 50,
        encrypted_bytes=size,
    )


def receipt_for(entry: NewItem) -> UploadReceipt:
    return UploadReceipt(
        receipt_id=f"upl-{entry.idempotency_key[-12:]}",
        idempotency_key=entry.idempotency_key,
        object_key=entry.object_key,
        provider="filesystem",
        bucket="lake",
        etag=None,
        ciphertext_sha256=entry.ciphertext_sha256,
        plaintext_sha256=entry.plaintext_sha256,
        key_id=entry.key_id,
        encrypted_bytes=entry.encrypted_bytes,
        created_object=True,
        verified_at="2026-10-01T00:00:00.000000Z",
    )


@pytest.fixture
def queue(tmp_path: Path, clock: FakeClock) -> UploadQueue:
    return UploadQueue(tmp_path / "queue.sqlite", max_pending_bytes=1_000, clock=clock)


def test_enqueue_is_idempotent_and_rejects_conflicts(queue: UploadQueue) -> None:
    first = queue.enqueue(item(1))
    assert queue.enqueue(item(1)) == first
    changed = NewItem(**{**item(1).__dict__, "ciphertext_sha256": "d" * 64})
    with pytest.raises(ConflictError):
        queue.enqueue(changed)


def test_backpressure_when_in_flight_bytes_exceed_the_cap(queue: UploadQueue) -> None:
    queue.enqueue(item(1, 600))
    with pytest.raises(CapacityError):
        queue.enqueue(item(2, 600))


def test_full_state_machine(queue: UploadQueue) -> None:
    queue.enqueue(item(1))
    lease = queue.claim("worker-a")
    assert lease is not None and lease.item.status == "uploading"
    assert queue.claim("worker-b") is None  # leased, not claimable
    queue.record_receipt(lease, receipt_for(item(1)))
    assert queue.get(item(1).idempotency_key).status == "object_durable"  # type: ignore[union-attr]
    queue.complete(lease)
    assert queue.get(item(1).idempotency_key).status == "completed"  # type: ignore[union-attr]
    statuses = [h["status"] for h in queue.history()]
    assert statuses == ["pending", "uploading", "object_durable", "completed"]


def test_complete_requires_a_receipt(queue: UploadQueue) -> None:
    queue.enqueue(item(1))
    lease = queue.claim("w")
    assert lease is not None
    with pytest.raises(ConflictError, match="receipt"):
        queue.complete(lease)


def test_release_backs_off_and_keeps_durable_progress(queue: UploadQueue, clock: FakeClock) -> None:
    queue.enqueue(item(1))
    lease = queue.claim("w")
    assert lease is not None
    queue.release(lease, error="timeout", retry_after=30)
    assert queue.claim("w") is None
    clock.advance(31)
    lease = queue.claim("w")
    assert lease is not None and lease.item.attempts == 2
    queue.record_receipt(lease, receipt_for(item(1)))
    queue.release(lease, error="catalog down", retry_after=0)
    resumed = queue.claim("w")
    assert resumed is not None and resumed.item.status == "object_durable"


def test_expired_lease_is_reclaimed_and_the_stale_worker_is_fenced(
    queue: UploadQueue, clock: FakeClock
) -> None:
    queue.enqueue(item(1))
    stale = queue.claim("slow-worker", lease_seconds=10)
    assert stale is not None
    clock.advance(11)
    fresh = queue.claim("fast-worker", lease_seconds=10)
    assert fresh is not None and fresh.token == stale.token + 1
    with pytest.raises(LeaseLostError):
        queue.record_receipt(stale, receipt_for(item(1)))
    queue.record_receipt(fresh, receipt_for(item(1)))
    queue.complete(fresh)


def test_orphaned_leases_from_a_dead_process_are_recovered(queue: UploadQueue) -> None:
    queue.enqueue(item(1))
    queue.enqueue(item(2))
    dead = queue.claim("pid1-aaaa:upload_0")
    assert dead is not None
    assert queue.recover_orphans("pid2-bbbb") == 1
    lease = queue.claim("pid2-bbbb:upload_0")
    assert lease is not None and lease.item.idempotency_key == item(1).idempotency_key
    with pytest.raises(LeaseLostError):
        queue.release(dead, error="late", retry_after=0)


def test_receipts_and_history_are_append_only(queue: UploadQueue, tmp_path: Path) -> None:
    queue.enqueue(item(1))
    lease = queue.claim("w")
    assert lease is not None
    queue.record_receipt(lease, receipt_for(item(1)))
    raw = sqlite3.connect(tmp_path / "queue.sqlite")
    for statement in ("DELETE FROM upload_receipts", "UPDATE queue_history SET status='x'"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            raw.execute(statement)


def test_claim_can_skip_items_already_tried(queue: UploadQueue) -> None:
    queue.enqueue(item(1))
    queue.enqueue(item(2))
    lease = queue.claim("w", exclude=[item(1).idempotency_key])
    assert lease is not None and lease.item.idempotency_key == item(2).idempotency_key
