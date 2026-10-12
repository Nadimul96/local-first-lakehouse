"""Upload, read-back verification, duplicate uploads, and tampering."""

from __future__ import annotations

from dataclasses import replace

import pytest

from lakehouse.pipeline import Pipeline
from lakehouse.stores.filesystem import FilesystemObjectStore
from lakehouse.sync import SyncSummary, verify_object
from tests.conftest import SMALL


def staged(pipeline: Pipeline) -> Pipeline:
    pipeline.ingest(SMALL)
    pipeline.export()
    return pipeline


def flip_byte(pipeline: Pipeline, object_key: str) -> None:
    assert isinstance(pipeline.store, FilesystemObjectStore)
    path = pipeline.store.object_path(object_key)
    blob = bytearray(path.read_bytes())
    blob[len(blob) // 2] ^= 0x01
    path.write_bytes(bytes(blob))


def test_sync_uploads_verifies_and_catalogs_every_object(pipeline: Pipeline) -> None:
    staged(pipeline)
    result = pipeline.sync()["result"]
    items = pipeline.queue.items()
    assert result["uploaded"] == result["verified"] == len(items) > 0
    assert {i.status for i in items} == {"completed"}
    assert pipeline.catalog.count() == len(items)
    assert all(v.ok for v in pipeline.verify())


def test_an_object_already_in_the_store_is_verified_not_overwritten(pipeline: Pipeline) -> None:
    staged(pipeline)
    first = pipeline.queue.items()[0]
    pipeline.store.put_if_absent(first.object_key, pipeline.spool.read(first), metadata={})
    result = pipeline.sync()["result"]
    assert result["already_present"] == 1
    receipt = pipeline.queue.receipt(first.idempotency_key)
    assert receipt is not None and receipt.created_object is False


def test_flipped_ciphertext_byte_fails_verification(pipeline: Pipeline) -> None:
    staged(pipeline)
    pipeline.sync()
    target = pipeline.queue.items()[0]
    flip_byte(pipeline, target.object_key)
    result = verify_object(pipeline.store, pipeline.keyring, target)
    assert not result.ok
    assert result.checks["bytes_match"] is False
    assert result.checks["gcm_tag"] is False
    assert result.checks["plaintext_match"] is None  # cannot be evaluated without decryption


def test_a_corrupt_remote_copy_is_never_marked_durable(pipeline: Pipeline) -> None:
    staged(pipeline)
    target = pipeline.queue.items()[0]
    # Something else already wrote a wrong object at this key (a corrupted or foreign copy).
    pipeline.store.put_if_absent(target.object_key, b"not the ciphertext", metadata={})
    result = pipeline.sync()["result"]
    assert [f["object_key"] for f in result["failures"]] == [target.object_key]
    item = pipeline.queue.get(target.idempotency_key)
    assert item is not None and item.status == "pending"
    assert "VerificationError" in (item.last_error or "")
    assert pipeline.queue.receipt(target.idempotency_key) is None


def test_tiering_refuses_to_reclaim_against_a_tampered_object(pipeline: Pipeline) -> None:
    staged(pipeline)
    pipeline.sync()
    target = pipeline.queue.items()[0]
    flip_byte(pipeline, target.object_key)
    summary = pipeline.tier()["result"]
    assert [r["object_key"] for r in summary["refused"]] == [target.object_key]
    assert pipeline.spool.path(target.spool_path).exists()
    affected = [
        s
        for s in pipeline.log.segments()
        if s.segment_id not in pipeline.log.reclaimed_segment_ids()
    ]
    assert affected, "the segment that produced the tampered object keeps its local rows"


def test_a_valid_object_moved_to_another_key_is_caught_by_the_key_binding(
    pipeline: Pipeline,
) -> None:
    staged(pipeline)
    pipeline.sync()
    original = pipeline.queue.items()[0]
    blob = pipeline.store.get(original.object_key, max_bytes=original.encrypted_bytes)
    pipeline.store.put_if_absent("elsewhere/moved.enc", blob, metadata={})
    moved = replace(original, object_key="elsewhere/moved.enc")
    result = verify_object(pipeline.store, pipeline.keyring, moved)
    # Bytes, tag and plaintext all check out; only the authenticated binding catches it.
    assert result.checks == {
        "bytes_match": True,
        "gcm_tag": True,
        "plaintext_match": True,
        "bound_to_key": False,
    }
    assert not result.ok


def test_failed_items_are_not_retried_in_a_tight_loop(pipeline: Pipeline) -> None:
    staged(pipeline)
    target = pipeline.queue.items()[0]
    pipeline.store.put_if_absent(target.object_key, b"garbage", metadata={})
    summary = SyncSummary()
    while pipeline.uploader.sync_once(summary):
        pass
    assert len(summary.failures) == 1


@pytest.mark.parametrize("workers", [1, 4])
def test_parallel_upload_workers_converge(pipeline: Pipeline, workers: int) -> None:
    staged(pipeline)
    summary = pipeline.uploader.sync_all(workers=workers)
    assert summary.uploaded == len(pipeline.queue.items())
    assert not summary.failures
