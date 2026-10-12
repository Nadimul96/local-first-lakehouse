from __future__ import annotations

import sqlite3

import pytest

from lakehouse.errors import IntegrityError
from lakehouse.events import PreparedEvent, content_sha256
from lakehouse.ingest.eventlog import Admission, EventLog


def prepared(producer: str, seq: int, payload: str = "{}") -> PreparedEvent:
    fields = {
        "producer_id": producer,
        "producer_seq": seq,
        "event_id": f"{producer}-{seq}",
        "kind": "page_view",
        "occurred_at": "2026-10-01T00:00:00.000000Z",
        "payload_json": payload,
    }
    return PreparedEvent(**fields, content_sha256=content_sha256(**fields))  # type: ignore[arg-type]


def test_commit_assigns_contiguous_sequence_in_order(log: EventLog) -> None:
    results = log.commit([prepared("a", 1), prepared("b", 1), prepared("a", 2)])
    assert [r.seq for r in results] == [1, 2, 3]
    assert all(r.admission is Admission.COMMITTED for r in results)
    assert log.stats()["committed"] == 3


def test_same_key_same_content_is_duplicate_different_content_is_conflict(log: EventLog) -> None:
    log.commit([prepared("a", 1)])
    again = log.commit([prepared("a", 1), prepared("a", 1, payload='{"x":1}')])
    assert [r.admission for r in again] == [Admission.DUPLICATE, Admission.CONFLICT]
    stats = log.stats()
    assert (stats["committed"], stats["duplicates"], stats["conflicts"]) == (1, 1, 1)


def test_watermark_records_gap_and_late_arrival_closes_it(log: EventLog) -> None:
    log.commit([prepared("a", 1), prepared("a", 5)])
    gaps = log.stats()["open_gaps"]
    assert [(g["start_seq"], g["end_seq"]) for g in gaps] == [(2, 4)]
    log.commit([prepared("a", 3)])
    assert [(g["start_seq"], g["end_seq"]) for g in log.stats()["open_gaps"]] == [(2, 2), (4, 4)]
    log.commit([prepared("a", 2), prepared("a", 4)])
    stats = log.stats()
    assert stats["missing_now"] == 0
    assert stats["late_arrivals"] == 3
    assert stats["peak_missing"] == 3


def test_seal_creates_full_segments_and_optional_tail(log: EventLog) -> None:
    log.commit([prepared("a", n) for n in range(1, 26)])
    sealed = log.seal(max_rows=10)
    assert [(s.first_seq, s.last_seq) for s in sealed] == [(1, 10), (11, 20)]
    tail = log.seal(max_rows=10, include_tail=True)
    assert [(s.first_seq, s.last_seq) for s in tail] == [(21, 25)]
    assert log.seal(max_rows=10, include_tail=True) == []


def test_events_and_segments_are_immutable_in_the_database(log: EventLog) -> None:
    log.commit([prepared("a", n) for n in range(1, 4)])
    log.seal(max_rows=3)
    db = sqlite3.connect(log.path)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        db.execute("UPDATE events SET payload_json='{}' WHERE seq=1")
    with pytest.raises(sqlite3.IntegrityError, match="recorded reclamation"):
        db.execute("DELETE FROM events WHERE seq=1")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        db.execute("DELETE FROM segments")


def test_reclaim_deletes_rows_but_dedup_survives_via_watermarks(log: EventLog) -> None:
    log.commit([prepared("a", n) for n in range(1, 11)])
    (segment,) = log.seal(max_rows=10)
    first = log.reclaim_segment(segment.segment_id, {"proof": "test"})
    assert first.rows_deleted == 10 and first.payload_bytes == 20
    assert log.reclaim_segment(segment.segment_id, {"proof": "other"}) == first  # idempotent
    assert log.stats()["hot_rows"] == 0
    results = log.commit([prepared("a", 3), prepared("a", 11)])
    assert [r.admission for r in results] == [Admission.DUPLICATE, Admission.COMMITTED]
    assert results[1].seq == 11  # the sequence continues past reclaimed rows


def test_reclaim_requires_a_sealed_segment(log: EventLog) -> None:
    with pytest.raises(IntegrityError):
        log.reclaim_segment(99, {})


def test_reclamation_returns_space_to_the_filesystem(log: EventLog) -> None:
    payload = '{"text":"' + "x" * 4000 + '"}'
    log.commit([prepared("a", n, payload) for n in range(1, 501)])
    (segment,) = log.seal(max_rows=500)
    before = log.stats()["file_bytes"]
    log.reclaim_segment(segment.segment_id, {})
    assert log.stats()["file_bytes"] < before / 4
