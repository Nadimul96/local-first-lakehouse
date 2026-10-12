from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

import pytest

from lakehouse.ingest.eventlog import EventLog
from lakehouse.ingest.hub import Ack, IngestHub
from lakehouse.ingest.producers import Workload, deliver, generate
from lakehouse.ingest.redaction import Redactor
from tests.conftest import event


def test_receipts_are_durable_only_after_commit(hub: IngestHub, log: EventLog) -> None:
    receipts = hub.submit([event("web", 1), event("web", 2)])
    assert [r.ack for r in receipts] == [Ack.COMMITTED, Ack.COMMITTED]
    assert [r.seq for r in receipts] == [1, 2]
    assert log.stats()["committed"] == 2


def test_redelivery_is_acknowledged_as_duplicate(hub: IngestHub) -> None:
    hub.submit([event("web", 1)])
    (receipt,) = hub.submit([event("web", 1)])
    assert receipt.ack is Ack.DUPLICATE
    assert receipt.ack.durable


def test_reused_sequence_with_new_content_is_a_conflict(hub: IngestHub, log: EventLog) -> None:
    hub.submit([event("web", 1)])
    (receipt,) = hub.submit([event("web", 1, sku="different")])
    assert receipt.ack is Ack.CONFLICT
    assert log.stats()["conflicts"] == 1


def test_buffered_events_are_pending_until_drained(log: EventLog, redactor: Redactor) -> None:
    hub = IngestHub(log, redactor, segment_rows=100, reorder_window_ms=60_000)
    receipts = hub.submit([event("web", 1), event("web", 2)])
    assert {r.ack for r in receipts} == {Ack.PENDING}
    assert log.stats()["committed"] == 0
    (again,) = hub.submit([event("web", 1)])
    assert again.ack is Ack.PENDING  # not buffered twice
    hub.drain()
    assert hub.submit([event("web", 1)])[0].ack is Ack.DUPLICATE
    assert log.stats()["committed"] == 2


def test_flush_seals_the_tail(hub: IngestHub, log: EventLog) -> None:
    hub.submit([event("web", n) for n in range(1, 4)])
    sealed = hub.flush()
    assert [(s.first_seq, s.last_seq) for s in sealed] == [(1, 3)]


def test_a_failed_commit_does_not_strand_events_as_pending(
    hub: IngestHub, log: EventLog, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = log.commit
    calls = Counter[str]()

    def flaky(*args: object, **kwargs: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("disk I/O error")
        return original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(log, "commit", flaky)
    with pytest.raises(sqlite3.OperationalError):
        hub.submit([event("web", 1)])
    (retry,) = hub.submit([event("web", 1)])
    assert retry.ack is Ack.COMMITTED


def test_secrets_are_redacted_before_they_reach_disk(hub: IngestHub, log: EventLog) -> None:
    email = "jamie" + "@" + "example.com"
    hub.submit([event("support-desk", 1, text=f"reach me at {email}")])
    raw = Path(log.path).read_bytes() + Path(f"{log.path}-wal").read_bytes()
    assert email.encode() not in raw
    assert hub.redactions["email"] == 1


def test_producers_deliver_every_event_exactly_once(hub: IngestHub, log: EventLog) -> None:
    workload = Workload(
        events_per_producer=300, batch_size=40, duplicate_rate=0.05, late_rate=0.05, swap_rate=0.2
    )
    stats = deliver(hub, workload)
    assert stats.redelivered > 0 and stats.held_late > 0
    db = sqlite3.connect(log.path)
    stored = {row[0] for row in db.execute("SELECT event_id FROM events")}
    expected = {e.event_id for p in workload.producers for e in generate(workload, p)}
    assert stored == expected
    assert log.stats()["committed"] == workload.total_events
    assert log.stats()["missing_now"] == 0


def test_generation_is_deterministic() -> None:
    workload = Workload(events_per_producer=50)
    assert generate(workload, "ios-app") == generate(workload, "ios-app")
