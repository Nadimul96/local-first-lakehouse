from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from lakehouse.bronze import BronzeFile, BronzeWriter
from lakehouse.errors import ConflictError, IntegrityError
from lakehouse.export import Exporter, ExportLedger
from lakehouse.ingest.eventlog import EventLog
from lakehouse.ingest.hub import IngestHub
from lakehouse.source import SQLiteEventSource
from tests.conftest import event


class RecordingStager:
    def __init__(self) -> None:
        self.staged: list[str] = []

    def stage(self, file: BronzeFile) -> str:
        self.staged.append(file.relative_path)
        return f"key-{file.relative_path}"


def fill(hub: IngestHub, count: int) -> None:
    hub.submit([event("web", n, second=n * 1800) for n in range(1, count + 1)])  # spans 2 days
    hub.flush()


def test_source_reads_sealed_batches_contiguously(hub: IngestHub, log: EventLog) -> None:
    fill(hub, 25)
    source = SQLiteEventSource(log.path)
    first = source.next_batch(0)
    assert first is not None and (first.first_seq, first.last_seq) == (1, 10)
    second = source.next_batch(10)
    assert second is not None and second.first_seq == 11
    assert source.next_batch(25) is None
    with pytest.raises(IntegrityError, match="expected 6"):
        source.next_batch(5)


def test_source_connection_cannot_write(hub: IngestHub, log: EventLog) -> None:
    fill(hub, 3)
    from lakehouse.db import connect_readonly

    db = connect_readonly(log.path)
    with pytest.raises(sqlite3.OperationalError):
        db.execute("INSERT INTO log_meta VALUES ('x', 1)")


def test_source_detects_a_tampered_row(hub: IngestHub, log: EventLog) -> None:
    fill(hub, 10)
    raw = sqlite3.connect(log.path)
    raw.execute("DROP TRIGGER events_no_update")  # simulate corruption below the API
    raw.execute('UPDATE events SET payload_json=\'{"user_id":"forged"}\' WHERE seq=4')
    raw.commit()
    with pytest.raises(IntegrityError, match="content hash"):
        SQLiteEventSource(log.path).next_batch(0)


def test_bronze_partitions_by_day_and_is_readable_by_duckdb(
    hub: IngestHub, log: EventLog, tmp_path: Path
) -> None:
    fill(hub, 10)
    batch = SQLiteEventSource(log.path).next_batch(0)
    assert batch is not None
    files = BronzeWriter(tmp_path / "lake").write(batch)
    assert sum(f.rows for f in files) == 10
    assert len({f.event_date for f in files}) == 1
    path = str(tmp_path / "lake" / files[0].relative_path)
    with duckdb.connect() as con:
        rows = con.execute(
            "SELECT count(*), min(seq), max(seq) FROM read_parquet(?, hive_partitioning=true)",
            [path],
        ).fetchone()
        meta = dict(con.execute("SELECT key, value FROM parquet_kv_metadata(?)", [path]).fetchall())
    assert rows == (10, 1, 10)
    assert meta[b"schema"] == b"lakehouse.bronze.v1"


def test_rewriting_bronze_verifies_instead_of_overwriting(
    hub: IngestHub, log: EventLog, tmp_path: Path
) -> None:
    fill(hub, 10)
    batch = SQLiteEventSource(log.path).next_batch(0)
    assert batch is not None
    writer = BronzeWriter(tmp_path / "lake")
    first = writer.write(batch)
    assert writer.write(batch) == first
    forged = replace(batch, events=tuple(replace(e, kind="forged") for e in batch.events))
    with pytest.raises(IntegrityError, match="different rows"):
        writer.write(forged)


def exporter_for(log: EventLog, tmp_path: Path) -> tuple[Exporter, ExportLedger, RecordingStager]:
    ledger = ExportLedger(tmp_path / "export.sqlite", source="event-log")
    stager = RecordingStager()
    exporter = Exporter(
        source=SQLiteEventSource(log.path),
        writer=BronzeWriter(tmp_path / "lake"),
        ledger=ledger,
        stager=stager,
    )
    return exporter, ledger, stager


def test_export_moves_the_cursor_once_per_segment(
    hub: IngestHub, log: EventLog, tmp_path: Path
) -> None:
    fill(hub, 25)
    exporter, ledger, stager = exporter_for(log, tmp_path)
    receipts = exporter.export_all()
    assert [(r.first_seq, r.high_water) for r in receipts] == [(1, 10), (11, 20), (21, 25)]
    assert ledger.high_water() == 25 and ledger.active() is None
    assert len(stager.staged) == sum(len(r.files) for r in receipts)
    assert exporter.export_all() == []


def test_interrupted_export_resumes_the_same_reservation(
    hub: IngestHub, log: EventLog, tmp_path: Path
) -> None:
    fill(hub, 10)
    exporter, ledger, _ = exporter_for(log, tmp_path)
    batch = SQLiteEventSource(log.path).next_batch(0)
    assert batch is not None
    reservation = ledger.reserve(batch)
    assert ledger.reserve(batch) == reservation  # re-reserving is idempotent
    receipt = exporter.export_next()
    assert receipt is not None and receipt.reservation_id == reservation.reservation_id
    assert ledger.high_water() == 10


def test_ledger_refuses_overlap_and_gaps(hub: IngestHub, log: EventLog, tmp_path: Path) -> None:
    fill(hub, 25)
    exporter, ledger, _ = exporter_for(log, tmp_path)
    exporter.export_next()
    source = SQLiteEventSource(log.path)
    later = source.next_batch(20)
    assert later is not None
    with pytest.raises(ConflictError, match="cursor is 10"):
        ledger.reserve(later)


def test_receipts_are_append_only(hub: IngestHub, log: EventLog, tmp_path: Path) -> None:
    fill(hub, 10)
    exporter, _, _ = exporter_for(log, tmp_path)
    exporter.export_all()
    raw = sqlite3.connect(tmp_path / "export.sqlite")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        raw.execute("DELETE FROM export_receipts")


def test_tampered_receipt_chain_fails_on_open(
    hub: IngestHub, log: EventLog, tmp_path: Path
) -> None:
    fill(hub, 25)
    exporter, ledger, _ = exporter_for(log, tmp_path)
    exporter.export_all()
    ledger.close()
    raw = sqlite3.connect(tmp_path / "export.sqlite")
    raw.execute("UPDATE export_cursor SET high_water = 20")
    raw.commit()
    with pytest.raises(IntegrityError, match="does not match"):
        ExportLedger(tmp_path / "export.sqlite", source="event-log")
