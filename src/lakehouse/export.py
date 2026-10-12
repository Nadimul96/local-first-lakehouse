"""Exactly-once export from the event log to Bronze, one sealed segment at a time.

Protocol (each step is safe to repeat after a crash):

1. reserve: record the next contiguous window as the single active reservation;
2. write the window's Bronze files (no-clobber, verified if already present);
3. stage each file: encrypt it and admit it to the durable upload queue;
4. commit: in one transaction, store an immutable receipt, advance the high-water
   cursor and clear the reservation.

The cursor moves only after the objects are in the queue, so an advanced cursor always
means "these events are on their way to remote storage". The ledger refuses any window
that does not start exactly at high-water + 1: no overlap, no gap.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .bronze import BronzeFile, BronzeWriter
from .canonical import Clock, canonical_json, iso_utc, sha256_hex, system_clock
from .db import Migration, connect, immutable_triggers, migrate, transaction
from .errors import ConflictError, IntegrityError
from .faults import Faults
from .source import EventSource, SourceBatch

MIGRATIONS = (
    Migration(
        "001_export_ledger",
        """
        CREATE TABLE export_cursor (
            source TEXT PRIMARY KEY,
            high_water INTEGER NOT NULL CHECK (high_water >= 0),
            last_receipt_id TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE active_export (
            source TEXT PRIMARY KEY REFERENCES export_cursor (source),
            reservation_json TEXT NOT NULL
        );
        CREATE TABLE export_receipts (
            receipt_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            first_seq INTEGER NOT NULL,
            high_water INTEGER NOT NULL,
            receipt_json TEXT NOT NULL,
            UNIQUE (source, first_seq),
            UNIQUE (source, high_water)
        );
        """
        + immutable_triggers("export_receipts"),
    ),
)


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    batch_id: str
    batch_sha256: str
    previous_high_water: int
    first_seq: int
    last_seq: int
    created_at: str

    @classmethod
    def for_batch(cls, batch: SourceBatch, previous: int, created_at: str) -> Reservation:
        return cls(
            reservation_id=f"res-{batch.sha256[:16]}",
            batch_id=batch.batch_id,
            batch_sha256=batch.sha256,
            previous_high_water=previous,
            first_seq=batch.first_seq,
            last_seq=batch.last_seq,
            created_at=created_at,
        )

    def matches(self, batch: SourceBatch) -> bool:
        return (self.batch_sha256, self.first_seq, self.last_seq) == (
            batch.sha256,
            batch.first_seq,
            batch.last_seq,
        )


@dataclass(frozen=True)
class ExportReceipt:
    receipt_id: str
    reservation_id: str
    batch_id: str
    batch_sha256: str
    first_seq: int
    high_water: int
    files: tuple[BronzeFile, ...]
    queue_keys: tuple[str, ...]
    durable_at: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, raw: str) -> ExportReceipt:
        value = json.loads(raw)
        value["files"] = tuple(BronzeFile(**f) for f in value["files"])
        value["queue_keys"] = tuple(value["queue_keys"])
        return cls(**value)


class Stager(Protocol):
    def stage(self, file: BronzeFile) -> str:
        """Encrypt the file, admit it to the upload queue, return its idempotency key."""
        ...


class ExportLedger:
    def __init__(self, path: Path, *, source: str, clock: Clock = system_clock) -> None:
        self.source = source
        self._clock = clock
        self._db = connect(path)
        migrate(self._db, MIGRATIONS)
        with transaction(self._db) as db:
            db.execute(
                "INSERT OR IGNORE INTO export_cursor VALUES (?, 0, NULL, ?)",
                (source, iso_utc(clock())),
            )
            self._validate_chain(db)

    def close(self) -> None:
        self._db.close()

    def high_water(self) -> int:
        row = self._db.execute(
            "SELECT high_water FROM export_cursor WHERE source=?", (self.source,)
        ).fetchone()
        return int(row[0])

    def active(self) -> Reservation | None:
        row = self._db.execute(
            "SELECT reservation_json FROM active_export WHERE source=?", (self.source,)
        ).fetchone()
        return Reservation(**json.loads(row[0])) if row else None

    def reserve(self, batch: SourceBatch) -> Reservation:
        with transaction(self._db) as db:
            high_water = db.execute(
                "SELECT high_water FROM export_cursor WHERE source=?", (self.source,)
            ).fetchone()[0]
            proposed = Reservation.for_batch(batch, high_water, iso_utc(self._clock()))
            row = db.execute(
                "SELECT reservation_json FROM active_export WHERE source=?", (self.source,)
            ).fetchone()
            if row is not None:
                active = Reservation(**json.loads(row[0]))
                if not active.matches(batch):
                    raise ConflictError(f"{active.reservation_id} is already active")
                return active
            if batch.first_seq != high_water + 1:
                raise ConflictError(f"window starts at {batch.first_seq}, cursor is {high_water}")
            db.execute(
                "INSERT INTO active_export VALUES (?, ?)",
                (self.source, json.dumps(asdict(proposed), sort_keys=True)),
            )
        return proposed

    def commit(
        self, reservation: Reservation, files: Sequence[BronzeFile], queue_keys: Sequence[str]
    ) -> ExportReceipt:
        _check_coverage(reservation, files)
        receipt = ExportReceipt(
            receipt_id=_receipt_id(reservation, files),
            reservation_id=reservation.reservation_id,
            batch_id=reservation.batch_id,
            batch_sha256=reservation.batch_sha256,
            first_seq=reservation.first_seq,
            high_water=reservation.last_seq,
            files=tuple(files),
            queue_keys=tuple(queue_keys),
            durable_at=iso_utc(self._clock()),
        )
        with transaction(self._db) as db:
            existing = db.execute(
                "SELECT receipt_json FROM export_receipts WHERE source=? AND first_seq=?",
                (self.source, reservation.first_seq),
            ).fetchone()
            if existing is not None:
                return self._existing(existing[0], receipt)
            self._require_active(db, reservation)
            self._append_receipt(db, receipt)
        return receipt

    def receipts(self) -> list[ExportReceipt]:
        rows = self._db.execute(
            "SELECT receipt_json FROM export_receipts WHERE source=? ORDER BY high_water",
            (self.source,),
        ).fetchall()
        return [ExportReceipt.from_json(row[0]) for row in rows]

    @staticmethod
    def _existing(raw: str, proposed: ExportReceipt) -> ExportReceipt:
        stored = ExportReceipt.from_json(raw)
        if stored.receipt_id != proposed.receipt_id:
            raise ConflictError(f"{stored.batch_id} was already exported with other files")
        return stored

    def _require_active(self, db: sqlite3.Connection, reservation: Reservation) -> None:
        row = db.execute(
            "SELECT reservation_json FROM active_export WHERE source=?", (self.source,)
        ).fetchone()
        if row is None or Reservation(**json.loads(row[0])) != reservation:
            raise ConflictError(f"{reservation.reservation_id} is not the active reservation")

    def _append_receipt(self, db: sqlite3.Connection, receipt: ExportReceipt) -> None:
        db.execute(
            "INSERT INTO export_receipts VALUES (?,?,?,?,?)",
            (
                receipt.receipt_id,
                self.source,
                receipt.first_seq,
                receipt.high_water,
                json.dumps(receipt.as_dict(), sort_keys=True),
            ),
        )
        moved = db.execute(
            "UPDATE export_cursor SET high_water=?, last_receipt_id=?, updated_at=?"
            " WHERE source=? AND high_water=?",
            (
                receipt.high_water,
                receipt.receipt_id,
                receipt.durable_at,
                self.source,
                receipt.first_seq - 1,
            ),
        )
        if moved.rowcount != 1:
            raise ConflictError("export cursor moved concurrently")
        db.execute("DELETE FROM active_export WHERE source=?", (self.source,))

    def _validate_chain(self, db: sqlite3.Connection) -> None:
        expected_first, last_id = 1, None
        for row in db.execute(
            "SELECT receipt_id, receipt_json FROM export_receipts WHERE source=?"
            " ORDER BY high_water",
            (self.source,),
        ):
            receipt = ExportReceipt.from_json(row[1])
            if receipt.first_seq != expected_first or receipt.receipt_id != row[0]:
                raise IntegrityError(f"export receipt chain breaks at {row[0]}")
            expected_first, last_id = receipt.high_water + 1, receipt.receipt_id
        cursor = db.execute(
            "SELECT high_water, last_receipt_id FROM export_cursor WHERE source=?", (self.source,)
        ).fetchone()
        if (cursor[0], cursor[1]) != (expected_first - 1, last_id):
            raise IntegrityError("export cursor does not match the receipt chain")


def _check_coverage(reservation: Reservation, files: Sequence[BronzeFile]) -> None:
    covered = sum(file.rows for file in files)
    expected = reservation.last_seq - reservation.first_seq + 1
    if not files or covered != expected:
        raise IntegrityError(f"{reservation.batch_id}: files cover {covered} of {expected} rows")
    if len({file.relative_path for file in files}) != len(files):
        raise IntegrityError(f"{reservation.batch_id}: duplicate file paths")
    if (
        min(f.first_seq for f in files) != reservation.first_seq
        or max(f.last_seq for f in files) != reservation.last_seq
    ):
        raise IntegrityError(f"{reservation.batch_id}: files do not span the reservation")


def _receipt_id(reservation: Reservation, files: Sequence[BronzeFile]) -> str:
    identity = {
        "reservation_id": reservation.reservation_id,
        "files": sorted((f.relative_path, f.sha256) for f in files),
    }
    return f"exp-{sha256_hex(canonical_json(identity))[:16]}"


class Exporter:
    def __init__(
        self,
        *,
        source: EventSource,
        writer: BronzeWriter,
        ledger: ExportLedger,
        stager: Stager,
        faults: Faults | None = None,
    ) -> None:
        self.source = source
        self.writer = writer
        self.ledger = ledger
        self.stager = stager
        self.faults = faults or Faults()

    def export_next(self) -> ExportReceipt | None:
        reservation = self.ledger.active()
        if reservation is None:
            batch = self.source.next_batch(self.ledger.high_water())
            if batch is None:
                return None
            reservation = self.ledger.reserve(batch)
        else:
            # Resume the interrupted window; the source must still hold identical events.
            resumed = self.source.next_batch(reservation.previous_high_water)
            if resumed is None or not reservation.matches(resumed):
                raise IntegrityError(f"source changed under {reservation.reservation_id}")
            batch = resumed
        self.faults.checkpoint("export.after_reserve")
        files = self.writer.write(batch)
        self.faults.checkpoint("export.after_parquet_write")
        queue_keys = [self.stager.stage(file) for file in files]
        self.faults.checkpoint("export.after_stage")
        receipt = self.ledger.commit(reservation, files, queue_keys)
        self.faults.checkpoint("export.after_commit")
        return receipt

    def export_all(self) -> list[ExportReceipt]:
        receipts = []
        while (receipt := self.export_next()) is not None:
            receipts.append(receipt)
        return receipts
