"""Durable upload queue in SQLite, keyed by idempotency key, with leases and fencing.

State machine for one object::

    pending --claim--> uploading --read-back verified--> object_durable --catalogued--> completed
       ^                   |                                  |
       +----- release -----+          (a claim of an object_durable row resumes here)

* Claims run under `BEGIN IMMEDIATE`, so two workers can never claim the same row.
* Every claim bumps `lease_token`. Writes from a worker whose token is stale are refused
  (`LeaseLostError`), so a worker that stalled past its lease cannot overwrite the work of
  the worker that took over.
* Upload receipts and the transition history are append-only (enforced by triggers).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Collection, Iterator
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

from .canonical import Clock, iso_utc, system_clock
from .db import Migration, connect, immutable_triggers, migrate, transaction
from .errors import CapacityError, ConflictError, LeaseLostError

STATUSES = ("pending", "uploading", "object_durable", "completed")

MIGRATIONS = (
    Migration(
        "001_upload_queue",
        """
        CREATE TABLE queue (
            idempotency_key TEXT PRIMARY KEY,
            object_key TEXT NOT NULL UNIQUE,
            source_path TEXT NOT NULL,
            spool_path TEXT NOT NULL,
            plaintext_sha256 TEXT NOT NULL,
            ciphertext_sha256 TEXT NOT NULL,
            key_id TEXT NOT NULL,
            plaintext_bytes INTEGER NOT NULL CHECK (plaintext_bytes > 0),
            encrypted_bytes INTEGER NOT NULL CHECK (encrypted_bytes > 0),
            status TEXT NOT NULL
                CHECK (status IN ('pending', 'uploading', 'object_durable', 'completed')),
            attempts INTEGER NOT NULL DEFAULT 0,
            lease_owner TEXT,
            lease_token INTEGER NOT NULL DEFAULT 0,
            leased_until REAL,
            available_at REAL NOT NULL,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX queue_claimable ON queue (status, available_at);
        CREATE TABLE upload_receipts (
            idempotency_key TEXT PRIMARY KEY REFERENCES queue (idempotency_key),
            receipt_id TEXT NOT NULL UNIQUE,
            receipt_json TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE TABLE queue_history (
            id INTEGER PRIMARY KEY,
            idempotency_key TEXT NOT NULL,
            status TEXT NOT NULL,
            detail TEXT,
            at REAL NOT NULL
        );
        """
        + immutable_triggers("upload_receipts")
        + immutable_triggers("queue_history"),
    ),
)


@dataclass(frozen=True)
class NewItem:
    idempotency_key: str
    object_key: str
    source_path: str
    spool_path: str
    plaintext_sha256: str
    ciphertext_sha256: str
    key_id: str
    plaintext_bytes: int
    encrypted_bytes: int


@dataclass(frozen=True)
class QueueItem(NewItem):
    status: str
    attempts: int
    lease_owner: str | None
    lease_token: int
    last_error: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class Lease:
    item: QueueItem
    token: int
    owner: str


@dataclass(frozen=True)
class UploadReceipt:
    receipt_id: str
    idempotency_key: str
    object_key: str
    provider: str
    bucket: str
    etag: str | None
    ciphertext_sha256: str
    plaintext_sha256: str
    key_id: str
    encrypted_bytes: int
    created_object: bool
    verified_at: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_NEW_FIELDS = tuple(f.name for f in fields(NewItem))
_ITEM_FIELDS = tuple(f.name for f in fields(QueueItem))


class UploadQueue:
    def __init__(self, path: Path, *, max_pending_bytes: int, clock: Clock = system_clock) -> None:
        if max_pending_bytes <= 0:
            raise ValueError("max_pending_bytes must be positive")
        self.path = path
        self.max_pending_bytes = max_pending_bytes
        self._clock = clock
        with closing(connect(path)) as db:
            migrate(db, MIGRATIONS)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        # One connection per operation keeps the queue safe to share across threads
        # and processes; SQLite's write lock is the only coordination point.
        with closing(connect(self.path)) as db, transaction(db):
            yield db

    def _history(self, db: sqlite3.Connection, key: str, status: str, detail: str) -> None:
        db.execute(
            "INSERT INTO queue_history (idempotency_key, status, detail, at) VALUES (?,?,?,?)",
            (key, status, detail, self._clock()),
        )

    # --- admission -------------------------------------------------------------------

    def enqueue(self, item: NewItem) -> QueueItem:
        """Idempotent: the same key with the same identity returns the existing row."""
        now = iso_utc(self._clock())
        with self._tx() as db:
            existing = _fetch(db, item.idempotency_key)
            if existing is not None:
                if tuple(getattr(existing, f) for f in _NEW_FIELDS) != astuple_new(item):
                    raise ConflictError(f"{item.idempotency_key} re-enqueued with other content")
                return existing
            in_flight = db.execute(
                "SELECT COALESCE(SUM(encrypted_bytes), 0) FROM queue WHERE status != 'completed'"
            ).fetchone()[0]
            if in_flight + item.encrypted_bytes > self.max_pending_bytes:
                raise CapacityError("upload queue is full; sync before exporting more")
            db.execute(
                f"INSERT INTO queue ({', '.join(_NEW_FIELDS)}, status, available_at,"
                f" created_at, updated_at) VALUES ({', '.join('?' * len(_NEW_FIELDS))},"
                " 'pending', ?, ?, ?)",
                (*astuple_new(item), self._clock(), now, now),
            )
            self._history(db, item.idempotency_key, "pending", "enqueued")
            created = _fetch(db, item.idempotency_key)
        if created is None:
            raise ConflictError(f"{item.idempotency_key} vanished during enqueue")
        return created

    # --- leases ----------------------------------------------------------------------

    def claim(
        self, owner: str, *, lease_seconds: float = 300.0, exclude: Collection[str] = ()
    ) -> Lease | None:
        """Lease the oldest claimable item. `exclude` skips items already tried this pass."""
        now = self._clock()
        skip = ", ".join("?" * len(exclude))
        with self._tx() as db:
            row = db.execute(
                "SELECT idempotency_key FROM queue WHERE status != 'completed'"
                " AND available_at <= ? AND (leased_until IS NULL OR leased_until < ?)"
                f" AND idempotency_key NOT IN ({skip})"
                " ORDER BY created_at, idempotency_key LIMIT 1",
                (now, now, *exclude),
            ).fetchone()
            if row is None:
                return None
            key = row[0]
            db.execute(
                "UPDATE queue SET status = CASE status WHEN 'object_durable'"
                " THEN 'object_durable' ELSE 'uploading' END,"
                " attempts = attempts + 1, lease_token = lease_token + 1, lease_owner = ?,"
                " leased_until = ?, updated_at = ? WHERE idempotency_key = ?",
                (owner, now + lease_seconds, iso_utc(now), key),
            )
            item = _fetch(db, key)
            if item is None:
                raise LeaseLostError(f"{key} vanished during claim")
            self._history(db, key, item.status, f"claimed by {owner}")
        return Lease(item=item, token=item.lease_token, owner=owner)

    def _guard(self, db: sqlite3.Connection, lease: Lease) -> QueueItem:
        current = _fetch(db, lease.item.idempotency_key)
        if current is None or (current.lease_token, current.lease_owner) != (
            lease.token,
            lease.owner,
        ):
            raise LeaseLostError(f"lease on {lease.item.idempotency_key} is no longer held")
        return current

    def record_receipt(self, lease: Lease, receipt: UploadReceipt) -> None:
        encoded = json.dumps(receipt.as_dict(), sort_keys=True)
        with self._tx() as db:
            item = self._guard(db, lease)
            stored = db.execute(
                "SELECT receipt_json FROM upload_receipts WHERE idempotency_key=?",
                (item.idempotency_key,),
            ).fetchone()
            if stored is not None:
                if json.loads(stored[0])["receipt_id"] != receipt.receipt_id:
                    raise ConflictError(f"{item.idempotency_key} already has another receipt")
                return
            if (receipt.object_key, receipt.ciphertext_sha256, receipt.encrypted_bytes) != (
                item.object_key,
                item.ciphertext_sha256,
                item.encrypted_bytes,
            ):
                raise ConflictError(f"receipt does not describe {item.idempotency_key}")
            db.execute(
                "INSERT INTO upload_receipts VALUES (?,?,?,?)",
                (item.idempotency_key, receipt.receipt_id, encoded, receipt.verified_at),
            )
            db.execute(
                "UPDATE queue SET status='object_durable', last_error=NULL, updated_at=?"
                " WHERE idempotency_key=?",
                (iso_utc(self._clock()), item.idempotency_key),
            )
            self._history(db, item.idempotency_key, "object_durable", "read-back verified")

    def complete(self, lease: Lease) -> None:
        with self._tx() as db:
            item = self._guard(db, lease)
            if not _has_receipt(db, item.idempotency_key):
                raise ConflictError(f"{item.idempotency_key} cannot complete without a receipt")
            db.execute(
                "UPDATE queue SET status='completed', lease_owner=NULL, leased_until=NULL,"
                " updated_at=? WHERE idempotency_key=?",
                (iso_utc(self._clock()), item.idempotency_key),
            )
            self._history(db, item.idempotency_key, "completed", "catalogued")

    def release(self, lease: Lease, *, error: str, retry_after: float) -> None:
        with self._tx() as db:
            item = self._guard(db, lease)
            status = "object_durable" if _has_receipt(db, item.idempotency_key) else "pending"
            db.execute(
                "UPDATE queue SET status=?, lease_owner=NULL, leased_until=NULL,"
                " available_at=?, last_error=?, updated_at=? WHERE idempotency_key=?",
                (
                    status,
                    self._clock() + retry_after,
                    error[:200],
                    iso_utc(self._clock()),
                    item.idempotency_key,
                ),
            )
            self._history(db, item.idempotency_key, status, f"released: {error[:200]}")

    def recover_orphans(self, live_owner_prefix: str) -> int:
        """Free leases held by dead processes. Safe only while holding the writer lock."""
        with self._tx() as db:
            orphans = db.execute(
                "SELECT idempotency_key FROM queue WHERE status != 'completed'"
                " AND lease_owner IS NOT NULL AND substr(lease_owner, 1, ?) != ?",
                (len(live_owner_prefix), live_owner_prefix),
            ).fetchall()
            for (key,) in orphans:
                status = "object_durable" if _has_receipt(db, key) else "pending"
                db.execute(
                    "UPDATE queue SET status=?, lease_owner=NULL, leased_until=NULL,"
                    " available_at=? WHERE idempotency_key=?",
                    (status, self._clock(), key),
                )
                self._history(db, key, status, "orphaned lease recovered")
        return len(orphans)

    # --- reads -----------------------------------------------------------------------

    def get(self, key: str) -> QueueItem | None:
        with closing(connect(self.path)) as db:
            return _fetch(db, key)

    def items(self) -> list[QueueItem]:
        with closing(connect(self.path)) as db:
            rows = db.execute(
                f"SELECT {', '.join(_ITEM_FIELDS)} FROM queue ORDER BY created_at, idempotency_key"
            ).fetchall()
        return [QueueItem(**dict(row)) for row in rows]

    def receipt(self, key: str) -> UploadReceipt | None:
        with closing(connect(self.path)) as db:
            row = db.execute(
                "SELECT receipt_json FROM upload_receipts WHERE idempotency_key=?", (key,)
            ).fetchone()
        return UploadReceipt(**json.loads(row[0])) if row else None

    def history(self) -> list[dict[str, Any]]:
        with closing(connect(self.path)) as db:
            rows = db.execute("SELECT * FROM queue_history ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    def stats(self) -> dict[str, Any]:
        with closing(connect(self.path)) as db:
            rows = db.execute(
                "SELECT status, COUNT(*), COALESCE(SUM(encrypted_bytes), 0) FROM queue"
                " GROUP BY status"
            ).fetchall()
        counts = dict.fromkeys(STATUSES, 0)
        in_flight_bytes = 0
        for status, count, size in rows:
            counts[status] = count
            if status != "completed":
                in_flight_bytes += size
        return {
            "by_status": counts,
            "in_flight_bytes": in_flight_bytes,
            "cap_bytes": self.max_pending_bytes,
        }


def astuple_new(item: NewItem) -> tuple[Any, ...]:
    return tuple(getattr(item, name) for name in _NEW_FIELDS)


def _fetch(db: sqlite3.Connection, key: str) -> QueueItem | None:
    row = db.execute(
        f"SELECT {', '.join(_ITEM_FIELDS)} FROM queue WHERE idempotency_key=?", (key,)
    ).fetchone()
    return QueueItem(**dict(row)) if row else None


def _has_receipt(db: sqlite3.Connection, key: str) -> bool:
    return (
        db.execute("SELECT 1 FROM upload_receipts WHERE idempotency_key=?", (key,)).fetchone()
        is not None
    )
