"""The durable local event log: the commit point every producer acknowledgement depends on.

One SQLite file (WAL, `synchronous=FULL`) holds:

* `events`: committed rows keyed by a global `seq`, assigned inside the commit transaction
  so the sequence never has holes (a failed commit assigns nothing).
* `producer_watermarks` + `producer_gaps`: for each producer, the highest `producer_seq`
  seen and the exact ranges below it that have not arrived. Together they describe the
  full set of committed producer sequences, so duplicates are detected even after the
  rows themselves have been reclaimed, and missing events are proven, not assumed.
* `segments`: sealed, immutable, hash-digested ranges of `seq`. Export works on sealed
  segments only; reclamation deletes a segment's rows only after a recorded proof.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from ..canonical import Clock, iso_utc, system_clock
from ..db import Migration, connect, immutable_triggers, migrate, transaction
from ..errors import IntegrityError
from ..events import PreparedEvent, segment_digest

_COUNTERS = ("last_seq", "duplicates", "conflicts", "late_arrivals", "peak_missing")

MIGRATIONS = (
    Migration(
        "001_event_log",
        """
        CREATE TABLE log_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID;
        INSERT INTO log_meta (key, value) VALUES
            ('last_seq', 0), ('duplicates', 0), ('conflicts', 0),
            ('late_arrivals', 0), ('peak_missing', 0);

        CREATE TABLE events (
            seq INTEGER NOT NULL PRIMARY KEY,
            producer_id TEXT NOT NULL,
            producer_seq INTEGER NOT NULL CHECK (producer_seq > 0),
            event_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            committed_at TEXT NOT NULL
        ) WITHOUT ROWID;
        CREATE UNIQUE INDEX events_by_producer ON events (producer_id, producer_seq);

        CREATE TABLE producer_watermarks (
            producer_id TEXT NOT NULL PRIMARY KEY,
            max_seen INTEGER NOT NULL CHECK (max_seen > 0),
            committed INTEGER NOT NULL CHECK (committed > 0)
        ) WITHOUT ROWID;

        CREATE TABLE producer_gaps (
            producer_id TEXT NOT NULL,
            start_seq INTEGER NOT NULL,
            end_seq INTEGER NOT NULL CHECK (end_seq >= start_seq),
            opened_at TEXT NOT NULL,
            PRIMARY KEY (producer_id, start_seq)
        ) WITHOUT ROWID;

        CREATE TABLE segments (
            segment_id INTEGER PRIMARY KEY,
            first_seq INTEGER NOT NULL UNIQUE,
            last_seq INTEGER NOT NULL UNIQUE CHECK (last_seq >= first_seq),
            row_count INTEGER NOT NULL CHECK (row_count = last_seq - first_seq + 1),
            sha256 TEXT NOT NULL,
            sealed_at TEXT NOT NULL
        );

        CREATE TABLE reclaimed_segments (
            segment_id INTEGER PRIMARY KEY REFERENCES segments (segment_id),
            rows_deleted INTEGER NOT NULL,
            payload_bytes INTEGER NOT NULL,
            proof_json TEXT NOT NULL,
            reclaimed_at TEXT NOT NULL
        );

        CREATE TRIGGER events_no_update BEFORE UPDATE ON events
        BEGIN SELECT RAISE(ABORT, 'events are immutable'); END;

        CREATE TRIGGER events_delete_requires_reclamation BEFORE DELETE ON events
        WHEN NOT EXISTS (
            SELECT 1 FROM segments AS s JOIN reclaimed_segments AS r USING (segment_id)
            WHERE OLD.seq BETWEEN s.first_seq AND s.last_seq
        )
        BEGIN SELECT RAISE(ABORT, 'event rows are deleted only by a recorded reclamation'); END;
        """
        + immutable_triggers("segments")
        + immutable_triggers("reclaimed_segments"),
    ),
)


class Admission(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class CommitResult:
    event: PreparedEvent
    admission: Admission
    seq: int | None


@dataclass(frozen=True)
class Segment:
    segment_id: int
    first_seq: int
    last_seq: int
    row_count: int
    sha256: str
    sealed_at: str

    @property
    def name(self) -> str:
        return f"seg-{self.segment_id:06d}"


@dataclass(frozen=True)
class Reclamation:
    segment_id: int
    rows_deleted: int
    payload_bytes: int
    reclaimed_at: str


class EventLog:
    def __init__(self, path: Path, *, clock: Clock = system_clock) -> None:
        self.path = path
        self._clock = clock
        self._lock = threading.RLock()
        self._db = connect(path, auto_vacuum=True)
        migrate(self._db, MIGRATIONS)

    def close(self) -> None:
        self._db.close()

    # --- admission -------------------------------------------------------------------

    def classify(self, event: PreparedEvent) -> Admission | None:
        """Read-only pre-check so the hub can answer duplicates without buffering them."""
        with self._lock:
            return _seen(self._db, event)

    def commit(
        self,
        events: Sequence[PreparedEvent],
        *,
        rejected: Mapping[str, int] | None = None,
    ) -> list[CommitResult]:
        """Commit events in order in one fsynced transaction. Re-checks dedup authoritatively.

        `rejected` carries duplicate/conflict counts the hub resolved before buffering, so
        the persisted counters stay exact without an extra fsync.
        """
        rejected = dict(rejected or {})
        if not events and not any(rejected.values()):
            return []
        now = iso_utc(self._clock())
        results: list[CommitResult] = []
        with self._lock, transaction(self._db) as db:
            meta = _read_meta(db)
            last_seq = meta["last_seq"]
            for event in events:
                seen = _seen(db, event)
                if seen is not None:
                    counter = "duplicates" if seen is Admission.DUPLICATE else "conflicts"
                    rejected[counter] = rejected.get(counter, 0) + 1
                    results.append(CommitResult(event, seen, None))
                    continue
                last_seq += 1
                db.execute(
                    "INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        last_seq,
                        event.producer_id,
                        event.producer_seq,
                        event.event_id,
                        event.kind,
                        event.occurred_at,
                        event.payload_json,
                        event.content_sha256,
                        now,
                    ),
                )
                if _advance_watermark(db, event, now):
                    meta["late_arrivals"] += 1
                results.append(CommitResult(event, Admission.COMMITTED, last_seq))
            meta["last_seq"] = last_seq
            meta["duplicates"] += rejected.get("duplicates", 0)
            meta["conflicts"] += rejected.get("conflicts", 0)
            meta["peak_missing"] = max(meta["peak_missing"], _missing_count(db))
            db.executemany(
                "UPDATE log_meta SET value=? WHERE key=?", [(v, k) for k, v in meta.items()]
            )
        return results

    # --- sealing ---------------------------------------------------------------------

    def seal(self, *, max_rows: int, include_tail: bool = False) -> list[Segment]:
        """Seal every full run of `max_rows` unsealed rows (and the remainder if asked)."""
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        sealed: list[Segment] = []
        with self._lock, transaction(self._db) as db:
            last_seq = _read_meta(db)["last_seq"]
            next_first = db.execute(
                "SELECT COALESCE(MAX(last_seq), 0) + 1 FROM segments"
            ).fetchone()[0]
            while last_seq - next_first + 1 >= (1 if include_tail else max_rows):
                last = min(next_first + max_rows - 1, last_seq)
                sealed.append(self._seal_range(db, next_first, last))
                next_first = last + 1
        return sealed

    def _seal_range(self, db: sqlite3.Connection, first: int, last: int) -> Segment:
        rows = db.execute(
            "SELECT seq, content_sha256 FROM events WHERE seq BETWEEN ? AND ? ORDER BY seq",
            (first, last),
        ).fetchall()
        if len(rows) != last - first + 1:
            raise IntegrityError(f"log range {first}-{last} is not contiguous")
        digest = segment_digest((row[0], row[1]) for row in rows)
        sealed_at = iso_utc(self._clock())
        cursor = db.execute(
            "INSERT INTO segments (first_seq, last_seq, row_count, sha256, sealed_at)"
            " VALUES (?,?,?,?,?)",
            (first, last, len(rows), digest, sealed_at),
        )
        return Segment(int(cursor.lastrowid or 0), first, last, len(rows), digest, sealed_at)

    def segments(self) -> list[Segment]:
        with self._lock:
            rows = self._db.execute("SELECT * FROM segments ORDER BY segment_id").fetchall()
        return [Segment(**dict(row)) for row in rows]

    # --- reclamation -----------------------------------------------------------------

    def reclaim_segment(self, segment_id: int, proof: Mapping[str, Any]) -> Reclamation:
        """Delete a sealed segment's rows, recording the proof first. Idempotent."""
        with self._lock:
            with transaction(self._db) as db:
                existing = db.execute(
                    "SELECT * FROM reclaimed_segments WHERE segment_id=?", (segment_id,)
                ).fetchone()
                if existing is not None:
                    return _reclamation(existing)
                segment = db.execute(
                    "SELECT first_seq, last_seq FROM segments WHERE segment_id=?", (segment_id,)
                ).fetchone()
                if segment is None:
                    raise IntegrityError(f"segment {segment_id} is not sealed")
                rows, payload = db.execute(
                    "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload_json)), 0) FROM events"
                    " WHERE seq BETWEEN ? AND ?",
                    (segment["first_seq"], segment["last_seq"]),
                ).fetchone()
                record = (segment_id, rows, payload, json.dumps(proof, sort_keys=True))
                db.execute(
                    "INSERT INTO reclaimed_segments VALUES (?,?,?,?,?)",
                    (*record, iso_utc(self._clock())),
                )
                db.execute(
                    "DELETE FROM events WHERE seq BETWEEN ? AND ?",
                    (segment["first_seq"], segment["last_seq"]),
                )
                result = db.execute(
                    "SELECT * FROM reclaimed_segments WHERE segment_id=?", (segment_id,)
                ).fetchone()
            # Return freed pages to the OS so the reclaimed bytes show up on disk.
            self._db.execute("PRAGMA incremental_vacuum").fetchall()  # steps until done
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return _reclamation(result)

    def reclaimed_segment_ids(self) -> set[int]:
        with self._lock:
            rows = self._db.execute("SELECT segment_id FROM reclaimed_segments").fetchall()
        return {row[0] for row in rows}

    # --- observability ---------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        with self._lock:
            db = self._db
            meta = _read_meta(db)
            hot_rows = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            sealed_through = db.execute(
                "SELECT COALESCE(MAX(last_seq), 0) FROM segments"
            ).fetchone()[0]
            segment_count = db.execute("SELECT COUNT(*) FROM segments").fetchone()[0]
            reclaimed = db.execute(
                "SELECT COUNT(*), COALESCE(SUM(rows_deleted), 0),"
                " COALESCE(SUM(payload_bytes), 0) FROM reclaimed_segments"
            ).fetchone()
            producers = [
                dict(row)
                for row in db.execute(
                    "SELECT w.producer_id, w.max_seen, w.committed,"
                    " COALESCE(SUM(g.end_seq - g.start_seq + 1), 0) AS missing"
                    " FROM producer_watermarks AS w"
                    " LEFT JOIN producer_gaps AS g USING (producer_id)"
                    " GROUP BY w.producer_id ORDER BY w.producer_id"
                ).fetchall()
            ]
            gaps = [dict(row) for row in db.execute("SELECT * FROM producer_gaps").fetchall()]
        return {
            "committed": meta["last_seq"],
            "duplicates": meta["duplicates"],
            "conflicts": meta["conflicts"],
            "late_arrivals": meta["late_arrivals"],
            "peak_missing": meta["peak_missing"],
            "missing_now": sum(g["end_seq"] - g["start_seq"] + 1 for g in gaps),
            "open_gaps": gaps,
            "hot_rows": hot_rows,
            "segments": segment_count,
            "sealed_through": sealed_through,
            "unsealed_rows": meta["last_seq"] - sealed_through,
            "reclaimed_segments": reclaimed[0],
            "reclaimed_rows": reclaimed[1],
            "reclaimed_payload_bytes": reclaimed[2],
            "producers": producers,
            "file_bytes": _db_file_bytes(self.path),
        }


def _read_meta(db: sqlite3.Connection) -> dict[str, int]:
    meta = {row[0]: int(row[1]) for row in db.execute("SELECT key, value FROM log_meta")}
    return {key: meta[key] for key in _COUNTERS}


def _seen(db: sqlite3.Connection, event: PreparedEvent) -> Admission | None:
    row = db.execute(
        "SELECT content_sha256 FROM events WHERE producer_id=? AND producer_seq=?",
        event.key,
    ).fetchone()
    if row is not None:
        same = row[0] == event.content_sha256
        return Admission.DUPLICATE if same else Admission.CONFLICT
    watermark = db.execute(
        "SELECT max_seen FROM producer_watermarks WHERE producer_id=?", (event.producer_id,)
    ).fetchone()
    if watermark is None or event.producer_seq > watermark[0]:
        return None
    in_gap = db.execute(
        "SELECT 1 FROM producer_gaps WHERE producer_id=? AND start_seq<=? AND end_seq>=?",
        (event.producer_id, event.producer_seq, event.producer_seq),
    ).fetchone()
    # Below the watermark and not in a gap means it was committed and later reclaimed.
    # The content can no longer be compared, so it is acknowledged as a duplicate.
    return None if in_gap else Admission.DUPLICATE


def _advance_watermark(db: sqlite3.Connection, event: PreparedEvent, now: str) -> bool:
    """Record `producer_seq` as seen. Returns True if it filled a gap (a late arrival)."""
    producer, seq = event.key
    row = db.execute(
        "SELECT max_seen FROM producer_watermarks WHERE producer_id=?", (producer,)
    ).fetchone()
    max_seen = row[0] if row else 0
    if seq > max_seen:
        if seq > max_seen + 1:
            db.execute(
                "INSERT INTO producer_gaps VALUES (?,?,?,?)", (producer, max_seen + 1, seq - 1, now)
            )
        db.execute(
            "INSERT INTO producer_watermarks VALUES (?,?,1) ON CONFLICT (producer_id)"
            " DO UPDATE SET max_seen=excluded.max_seen, committed=committed+1",
            (producer, seq),
        )
        return False
    gap = db.execute(
        "SELECT start_seq, end_seq, opened_at FROM producer_gaps"
        " WHERE producer_id=? AND start_seq<=? AND end_seq>=?",
        (producer, seq, seq),
    ).fetchone()
    if gap is None:
        raise IntegrityError(f"{producer}:{seq} is below the watermark but not in a gap")
    start, end, opened_at = gap
    db.execute("DELETE FROM producer_gaps WHERE producer_id=? AND start_seq=?", (producer, start))
    if start < seq:
        db.execute(
            "INSERT INTO producer_gaps VALUES (?,?,?,?)", (producer, start, seq - 1, opened_at)
        )
    if seq < end:
        db.execute(
            "INSERT INTO producer_gaps VALUES (?,?,?,?)", (producer, seq + 1, end, opened_at)
        )
    db.execute(
        "UPDATE producer_watermarks SET committed=committed+1 WHERE producer_id=?", (producer,)
    )
    return True


def _missing_count(db: sqlite3.Connection) -> int:
    return int(
        db.execute(
            "SELECT COALESCE(SUM(end_seq - start_seq + 1), 0) FROM producer_gaps"
        ).fetchone()[0]
    )


def _reclamation(row: sqlite3.Row) -> Reclamation:
    return Reclamation(
        segment_id=row["segment_id"],
        rows_deleted=row["rows_deleted"],
        payload_bytes=row["payload_bytes"],
        reclaimed_at=row["reclaimed_at"],
    )


def _db_file_bytes(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (path, path.with_name(path.name + "-wal"))
        if candidate.exists()
    )
