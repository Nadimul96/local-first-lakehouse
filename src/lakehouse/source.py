"""The export side's view of the event log: a small protocol and one read-only adapter.

The exporter only needs "give me the next sealed batch after sequence N". Keeping that
behind `EventSource` means the Bronze, queue and sync stages know nothing about how
events were captured, and a different log (Kafka, a CDC stream) needs one adapter.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .db import connect_readonly
from .errors import IntegrityError
from .events import LoggedEvent, segment_digest, verify_content


@dataclass(frozen=True)
class SourceBatch:
    batch_id: str
    first_seq: int
    last_seq: int
    sha256: str
    events: tuple[LoggedEvent, ...]


class EventSource(Protocol):
    name: str

    def next_batch(self, after_seq: int) -> SourceBatch | None:
        """The sealed batch starting at `after_seq + 1`, or None if none is sealed yet."""
        ...


class SQLiteEventSource:
    """Reads sealed segments from the event log without any ability to write to it.

    Every row is re-hashed and every segment digest re-checked on read, so local
    corruption or tampering of the log is caught before it reaches Bronze.
    """

    name = "event-log"

    def __init__(self, path: Path) -> None:
        self.path = path

    def next_batch(self, after_seq: int) -> SourceBatch | None:
        with closing(connect_readonly(self.path)) as db:
            segment = db.execute(
                "SELECT s.*, r.segment_id IS NOT NULL AS reclaimed FROM segments AS s"
                " LEFT JOIN reclaimed_segments AS r USING (segment_id)"
                " WHERE s.last_seq > ? ORDER BY s.first_seq LIMIT 1",
                (after_seq,),
            ).fetchone()
            if segment is None:
                return None
            if segment["first_seq"] != after_seq + 1:
                found, expected = segment["first_seq"], after_seq + 1
                raise IntegrityError(f"next sealed segment starts at {found}, expected {expected}")
            if segment["reclaimed"]:
                raise IntegrityError(f"segment {segment['segment_id']} was reclaimed before export")
            rows = db.execute(
                "SELECT seq, producer_id, producer_seq, event_id, kind, occurred_at,"
                " payload_json, content_sha256 FROM events WHERE seq BETWEEN ? AND ? ORDER BY seq",
                (segment["first_seq"], segment["last_seq"]),
            ).fetchall()
        events = tuple(LoggedEvent(**dict(row)) for row in rows)
        if len(events) != segment["row_count"]:
            raise IntegrityError(f"segment {segment['segment_id']} is missing rows")
        for event in events:
            if not verify_content(event):
                raise IntegrityError(f"event seq {event.seq} does not match its content hash")
        if segment_digest((e.seq, e.content_sha256) for e in events) != segment["sha256"]:
            raise IntegrityError(f"segment {segment['segment_id']} digest mismatch")
        return SourceBatch(
            batch_id=f"seg-{segment['segment_id']:06d}",
            first_seq=segment["first_seq"],
            last_seq=segment["last_seq"],
            sha256=segment["sha256"],
            events=events,
        )
