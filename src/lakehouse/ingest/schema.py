"""Schema of the event log. Changing a shipped migration fails closed (checksum pinned);
add a new migration instead."""

from __future__ import annotations

from ..db import Migration, immutable_triggers

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
