"""Reclaim local copies only after the remote copy is proven, with a crash-safe ledger.

Two kinds of local data are reclaimed:

* `spool`: the encrypted staged copy of a Bronze file, once its object is completed in
  the queue and a fresh read-back verification passes.
* `log_segment`: the event rows of a sealed log segment, once the segment's export
  receipt exists and every object it produced is completed and freshly verified. The
  segment's seal, digest and the producer watermarks stay, so dedup still works.

Each reclamation is intent -> delete -> receipt. The intent (with its proof) is written
before anything is deleted; a crash at any point is finished by the next run.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .canonical import Clock, iso_utc, parse_iso, system_clock
from .crypto import Keyring
from .db import Migration, connect, immutable_triggers, migrate, transaction
from .export import ExportLedger, ExportReceipt
from .faults import Faults
from .fsutil import fsync_dir
from .ingest.eventlog import EventLog, Segment
from .queue import QueueItem, UploadQueue
from .stores import ObjectStore
from .sync import Spool, Verification, verify_object

MIGRATIONS = (
    Migration(
        "001_tier_ledger",
        """
        CREATE TABLE intents (
            kind TEXT NOT NULL CHECK (kind IN ('spool', 'log_segment')),
            target TEXT NOT NULL,
            proof_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (kind, target)
        );
        CREATE TABLE reclamations (
            kind TEXT NOT NULL,
            target TEXT NOT NULL,
            bytes INTEGER NOT NULL CHECK (bytes >= 0),
            reclaimed_at TEXT NOT NULL,
            PRIMARY KEY (kind, target),
            FOREIGN KEY (kind, target) REFERENCES intents (kind, target)
        );
        """
        + immutable_triggers("intents")
        + immutable_triggers("reclamations"),
    ),
)


@dataclass(frozen=True)
class RetentionPolicy:
    hot_seconds: float = 0.0
    reclaim_spool: bool = True
    reclaim_log: bool = True


@dataclass
class TierSummary:
    spool_reclaimed: int = 0
    spool_bytes: int = 0
    segments_reclaimed: int = 0
    log_payload_bytes: int = 0
    orphans_removed: int = 0
    waiting: list[tuple[str, str]] = field(default_factory=list)
    refused: list[Verification] = field(default_factory=list)


class TierLedger:
    def __init__(self, path: Path, *, clock: Clock = system_clock) -> None:
        self.path = path
        self._clock = clock
        with closing(connect(path)) as db:
            migrate(db, MIGRATIONS)

    def intent(self, kind: str, target: str, proof: dict[str, Any]) -> dict[str, Any]:
        """Record the intent once; on a retry return the original proof unchanged."""
        with closing(connect(self.path)) as db, transaction(db):
            db.execute(
                "INSERT OR IGNORE INTO intents VALUES (?,?,?,?)",
                (kind, target, json.dumps(proof, sort_keys=True), iso_utc(self._clock())),
            )
            row = db.execute(
                "SELECT proof_json FROM intents WHERE kind=? AND target=?", (kind, target)
            ).fetchone()
        stored: dict[str, Any] = json.loads(row[0])
        return stored

    def record(self, kind: str, target: str, size: int) -> None:
        with closing(connect(self.path)) as db, transaction(db):
            db.execute(
                "INSERT OR IGNORE INTO reclamations VALUES (?,?,?,?)",
                (kind, target, size, iso_utc(self._clock())),
            )

    def done(self) -> set[tuple[str, str]]:
        with closing(connect(self.path)) as db:
            rows = db.execute("SELECT kind, target FROM reclamations").fetchall()
        return {(row[0], row[1]) for row in rows}

    def timeline(self) -> list[dict[str, Any]]:
        with closing(connect(self.path)) as db:
            rows = db.execute(
                "SELECT i.kind, i.target, i.created_at AS intent_at, r.reclaimed_at, r.bytes"
                " FROM intents AS i LEFT JOIN reclamations AS r USING (kind, target)"
                " ORDER BY i.created_at"
            ).fetchall()
        return [dict(row) for row in rows]


class Tiering:
    def __init__(
        self,
        *,
        ledger: TierLedger,
        queue: UploadQueue,
        spool: Spool,
        log: EventLog,
        exports: ExportLedger,
        store: ObjectStore,
        keyring: Keyring,
        policy: RetentionPolicy,
        faults: Faults | None = None,
        clock: Clock = system_clock,
    ) -> None:
        self.ledger = ledger
        self.queue = queue
        self.spool = spool
        self.log = log
        self.exports = exports
        self.store = store
        self.keyring = keyring
        self.policy = policy
        self.faults = faults or Faults()
        self._clock = clock
        self._fresh: dict[str, Verification] = {}

    def run(self) -> TierSummary:
        summary = TierSummary()
        self._fresh = {}
        done = self.ledger.done()
        items = {item.idempotency_key: item for item in self.queue.items()}
        if self.policy.reclaim_spool:
            for item in items.values():
                if item.status == "completed" and ("spool", item.spool_path) not in done:
                    self._reclaim_spool(item, summary)
        if self.policy.reclaim_log:
            receipts = {receipt.first_seq: receipt for receipt in self.exports.receipts()}
            reclaimed = self.log.reclaimed_segment_ids()
            for segment in self.log.segments():
                if segment.segment_id not in reclaimed:
                    self._reclaim_segment(segment, receipts.get(segment.first_seq), items, summary)
        summary.orphans_removed = self._remove_orphan_spool(items.values())
        return summary

    def _proven(self, item: QueueItem, summary: TierSummary) -> bool:
        """Completed, catalogued, past the hot window, and verified again right now."""
        receipt = self.queue.receipt(item.idempotency_key)
        if item.status != "completed" or receipt is None:
            summary.waiting.append((item.object_key, "upload not completed"))
            return False
        if self._clock() < parse_iso(receipt.verified_at).timestamp() + self.policy.hot_seconds:
            summary.waiting.append((item.object_key, "inside hot window"))
            return False
        verification = self._fresh.get(item.idempotency_key)
        if verification is None:
            verification = verify_object(self.store, self.keyring, item)
            self._fresh[item.idempotency_key] = verification
            if not verification.ok:
                summary.refused.append(verification)
        return verification.ok

    def _reclaim(
        self, kind: str, target: str, proof: dict[str, Any], delete: Callable[[dict[str, Any]], int]
    ) -> int:
        recorded = self.ledger.intent(kind, target, proof)
        self.faults.checkpoint("tier.after_intent")
        size = delete(recorded)
        self.faults.checkpoint("tier.after_delete")
        self.ledger.record(kind, target, size)
        self.faults.checkpoint("tier.after_receipt")
        return size

    def _reclaim_spool(self, item: QueueItem, summary: TierSummary) -> None:
        if not self._proven(item, summary):
            return
        path = self.spool.path(item.spool_path)
        proof = {
            "object_key": item.object_key,
            "ciphertext_sha256": item.ciphertext_sha256,
            "bytes": item.encrypted_bytes,
            "verified_at": iso_utc(self._clock()),
        }

        def delete(recorded: dict[str, Any]) -> int:
            if path.exists():
                path.unlink()
                fsync_dir(path.parent)
            return int(recorded["bytes"])

        summary.spool_bytes += self._reclaim("spool", item.spool_path, proof, delete)
        summary.spool_reclaimed += 1

    def _reclaim_segment(
        self,
        segment: Segment,
        receipt: ExportReceipt | None,
        items: dict[str, QueueItem],
        summary: TierSummary,
    ) -> None:
        if receipt is None or (receipt.high_water, receipt.batch_sha256) != (
            segment.last_seq,
            segment.sha256,
        ):
            summary.waiting.append((segment.name, "not exported yet"))
            return
        objects = [items[key] for key in receipt.queue_keys if key in items]
        if len(objects) != len(receipt.queue_keys):
            summary.waiting.append((segment.name, "export objects missing from queue"))
            return
        if not all(self._proven(item, summary) for item in objects):
            return
        proof = {
            "export_receipt_id": receipt.receipt_id,
            "segment_sha256": segment.sha256,
            "objects": [
                {"object_key": item.object_key, "ciphertext_sha256": item.ciphertext_sha256}
                for item in objects
            ],
            "verified_at": iso_utc(self._clock()),
        }

        def delete(recorded: dict[str, Any]) -> int:
            return self.log.reclaim_segment(segment.segment_id, recorded).payload_bytes

        summary.log_payload_bytes += self._reclaim("log_segment", segment.name, proof, delete)
        summary.segments_reclaimed += 1

    def _remove_orphan_spool(self, items: Iterable[QueueItem]) -> int:
        """Delete spool blobs no queue row references: a crash hit between write and enqueue,
        so they were never admitted, never uploaded and never acknowledged."""
        referenced = {item.spool_path for item in items}
        removed = 0
        for path in self.spool.root.rglob("*.enc"):
            if str(path.relative_to(self.spool.root)) not in referenced:
                path.unlink()
                removed += 1
        return removed
