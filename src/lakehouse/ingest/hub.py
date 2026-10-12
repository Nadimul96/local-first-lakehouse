"""Ingest hub: redact, deduplicate, order, commit, then acknowledge.

Every receipt is decided after the commit returns. `committed` and `duplicate` both mean
"this exact event is durable on disk"; a producer may drop it from its outbox. `pending`
means it is still in the reorder buffer and is not durable yet; `conflict` means the
producer reused a sequence number for different content and the event was rejected.
"""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from ..canonical import canonical_json, normalize_iso, parse_iso
from ..events import NewEvent, PreparedEvent, content_sha256
from ..faults import Faults
from .eventlog import Admission, EventLog, Segment
from .redaction import Redactor
from .sequencer import Sequencer


class Ack(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    PENDING = "pending"
    CONFLICT = "conflict"

    @property
    def durable(self) -> bool:
        return self in (Ack.COMMITTED, Ack.DUPLICATE)


@dataclass(frozen=True)
class Receipt:
    producer_id: str
    producer_seq: int
    ack: Ack
    seq: int | None = None


class IngestHub:
    def __init__(
        self,
        log: EventLog,
        redactor: Redactor,
        *,
        segment_rows: int,
        reorder_window_ms: int = 0,
        max_pending: int = 10_000,
        faults: Faults | None = None,
    ) -> None:
        self.log = log
        self.redactor = redactor
        self.segment_rows = segment_rows
        self.faults = faults or Faults()
        self.redactions: Counter[str] = Counter()
        self._sequencer: Sequencer[PreparedEvent] = Sequencer(
            window_ms=reorder_window_ms, max_pending=max_pending
        )
        self._pending: dict[tuple[str, int], str] = {}
        # One lock covers redact -> commit -> receipts, so seq N is durable before any
        # caller can commit seq N+1.
        self._lock = threading.Lock()

    @property
    def pending(self) -> int:
        return self._sequencer.pending

    @property
    def forced_releases(self) -> int:
        return self._sequencer.forced_releases

    def submit(self, events: Sequence[NewEvent]) -> list[Receipt]:
        with self._lock:
            early: dict[int, Ack] = {}
            rejected: Counter[str] = Counter()
            released: list[PreparedEvent] = []
            prepared = [self._prepare(event) for event in events]
            for index, event in enumerate(prepared):
                ack = self._precheck(event)
                if ack is not None:
                    early[index] = ack
                    if ack is not Ack.PENDING:
                        rejected["duplicates" if ack is Ack.DUPLICATE else "conflicts"] += 1
                    continue
                self._pending[event.key] = event.content_sha256
                ts_ms = int(parse_iso(event.occurred_at).timestamp() * 1000)
                released.extend(self._sequencer.add(ts_ms, event))
            committed = self._commit(released, rejected)
            return [
                self._receipt(event, early.get(index), committed)
                for index, event in enumerate(prepared)
            ]

    def drain(self) -> None:
        """Commit everything still in the reorder buffer."""
        with self._lock:
            self._commit(self._sequencer.flush(), Counter())

    def flush(self) -> list[Segment]:
        """Drain the buffer and seal the tail, so every committed event is exportable."""
        with self._lock:
            self._commit(self._sequencer.flush(), Counter())
            sealed = self.log.seal(max_rows=self.segment_rows, include_tail=True)
            if sealed:
                self.faults.checkpoint("ingest.after_seal")
            return sealed

    def _prepare(self, event: NewEvent) -> PreparedEvent:
        payload, counts = self.redactor.redact_value(dict(event.payload))
        self.redactions += counts
        payload_json = canonical_json(payload).decode("utf-8")
        occurred_at = normalize_iso(event.occurred_at)
        return PreparedEvent(
            producer_id=event.producer_id,
            producer_seq=event.producer_seq,
            event_id=event.event_id,
            kind=event.kind,
            occurred_at=occurred_at,
            payload_json=payload_json,
            content_sha256=content_sha256(
                producer_id=event.producer_id,
                producer_seq=event.producer_seq,
                event_id=event.event_id,
                kind=event.kind,
                occurred_at=occurred_at,
                payload_json=payload_json,
            ),
        )

    def _precheck(self, event: PreparedEvent) -> Ack | None:
        buffered = self._pending.get(event.key)
        if buffered is not None:
            return Ack.PENDING if buffered == event.content_sha256 else Ack.CONFLICT
        seen = self.log.classify(event)
        if seen is None:
            return None
        return Ack.DUPLICATE if seen is Admission.DUPLICATE else Ack.CONFLICT

    def _commit(
        self, released: list[PreparedEvent], rejected: Counter[str]
    ) -> dict[tuple[str, int], tuple[Ack, int | None]]:
        try:
            results = self.log.commit(released, rejected=rejected)
        finally:
            # On failure nothing was acknowledged; forgetting these lets the producer's
            # retry be buffered again instead of being answered `pending` forever.
            for event in released:
                self._pending.pop(event.key, None)
        if released:
            self.faults.checkpoint("ingest.after_log_commit")
        if self.log.seal(max_rows=self.segment_rows):
            self.faults.checkpoint("ingest.after_seal")
        outcome: dict[tuple[str, int], tuple[Ack, int | None]] = {}
        for result in results:
            ack = {
                Admission.COMMITTED: Ack.COMMITTED,
                Admission.DUPLICATE: Ack.DUPLICATE,
                Admission.CONFLICT: Ack.CONFLICT,
            }[result.admission]
            outcome[result.event.key] = (ack, result.seq)
        return outcome

    def _receipt(
        self,
        event: PreparedEvent,
        early: Ack | None,
        committed: dict[tuple[str, int], tuple[Ack, int | None]],
    ) -> Receipt:
        if early in (Ack.DUPLICATE, Ack.CONFLICT):
            return Receipt(event.producer_id, event.producer_seq, early)
        if event.key in committed:
            ack, seq = committed[event.key]
            return Receipt(event.producer_id, event.producer_seq, ack, seq)
        return Receipt(event.producer_id, event.producer_seq, Ack.PENDING)
