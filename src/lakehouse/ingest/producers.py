"""Synthetic, deterministic multi-producer workload (clearly labeled fake data).

Producers emit e-commerce events and deliver them the way real clients do: in batches,
slightly out of order, some held back and delivered late, some delivered twice. Each
producer keeps an outbox and drops an event only on a durable receipt. Everything is
seeded, so a re-run after a crash re-delivers exactly the same events.
"""

from __future__ import annotations

import random
import string
import time
import uuid
from collections import Counter, deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ..events import NewEvent
from .hub import Ack, IngestHub

DEFAULT_PRODUCERS = ("web-checkout", "ios-app", "android-app", "support-desk")
DEFAULT_START = datetime(2026, 10, 1, tzinfo=UTC)
_NAMESPACE = uuid.UUID("6f1c0d3e-8b7a-4f60-9a51-2f5d0c9e4b11")
_KINDS = (
    ("page_view", 52),
    ("add_to_cart", 18),
    ("checkout_started", 10),
    ("payment_captured", 8),
    ("refund_issued", 1),
    ("support_message", 10),
)
_SKUS = tuple(f"sku-{n:03d}" for n in range(1, 41))
_SUPPORT_TEMPLATES = (
    "Where is my order? Reach me at {email}",
    "I was charged twice on card {card}",
    "The app keeps logging me out",
    "Debug info for support: api_key={token}",
    "Please cancel my subscription",
)


@dataclass(frozen=True)
class Workload:
    producers: tuple[str, ...] = DEFAULT_PRODUCERS
    events_per_producer: int = 3_000
    days: int = 3
    seed: int = 7
    start: datetime = DEFAULT_START
    batch_size: int = 250
    duplicate_rate: float = 0.02
    late_rate: float = 0.01
    swap_rate: float = 0.05

    @property
    def total_events(self) -> int:
        return len(self.producers) * self.events_per_producer


def generate(workload: Workload, producer_id: str) -> list[NewEvent]:
    rng = random.Random(f"{workload.seed}:{producer_id}:events")
    step = timedelta(days=workload.days) / workload.events_per_producer
    skew = timedelta(milliseconds=rng.randint(-400, 400))
    kinds = [kind for kind, _ in _KINDS]
    weights = [weight for _, weight in _KINDS]
    events = []
    for seq in range(1, workload.events_per_producer + 1):
        jitter = timedelta(milliseconds=rng.randint(-1500, 1500))
        # A small offset keeps clock skew and jitter from spilling into the previous day.
        occurred = workload.start + timedelta(seconds=5) + step * (seq - 1) + skew + jitter
        kind = rng.choices(kinds, weights)[0]
        events.append(
            NewEvent(
                producer_id=producer_id,
                producer_seq=seq,
                event_id=str(uuid.uuid5(_NAMESPACE, f"{producer_id}:{seq}")),
                kind=kind,
                occurred_at=occurred.isoformat(),
                payload=_payload(rng, kind),
            )
        )
    return events


def _payload(rng: random.Random, kind: str) -> dict[str, Any]:
    payload: dict[str, Any] = {"user_id": f"user-{rng.randrange(500):04d}"}
    sku = rng.choice(_SKUS)
    if kind in ("page_view", "add_to_cart"):
        payload["sku"] = sku
    elif kind in ("checkout_started", "payment_captured", "refund_issued"):
        payload |= {"sku": sku, "amount_cents": rng.randrange(500, 25_000), "currency": "USD"}
    elif kind == "support_message":
        token = "sk-" + "test-" + "".join(rng.choices(string.ascii_letters + string.digits, k=28))
        payload["text"] = rng.choice(_SUPPORT_TEMPLATES).format(
            email=f"customer{rng.randrange(1000)}@example.com",
            card="4242 4242 4242 4242",
            token=token,
        )
    return payload


@dataclass
class DeliveryStats:
    produced: int = 0
    sent: int = 0
    redelivered: int = 0
    held_late: int = 0
    acks: Counter[str] = field(default_factory=Counter)

    def as_dict(self) -> dict[str, Any]:
        return {
            "produced": self.produced,
            "sent": self.sent,
            "redelivered": self.redelivered,
            "held_late": self.held_late,
            "acks": dict(self.acks),
        }


class _Producer:
    def __init__(self, events: list[NewEvent], workload: Workload, stats: DeliveryStats) -> None:
        self.rng = random.Random(f"{workload.seed}:{events[0].producer_id}:delivery")
        self.workload = workload
        self.stats = stats
        self.queue: deque[NewEvent] = deque(events)
        self.held: list[NewEvent] = []
        self.retry: list[NewEvent] = []
        self.unacked = {event.producer_seq: event for event in events}
        self.acked: list[NewEvent] = []

    @property
    def done(self) -> bool:
        return not self.unacked

    def next_batch(self, *, release_held: bool) -> list[NewEvent]:
        batch, self.retry = self.retry, []
        while self.queue and len(batch) < self.workload.batch_size:
            event = self.queue.popleft()
            if self.rng.random() < self.workload.late_rate:
                self.held.append(event)
                self.stats.held_late += 1
            else:
                batch.append(event)
        if release_held and not self.queue:
            batch.extend(self.held)
            self.held = []
        for index in range(len(batch) - 1):
            if self.rng.random() < self.workload.swap_rate:
                batch[index], batch[index + 1] = batch[index + 1], batch[index]
        redeliveries = sum(self.rng.random() < self.workload.duplicate_rate for _ in batch)
        if self.acked and redeliveries:
            # At-least-once delivery: re-send events that were already acknowledged.
            redelivery = self.rng.sample(self.acked, k=min(len(self.acked), redeliveries))
            batch.extend(redelivery)
            self.stats.redelivered += len(redelivery)
        return batch

    def on_receipts(self, batch: Sequence[NewEvent], acks: Sequence[Ack]) -> None:
        for event, ack in zip(batch, acks, strict=True):
            self.stats.acks[ack.value] += 1
            if event.producer_seq not in self.unacked:
                continue
            if ack.durable or ack is Ack.CONFLICT:
                del self.unacked[event.producer_seq]
                self.acked.append(event)
            elif event.producer_seq not in {queued.producer_seq for queued in self.retry}:
                self.retry.append(event)


def deliver(
    hub: IngestHub, workload: Workload, *, ack_latencies: list[float] | None = None
) -> DeliveryStats:
    """Run every producer to completion: each event ends with a durable receipt.

    `ack_latencies`, if given, collects the seconds each batch waited for its receipts.
    """
    stats = DeliveryStats()
    producers = []
    for producer_id in workload.producers:
        events = generate(workload, producer_id)
        stats.produced += len(events)
        producers.append(_Producer(events, workload, stats))
    rounds_without_queue = 0
    while not all(producer.done for producer in producers):
        release_held = all(not producer.queue for producer in producers)
        for producer in producers:
            batch = producer.next_batch(release_held=release_held)
            if not batch:
                continue
            started = time.perf_counter()
            receipts = hub.submit(batch)
            if ack_latencies is not None:
                ack_latencies.append(time.perf_counter() - started)
            stats.sent += len(batch)
            producer.on_receipts(batch, [receipt.ack for receipt in receipts])
        if release_held:
            rounds_without_queue += 1
            if rounds_without_queue > 1:
                # Nothing new to send: drain the reorder buffer so pending events commit.
                hub.drain()
    hub.flush()
    return stats
