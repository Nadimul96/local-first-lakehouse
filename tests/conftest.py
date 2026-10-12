from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from lakehouse.events import NewEvent
from lakehouse.ingest.eventlog import EventLog
from lakehouse.ingest.hub import IngestHub
from lakehouse.ingest.producers import Workload
from lakehouse.ingest.redaction import Redactor

if TYPE_CHECKING:
    from lakehouse.config import Settings
    from lakehouse.pipeline import Pipeline

BASE_TIME = datetime(2026, 10, 1, tzinfo=UTC)
SMALL = Workload(events_per_producer=250, batch_size=50)


class FakeClock:
    def __init__(self, start: float = 1_790_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    from lakehouse.config import Settings

    base = Settings.from_env(home=tmp_path / "home", environ={})
    return replace(base, segment_rows=200, retry_base_seconds=0.0, upload_workers=2)


@pytest.fixture
def pipeline(settings: Settings) -> Iterator[Pipeline]:
    from lakehouse.pipeline import Pipeline

    with Pipeline(settings) as opened:
        yield opened


@pytest.fixture
def log(tmp_path: Path) -> Iterator[EventLog]:
    opened = EventLog(tmp_path / "log.sqlite")
    yield opened
    opened.close()


@pytest.fixture
def redactor() -> Redactor:
    return Redactor(os.urandom(32))


@pytest.fixture
def hub(log: EventLog, redactor: Redactor) -> IngestHub:
    return IngestHub(log, redactor, segment_rows=10, reorder_window_ms=0)


def event(producer: str, seq: int, *, second: int | None = None, **payload: object) -> NewEvent:
    """A small valid event; `second` sets the event time (defaults to `seq`)."""
    second = seq if second is None else second
    return NewEvent(
        producer_id=producer,
        producer_seq=seq,
        event_id=f"{producer}-{seq}",
        kind="page_view",
        occurred_at=(BASE_TIME + timedelta(seconds=second)).isoformat(),
        payload={"user_id": "user-1", **payload},
    )
