from __future__ import annotations

from dataclasses import replace

import pytest

from lakehouse.config import Settings
from lakehouse.faults import Faults, InjectedCrash
from lakehouse.pipeline import Pipeline
from tests.conftest import SMALL, FakeClock


def synced(pipeline: Pipeline) -> Pipeline:
    pipeline.ingest(SMALL)
    pipeline.export()
    pipeline.sync()
    return pipeline


def test_reclaims_spool_and_log_only_after_completed_verified_uploads(pipeline: Pipeline) -> None:
    pipeline.ingest(SMALL)
    pipeline.export()
    nothing = pipeline.tier()["result"]
    assert nothing["spool_reclaimed"] == nothing["segments_reclaimed"] == 0
    assert pipeline.log.stats()["hot_rows"] == SMALL.total_events
    pipeline.sync()
    done = pipeline.tier()["result"]
    assert done["spool_reclaimed"] == len(pipeline.queue.items())
    assert done["segments_reclaimed"] == len(pipeline.log.segments())
    assert pipeline.log.stats()["hot_rows"] == 0
    assert not list(pipeline.spool.root.rglob("*.enc"))


def test_hot_window_delays_reclamation(settings: Settings, clock: FakeClock) -> None:
    with Pipeline(replace(settings, hot_seconds=3600), clock=clock) as pipeline:
        synced(pipeline)
        waiting = pipeline.tier()["result"]
        assert waiting["spool_reclaimed"] == 0 and waiting["waiting"] > 0
        clock.advance(3601)
        assert pipeline.tier()["result"]["spool_reclaimed"] == len(pipeline.queue.items())


def test_unexported_segments_keep_their_rows(pipeline: Pipeline) -> None:
    synced(pipeline)
    pipeline.tier()
    pipeline.ingest(replace(SMALL, events_per_producer=300))  # 200 new events, not exported
    pipeline.tier()
    assert pipeline.log.stats()["hot_rows"] == 200


def test_crash_between_intent_and_delete_is_finished_next_run(settings: Settings) -> None:
    with Pipeline(settings, faults=Faults("tier.after_intent")) as pipeline:
        synced(pipeline)
        with pytest.raises(InjectedCrash):
            pipeline.tier()
        pending = [t for t in pipeline.tier_ledger.timeline() if t["reclaimed_at"] is None]
        assert len(pending) == 1
    with Pipeline(settings) as pipeline:
        pipeline.tier()
        timeline = pipeline.tier_ledger.timeline()
        assert all(t["reclaimed_at"] for t in timeline)
        assert pipeline.log.stats()["hot_rows"] == 0


def test_orphaned_spool_blobs_are_removed(pipeline: Pipeline) -> None:
    synced(pipeline)
    orphan = pipeline.spool.root / "ff" / ("f" * 64 + ".enc")
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"never enqueued")
    assert pipeline.tier()["result"]["orphans_removed"] == 1
    assert not orphan.exists()
