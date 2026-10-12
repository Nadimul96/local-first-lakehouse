"""Running the pipeline again on the same input changes nothing durable.

Only observability counters move (the second run's redeliveries are counted as rejected
duplicates). Events, segments, Bronze bytes, queue rows, receipts, remote objects,
catalog rows, Silver/Gold files and tiering records are byte-for-byte identical.
"""

from __future__ import annotations

from pathlib import Path

from lakehouse.config import Settings
from lakehouse.pipeline import Pipeline
from tests.convergence import WORKLOAD, assert_converged, fingerprint, settings_for


def test_whole_pipeline_twice_leaves_identical_state(tmp_path: Path) -> None:
    settings = settings_for(tmp_path / "home")
    with Pipeline(settings) as pipeline:
        pipeline.run_all(WORKLOAD)
        duplicates_before = pipeline.log.stats()["duplicates"]
    first = fingerprint(settings)
    with Pipeline(settings) as pipeline:
        pipeline.run_all(WORKLOAD)
        assert pipeline.log.stats()["duplicates"] > duplicates_before
        assert_converged(pipeline)
    assert fingerprint(settings) == first


def _hot_steps(settings: Settings) -> None:
    with Pipeline(settings) as pipeline:
        pipeline.ingest(WORKLOAD)
        pipeline.export()
        pipeline.sync()
        pipeline.model()


def test_each_step_is_idempotent_before_tiering(tmp_path: Path) -> None:
    """Same property while data is still hot (log rows and spool blobs present)."""
    settings = settings_for(tmp_path / "home")
    _hot_steps(settings)
    hot = fingerprint(settings)
    assert hot["events"] and hot["spool"]
    _hot_steps(settings)
    assert fingerprint(settings) == hot
