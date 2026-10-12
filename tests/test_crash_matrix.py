"""Crash after every durable step, in two ways, then re-run and require convergence.

* raise: `InjectedCrash` unwinds the stack (context managers and `finally` blocks run).
* kill: the pipeline runs in a subprocess that calls `os._exit(137)` at the checkpoint,
  so nothing runs after it, the same as `kill -9`. Unflushed state is simply gone.

Convergence means: no event lost, none duplicated in Silver, every object verified,
the export cursor at the end of the log, and every local copy reclaimed.
"""

from __future__ import annotations

import contextlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.faults import CHECKPOINTS, KILL_EXIT_CODE, Faults, InjectedCrash
from lakehouse.pipeline import Pipeline
from tests.convergence import ENV, WORKLOAD, assert_converged, settings_for

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("checkpoint", CHECKPOINTS)
def test_raise_then_rerun_converges(tmp_path: Path, checkpoint: str) -> None:
    settings = settings_for(tmp_path / "home")
    with Pipeline(settings, faults=Faults(checkpoint, mode="raise")) as pipeline:
        with pytest.raises(InjectedCrash):
            pipeline.run_all(WORKLOAD)
    with Pipeline(settings) as pipeline:
        pipeline.run_all(WORKLOAD)
        assert_converged(pipeline)


@pytest.mark.slow
@pytest.mark.parametrize("checkpoint", CHECKPOINTS)
def test_kill_then_rerun_converges(tmp_path: Path, checkpoint: str) -> None:
    home = tmp_path / "home"
    env = {**os.environ, **ENV, "LAKEHOUSE_CRASH_AT": checkpoint, "LAKEHOUSE_CRASH_MODE": "kill"}
    env["PYTHONPATH"] = str(ROOT)
    killed = subprocess.run(
        [sys.executable, "-m", "tests.convergence", str(home)],
        env=env,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert killed.returncode == KILL_EXIT_CODE, killed.stderr[-2000:]
    with Pipeline(settings_for(home)) as pipeline:
        assert pipeline.recovered_leases >= 0
        pipeline.run_all(WORKLOAD)
        assert_converged(pipeline)


def test_repeated_crashes_at_different_points_still_converge(tmp_path: Path) -> None:
    settings = settings_for(tmp_path / "home")
    for checkpoint, hit in (
        ("export.after_stage", 2),
        ("sync.after_upload", 3),
        ("tier.after_delete", 2),
        ("sync.after_receipt", 1),
    ):
        with Pipeline(settings, faults=Faults(checkpoint, mode="raise", on_hit=hit)) as pipeline:
            with contextlib.suppress(InjectedCrash):
                pipeline.run_all(WORKLOAD)
    with Pipeline(settings) as pipeline:
        pipeline.run_all(WORKLOAD)
        assert_converged(pipeline)


def test_every_checkpoint_is_reachable() -> None:
    """Guards the matrix: a checkpoint the pipeline never hits would test nothing."""
    source = "\n".join(p.read_text() for p in (ROOT / "src" / "lakehouse").rglob("*.py"))
    for checkpoint in CHECKPOINTS:
        assert f'checkpoint("{checkpoint}")' in source, checkpoint
