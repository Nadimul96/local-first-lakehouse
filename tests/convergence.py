"""Shared invariants for the crash and idempotency suites, plus a subprocess entry point."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Any

import duckdb

from lakehouse.config import Settings
from lakehouse.faults import Faults
from lakehouse.ingest.producers import Workload, generate
from lakehouse.pipeline import Pipeline

WORKLOAD = Workload(events_per_producer=250, batch_size=50)
ENV = {
    "LAKEHOUSE_SEGMENT_ROWS": "200",
    "LAKEHOUSE_RETRY_BASE_SECONDS": "0",
    "LAKEHOUSE_UPLOAD_WORKERS": "2",
}


def settings_for(home: Path) -> Settings:
    return Settings.from_env(home=home, environ=ENV)


def expected_event_ids(workload: Workload = WORKLOAD) -> set[str]:
    return {e.event_id for p in workload.producers for e in generate(workload, p)}


def silver_event_ids(pipeline: Pipeline) -> list[str]:
    pattern = pipeline.warehouse.silver_glob()
    assert pattern is not None, "Silver was never built"
    with duckdb.connect() as con:
        return [
            r[0]
            for r in con.execute(
                "SELECT event_id FROM read_parquet(?, hive_partitioning=true)", [pattern]
            ).fetchall()
        ]


def assert_converged(pipeline: Pipeline, workload: Workload = WORKLOAD) -> None:
    """Every event exactly once in Silver, every object verified, every local copy reclaimed."""
    log = pipeline.log.stats()
    assert log["committed"] == workload.total_events
    assert log["missing_now"] == 0 and log["unsealed_rows"] == 0
    assert pipeline.exports.high_water() == workload.total_events
    assert pipeline.exports.active() is None
    items = pipeline.queue.items()
    files = [f for r in pipeline.exports.receipts() for f in r.files]
    assert len(items) == len(files)
    assert {i.status for i in items} == {"completed"}
    assert all(v.ok for v in pipeline.verify())
    assert pipeline.catalog.count() == len(items)
    silver = silver_event_ids(pipeline)
    assert len(silver) == len(set(silver)), "duplicates in Silver"
    assert set(silver) == expected_event_ids(workload), "events lost or invented"
    gold = pipeline.warehouse.query("gold_daily")
    assert sum(row["events"] for row in gold) == workload.total_events
    assert log["hot_rows"] == 0
    assert log["reclaimed_segments"] == log["segments"]
    assert not list(pipeline.spool.root.rglob("*.enc"))


def _rows(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with closing(sqlite3.connect(path)) as db:
        return [tuple(row) for row in db.execute(sql).fetchall()]


def _tree(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
        and not p.name.startswith(".tmp-")
        and "-wal" not in p.name
        and "-shm" not in p.name
        and not p.name.endswith(".sqlite")
    }


def fingerprint(settings: Settings) -> dict[str, Any]:
    """All durable data, receipts and objects. Excludes observability counters and journal."""
    log, state = settings.log_db, settings.home / "state"
    return {
        "events": _rows(log, "SELECT seq, producer_id, producer_seq, content_sha256 FROM events"),
        "segments": _rows(log, "SELECT * FROM segments"),
        "reclaimed": _rows(log, "SELECT * FROM reclaimed_segments"),
        "watermarks": _rows(log, "SELECT * FROM producer_watermarks"),
        "gaps": _rows(log, "SELECT * FROM producer_gaps"),
        "exports": _rows(state / "export.sqlite", "SELECT * FROM export_receipts"),
        "cursor": _rows(
            state / "export.sqlite", "SELECT high_water, last_receipt_id FROM export_cursor"
        ),
        "queue": _rows(
            state / "queue.sqlite",
            "SELECT idempotency_key, object_key, status, ciphertext_sha256, attempts,"
            " lease_token FROM queue ORDER BY idempotency_key",
        ),
        "queue_history": _rows(state / "queue.sqlite", "SELECT COUNT(*) FROM queue_history"),
        "receipts": _rows(state / "queue.sqlite", "SELECT * FROM upload_receipts"),
        "tiering": _rows(state / "tiering.sqlite", "SELECT * FROM reclamations"),
        "catalog": _rows(settings.remote_dir / "catalog.sqlite", "SELECT * FROM objects"),
        "remote": _tree(settings.remote_dir),
        "lake": _tree(settings.lake_dir),
        "spool": _tree(settings.spool_dir),
        "warehouse": _tree(settings.warehouse_dir),
    }


if __name__ == "__main__":
    # Subprocess entry point for kill-mode crashes: LAKEHOUSE_CRASH_AT picks the point.
    with Pipeline(settings_for(Path(sys.argv[1])), faults=Faults.from_env()) as opened:
        opened.run_all(WORKLOAD)
    print(json.dumps({"completed": True}))
