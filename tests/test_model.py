from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import replace

import duckdb
import pytest

from lakehouse.bronze import BronzeWriter
from lakehouse.errors import IntegrityError
from lakehouse.ingest.producers import generate
from lakehouse.model import Warehouse
from lakehouse.pipeline import Pipeline
from lakehouse.source import SourceBatch
from tests.conftest import SMALL


def modeled(pipeline: Pipeline) -> Pipeline:
    pipeline.ingest(SMALL)
    pipeline.export()
    pipeline.model()
    return pipeline


def test_gold_matches_an_independent_python_computation(pipeline: Pipeline) -> None:
    modeled(pipeline)
    expected: dict[str, Counter[str]] = defaultdict(Counter)
    for producer in SMALL.producers:
        for e in generate(SMALL, producer):
            day = e.occurred_at[:10]
            expected[day]["events"] += 1
            if e.kind == "payment_captured":
                expected[day]["gross"] += int(e.payload["amount_cents"])
            if e.kind == "refund_issued":
                expected[day]["refunded"] += int(e.payload["amount_cents"])
    gold = {str(r["event_date"]): r for r in pipeline.warehouse.query("gold_daily")}
    assert set(gold) == set(expected)
    for day, counts in expected.items():
        assert gold[day]["events"] == counts["events"]
        assert gold[day]["gross_cents"] == counts["gross"]
        assert gold[day]["net_cents"] == counts["gross"] - counts["refunded"]


def test_silver_is_typed_and_partitioned(pipeline: Pipeline) -> None:
    modeled(pipeline)
    pattern = pipeline.warehouse.silver_glob()
    with duckdb.connect() as con:
        types = dict(
            con.execute(
                "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM"
                " read_parquet(?, hive_partitioning=true))",
                [pattern],
            ).fetchall()
        )
    assert types["occurred_at"] == "TIMESTAMP"
    assert types["amount_cents"] == "BIGINT"
    assert types["event_date"] == "DATE"


def test_silver_removes_duplicates_that_reach_bronze(pipeline: Pipeline) -> None:
    modeled(pipeline)
    receipts = pipeline.exports.receipts()
    files = [f for r in receipts for f in r.files]
    # Write a second Bronze file that repeats the first segment's events under a new name.
    batch = pipeline.exporter.source.next_batch(0)
    assert batch is not None
    duplicate = SourceBatch("seg-dup", batch.first_seq, batch.last_seq, batch.sha256, batch.events)
    extra = BronzeWriter(pipeline.lake.root).write(duplicate)
    manifest, built = pipeline.warehouse.build(files + extra)
    assert built and manifest is not None
    assert manifest.duplicates_removed == batch.last_seq - batch.first_seq + 1
    assert manifest.silver_rows == SMALL.total_events


def test_unchanged_inputs_are_a_no_op_and_versions_swap_atomically(pipeline: Pipeline) -> None:
    modeled(pipeline)
    files = [f for r in pipeline.exports.receipts() for f in r.files]
    first = pipeline.warehouse.current()
    manifest, built = pipeline.warehouse.build(files)
    assert not built and manifest == first
    smaller, rebuilt = pipeline.warehouse.build(files[:-1])
    assert rebuilt and smaller is not None and smaller.version != first.version  # type: ignore[union-attr]
    versions = [p.name for p in pipeline.warehouse.versions.iterdir()]
    assert versions == [smaller.version]  # old version garbage-collected
    pointer = json.loads((pipeline.warehouse.root / "CURRENT.json").read_text())
    assert pointer == {"version": smaller.version}


def test_model_refuses_bronze_that_changed_after_export(pipeline: Pipeline) -> None:
    modeled(pipeline)
    files = [f for r in pipeline.exports.receipts() for f in r.files]
    tampered = replace(files[0], sha256="0" * 64)
    with pytest.raises(IntegrityError, match="changed since export"):
        Warehouse(pipeline.warehouse.root, lake=pipeline.lake).build([tampered, *files[1:]])
