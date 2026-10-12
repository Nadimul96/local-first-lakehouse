"""A JSON-ready picture of a data root: the input to `status` and to the HTML report."""

from __future__ import annotations

import platform
from dataclasses import asdict
from typing import Any

import duckdb

from .canonical import iso_utc
from .fsutil import tree_bytes
from .pipeline import Pipeline


def _silver_counts(pipeline: Pipeline) -> dict[str, int]:
    pattern = pipeline.warehouse.silver_glob()
    if pattern is None:
        return {"rows": 0, "distinct_event_ids": 0}
    with duckdb.connect() as con:
        rows, distinct = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT event_id)"
            " FROM read_parquet(?, hive_partitioning=true)",
            [pattern],
        ).fetchone() or (0, 0)
    return {"rows": int(rows), "distinct_event_ids": int(distinct)}


def _accounting(log: dict[str, Any], exported: int, silver: dict[str, int]) -> dict[str, Any]:
    committed = int(log["committed"])
    return {
        "committed": committed,
        "exported": exported,
        "silver_rows": silver["rows"],
        "lost": committed - silver["distinct_event_ids"],
        "duplicated": silver["rows"] - silver["distinct_event_ids"],
        "complete": committed == exported == silver["rows"] == silver["distinct_event_ids"],
    }


def _segment_lanes(pipeline: Pipeline, verified_at: dict[str, str]) -> list[dict[str, Any]]:
    """Per sealed segment: when it was sealed, exported, verified remotely and reclaimed."""
    receipts = {receipt.first_seq: receipt for receipt in pipeline.exports.receipts()}
    reclaimed = {
        row["target"]: row["reclaimed_at"]
        for row in pipeline.tier_ledger.timeline()
        if row["kind"] == "log_segment"
    }
    lanes = []
    for segment in pipeline.log.segments():
        receipt = receipts.get(segment.first_seq)
        keys = receipt.queue_keys if receipt else ()
        proofs = [verified_at[key] for key in keys if key in verified_at]
        lanes.append(
            {
                "name": segment.name,
                "first_seq": segment.first_seq,
                "last_seq": segment.last_seq,
                "rows": segment.row_count,
                "objects": len(keys),
                "sealed_at": segment.sealed_at,
                "exported_at": receipt.durable_at if receipt else None,
                "verified_at": max(proofs) if keys and len(proofs) == len(keys) else None,
                "reclaimed_at": reclaimed.get(segment.name),
            }
        )
    return lanes


def take(pipeline: Pipeline, *, verify: bool = False) -> dict[str, Any]:
    s = pipeline.settings
    log = pipeline.log.stats()
    receipts = pipeline.exports.receipts()
    files = [f for receipt in receipts for f in receipt.files]
    manifest = pipeline.warehouse.current()
    silver = _silver_counts(pipeline)
    items = pipeline.queue.items()
    verified_at = {
        item.idempotency_key: receipt.verified_at
        for item in items
        if (receipt := pipeline.queue.receipt(item.idempotency_key)) is not None
    }
    snapshot: dict[str, Any] = {
        "generated_at": iso_utc(pipeline.clock()),
        "machine": f"{platform.system()} {platform.machine()}, Python {platform.python_version()}",
        "settings": {
            "store": s.store,
            "key_provider": s.key_provider,
            "segment_rows": s.segment_rows,
            "reorder_window_ms": s.reorder_window_ms,
            "hot_seconds": s.hot_seconds,
        },
        "log": log,
        "export": {
            "high_water": pipeline.exports.high_water(),
            "segments": len(receipts),
            "files": len(files),
            "rows": sum(f.rows for f in files),
            "bytes": sum(f.bytes for f in files),
            "active_reservation": pipeline.exports.active() is not None,
        },
        "queue": {
            **pipeline.queue.stats(),
            "items": [
                {
                    "idempotency_key": item.idempotency_key,
                    "object_key": item.object_key,
                    "verified_at": verified_at.get(item.idempotency_key),
                    "status": item.status,
                    "attempts": item.attempts,
                    "plaintext_bytes": item.plaintext_bytes,
                    "encrypted_bytes": item.encrypted_bytes,
                    "key_id": item.key_id,
                    "last_error": item.last_error,
                }
                for item in items
            ],
            "history": pipeline.queue.history(),
        },
        "catalog": {"objects": pipeline.catalog.count()},
        "warehouse": {
            "manifest": asdict(manifest) if manifest else None,
            "silver": silver,
            "gold_daily": pipeline.warehouse.query("gold_daily"),
            "gold_by_kind": pipeline.warehouse.query("gold_by_kind"),
        },
        "segments": _segment_lanes(pipeline, verified_at),
        "tiering": {"timeline": pipeline.tier_ledger.timeline()},
        "disk": {
            "log": tree_bytes(s.log_db.parent),
            "spool": tree_bytes(s.spool_dir),
            "bronze": tree_bytes(s.lake_dir),
            "warehouse": tree_bytes(s.warehouse_dir),
            "state": tree_bytes(s.home / "state"),
            "remote": tree_bytes(s.remote_dir) if s.store == "filesystem" else None,
        },
        "accounting": _accounting(log, sum(f.rows for f in files), silver),
        "journal": pipeline.journal.entries(),
    }
    if verify:
        snapshot["verification"] = [asdict(v) for v in pipeline.verify()]
    return snapshot
