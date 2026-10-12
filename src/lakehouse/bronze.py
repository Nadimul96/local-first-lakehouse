"""Immutable Bronze Parquet: the raw events of one sealed segment, partitioned by day.

Files are named after the segment (`part-seg-000042.parquet`), so a retry after a crash
targets the same path. An existing file is never overwritten; it is read back and must
match the expected rows exactly, otherwise the export stops with an IntegrityError.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb

from .canonical import file_sha256
from .errors import IntegrityError
from .events import LoggedEvent
from .fsutil import secure_dir, write_once
from .source import SourceBatch

SCHEMA_VERSION = "lakehouse.bronze.v1"
COLUMNS = (
    ("seq", "BIGINT"),
    ("event_id", "VARCHAR"),
    ("producer_id", "VARCHAR"),
    ("producer_seq", "BIGINT"),
    ("kind", "VARCHAR"),
    ("occurred_at", "VARCHAR"),
    ("payload_json", "VARCHAR"),
    ("content_sha256", "VARCHAR"),
)
CONTENT_TYPE = "application/vnd.apache.parquet"


@dataclass(frozen=True)
class BronzeFile:
    relative_path: str
    sha256: str
    bytes: int
    rows: int
    first_seq: int
    last_seq: int
    event_date: str
    batch_id: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _row(event: LoggedEvent) -> tuple[Any, ...]:
    return (
        event.seq,
        event.event_id,
        event.producer_id,
        event.producer_seq,
        event.kind,
        event.occurred_at,
        event.payload_json,
        event.content_sha256,
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class BronzeWriter:
    def __init__(self, lake_root: Path) -> None:
        self.root = secure_dir(lake_root)
        self._staging = secure_dir(lake_root / ".staging")

    def path(self, relative_path: str) -> Path:
        resolved = (self.root / relative_path).resolve()
        if self.root.resolve() not in resolved.parents:
            raise IntegrityError(f"{relative_path} escapes the lake root")
        return resolved

    def write(self, batch: SourceBatch) -> list[BronzeFile]:
        by_date: dict[str, list[LoggedEvent]] = defaultdict(list)
        for event in batch.events:
            by_date[event.occurred_at[:10]].append(event)  # occurred_at is normalized UTC
        files = []
        for event_date in sorted(by_date):
            rows = [_row(event) for event in by_date[event_date]]
            relative = f"bronze/events/event_date={event_date}/part-{batch.batch_id}.parquet"
            target = self.path(relative)
            if target.exists() or not write_once(target, self._encode(rows, batch)):
                self.verify(target, rows)
            files.append(
                BronzeFile(
                    relative_path=relative,
                    sha256=file_sha256(target),
                    bytes=target.stat().st_size,
                    rows=len(rows),
                    first_seq=rows[0][0],
                    last_seq=rows[-1][0],
                    event_date=event_date,
                    batch_id=batch.batch_id,
                )
            )
        return files

    def _encode(self, rows: list[tuple[Any, ...]], batch: SourceBatch) -> bytes:
        # Rows reach DuckDB as NDJSON: its C++ reader is ~10x faster than binding Python lists.
        staged = self._staging / f"{uuid.uuid4().hex}.jsonl"
        output = self._staging / f"{uuid.uuid4().hex}.parquet"
        names = [name for name, _ in COLUMNS]
        staged.write_text(
            "".join(
                json.dumps(dict(zip(names, row, strict=True)), ensure_ascii=False) + "\n"
                for row in rows
            ),
            encoding="utf-8",
        )
        columns = ", ".join(f"{name}: '{sql_type}'" for name, sql_type in COLUMNS)
        metadata = {
            "schema": SCHEMA_VERSION,
            "batch_id": batch.batch_id,
            "batch_sha256": batch.sha256,
        }
        kv = ", ".join(f"{key}: {_sql_string(value)}" for key, value in metadata.items())
        try:
            with duckdb.connect() as con:
                # One thread keeps row order and therefore file bytes deterministic.
                con.execute("SET threads=1")
                con.execute(
                    f"COPY (SELECT * FROM read_json({_sql_string(str(staged))},"
                    f" format='newline_delimited', columns={{{columns}}}) ORDER BY seq)"
                    f" TO {_sql_string(str(output))}"
                    f" (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA {{{kv}}})"
                )
            return output.read_bytes()
        finally:
            staged.unlink(missing_ok=True)
            output.unlink(missing_ok=True)

    def verify(self, target: Path, expected: list[tuple[Any, ...]]) -> None:
        names = ", ".join(name for name, _ in COLUMNS)
        try:
            with duckdb.connect() as con:
                actual = con.execute(
                    f"SELECT {names} FROM read_parquet(?) ORDER BY seq", [str(target)]
                ).fetchall()
        except duckdb.Error as error:
            raise IntegrityError(f"{target.name} exists but is unreadable") from error
        if actual != expected:
            raise IntegrityError(f"{target.name} exists with different rows")

    def read_verified(self, file: BronzeFile) -> Path:
        target = self.path(file.relative_path)
        if not target.exists() or file_sha256(target) != file.sha256:
            raise IntegrityError(f"{file.relative_path} is missing or changed since export")
        return target
