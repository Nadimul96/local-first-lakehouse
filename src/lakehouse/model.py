"""Silver and Gold with DuckDB, built as immutable versions behind a `CURRENT` pointer.

* Silver: one row per event (deduplicated by `event_id`, first copy by log order wins),
  typed (UTC timestamps, integer amounts) and partitioned by `event_date`.
* Gold: daily business aggregates over Silver.

A version is named by the hash of its exact Bronze inputs. Building writes into a hidden
staging directory, renames it into place, then atomically flips `CURRENT.json`. Readers
see the old version or the new one, never half of one, and re-running with unchanged
inputs is a no-op.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb

from .bronze import BronzeFile, BronzeWriter
from .canonical import Clock, canonical_json, iso_utc, sha256_hex, system_clock
from .faults import Faults
from .fsutil import atomic_replace, fsync_dir, secure_dir

SILVER_SQL = """
CREATE TABLE silver AS
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY seq) AS copy_number
    FROM bronze
)
SELECT
    seq,
    event_id,
    producer_id,
    producer_seq,
    kind,
    CAST(occurred_at AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS occurred_at,
    CAST(event_date AS DATE) AS event_date,
    json_extract_string(payload_json, '$.user_id') AS user_id,
    json_extract_string(payload_json, '$.sku') AS sku,
    TRY_CAST(json_extract(payload_json, '$.amount_cents') AS BIGINT) AS amount_cents,
    json_extract_string(payload_json, '$.currency') AS currency,
    json_extract_string(payload_json, '$.text') AS support_text
FROM ranked
WHERE copy_number = 1
ORDER BY seq
"""

GOLD_DAILY_SQL = """
CREATE TABLE gold_daily AS
SELECT
    event_date,
    COUNT(*) AS events,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(*) FILTER (WHERE kind = 'page_view') AS page_views,
    COUNT(*) FILTER (WHERE kind = 'checkout_started') AS checkouts,
    COUNT(*) FILTER (WHERE kind = 'payment_captured') AS payments,
    ROUND(payments / NULLIF(checkouts, 0), 4) AS checkout_conversion,
    CAST(COALESCE(SUM(amount_cents) FILTER (WHERE kind = 'payment_captured'), 0) AS BIGINT)
        AS gross_cents,
    CAST(COALESCE(SUM(amount_cents) FILTER (WHERE kind = 'refund_issued'), 0) AS BIGINT)
        AS refunded_cents,
    gross_cents - refunded_cents AS net_cents
FROM silver
GROUP BY event_date
ORDER BY event_date
"""

GOLD_KIND_SQL = """
CREATE TABLE gold_by_kind AS
SELECT event_date, kind, COUNT(*) AS events, COUNT(DISTINCT user_id) AS users
FROM silver
GROUP BY event_date, kind
ORDER BY event_date, kind
"""


@dataclass(frozen=True)
class Manifest:
    version: str
    input_sha256: str
    bronze_files: int
    bronze_rows: int
    silver_rows: int
    duplicates_removed: int
    gold_days: int
    built_at: str


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class Warehouse:
    def __init__(
        self,
        root: Path,
        *,
        lake: BronzeWriter,
        faults: Faults | None = None,
        clock: Clock = system_clock,
    ) -> None:
        self.root = secure_dir(root)
        self.versions = secure_dir(root / "versions")
        self.lake = lake
        self.faults = faults or Faults()
        self._clock = clock
        self._pointer = root / "CURRENT.json"

    def current(self) -> Manifest | None:
        if not self._pointer.exists():
            return None
        version = json.loads(self._pointer.read_text())["version"]
        manifest = self.versions / version / "manifest.json"
        return Manifest(**json.loads(manifest.read_text()))

    def build(self, files: Sequence[BronzeFile]) -> tuple[Manifest | None, bool]:
        """Build Silver and Gold for exactly these Bronze files. Returns (manifest, built)."""
        if not files:
            return None, False
        inputs = sorted((f.relative_path, f.sha256) for f in files)
        input_sha256 = sha256_hex(canonical_json([list(pair) for pair in inputs]))
        version = input_sha256[:16]
        current = self.current()
        if current is not None and current.input_sha256 == input_sha256:
            return current, False
        final = self.versions / version
        if not (final / "manifest.json").exists():
            self._build_version(files, version, input_sha256)
        atomic_replace(self._pointer, json.dumps({"version": version}).encode())
        self._collect_garbage(keep=version)
        return self.current(), True

    def _build_version(self, files: Sequence[BronzeFile], version: str, input_sha256: str) -> None:
        staging = self.versions / f".{version}.building"
        shutil.rmtree(staging, ignore_errors=True)
        secure_dir(staging / "gold")
        paths = ", ".join(_quote(str(self.lake.read_verified(f))) for f in files)
        with duckdb.connect() as con:
            con.execute("SET TimeZone='UTC'")
            con.execute(
                f"CREATE TABLE bronze AS SELECT * FROM read_parquet([{paths}],"
                " hive_partitioning=true)"
            )
            con.execute(SILVER_SQL)
            con.execute(
                f"COPY silver TO {_quote(str(staging / 'silver'))}"
                " (FORMAT PARQUET, PARTITION_BY (event_date), COMPRESSION ZSTD)"
            )
            self.faults.checkpoint("model.after_silver")
            con.execute(GOLD_DAILY_SQL)
            con.execute(GOLD_KIND_SQL)
            for table in ("gold_daily", "gold_by_kind"):
                con.execute(
                    f"COPY {table} TO {_quote(str(staging / 'gold' / f'{table}.parquet'))}"
                    " (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            self.faults.checkpoint("model.after_gold")
            counts = {
                table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # type: ignore[index]
                for table in ("bronze", "silver", "gold_daily")
            }
        manifest = Manifest(
            version=version,
            input_sha256=input_sha256,
            bronze_files=len(files),
            bronze_rows=counts["bronze"],
            silver_rows=counts["silver"],
            duplicates_removed=counts["bronze"] - counts["silver"],
            gold_days=counts["gold_daily"],
            built_at=iso_utc(self._clock()),
        )
        (staging / "manifest.json").write_text(json.dumps(asdict(manifest), indent=2))
        staging.rename(self.versions / version)
        fsync_dir(self.versions)

    def _collect_garbage(self, *, keep: str) -> None:
        for candidate in self.versions.iterdir():
            if candidate.name != keep:
                shutil.rmtree(candidate, ignore_errors=True)

    def query(self, table: str) -> list[dict[str, Any]]:
        """Rows of a Gold table (`gold_daily` or `gold_by_kind`) from the current version."""
        if table not in ("gold_daily", "gold_by_kind"):
            raise ValueError(f"unknown gold table {table!r}")
        manifest = self.current()
        if manifest is None:
            return []
        path = self.versions / manifest.version / "gold" / f"{table}.parquet"
        with duckdb.connect() as con:
            cursor = con.execute("SELECT * FROM read_parquet(?)", [str(path)])
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def silver_glob(self) -> str | None:
        manifest = self.current()
        if manifest is None:
            return None
        return str(self.versions / manifest.version / "silver" / "**" / "*.parquet")
