"""Opt-in: needs a Postgres with sql/postgres applied. Set LAKEHOUSE_TEST_PG_DSN to the
runtime role's DSN (for example via `docker compose --profile catalog up`)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from lakehouse.catalog import PostgresCatalog
from lakehouse.errors import ConflictError
from tests.test_queue import item, receipt_for

DSN = os.environ.get("LAKEHOUSE_TEST_PG_DSN")
SQL = Path(__file__).resolve().parents[1] / "sql" / "postgres"


def test_role_sql_grants_only_select_and_insert() -> None:
    grants = (SQL / "002_runtime_role.sql").read_text().upper()
    assert "GRANT SELECT, INSERT ON" in grants
    for forbidden in ("GRANT UPDATE", "GRANT DELETE", "GRANT TRUNCATE", "GRANT ALL"):
        assert forbidden not in grants


@pytest.mark.skipif(not DSN, reason="set LAKEHOUSE_TEST_PG_DSN to run against Postgres")
def test_postgres_catalog_is_insert_only() -> None:
    psycopg = pytest.importorskip("psycopg")
    catalog = PostgresCatalog.from_dsn(DSN or "")
    entry = item(int.from_bytes(os.urandom(4), "big"))
    catalog.record(receipt_for(entry))
    catalog.record(receipt_for(entry))  # idempotent
    other = receipt_for(entry)
    with pytest.raises(ConflictError):
        catalog.record(type(other)(**{**other.__dict__, "receipt_id": "upl-other"}))
    with psycopg.connect(DSN) as connection, pytest.raises(psycopg.errors.InsufficientPrivilege):
        connection.execute("DELETE FROM lakehouse.objects")
