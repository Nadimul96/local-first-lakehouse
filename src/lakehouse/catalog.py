"""The remote catalog: an insert-only record of every verified object.

Anyone with catalog access can see which objects exist, their sizes and hashes, without
being able to decrypt them. The writer only ever needs SELECT and INSERT:

* `SQLiteCatalog` (default) lives next to the local object store and enforces that with
  triggers.
* `PostgresCatalog` (optional) runs as a role granted exactly SELECT and INSERT; see
  `sql/postgres/`. A compromised pipeline host cannot rewrite or erase history.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, Protocol

from .db import Migration, connect, immutable_triggers, migrate, transaction
from .errors import ConflictError, DependencyMissingError
from .queue import UploadReceipt

MIGRATIONS = (
    Migration(
        "001_object_catalog",
        """
        CREATE TABLE objects (
            object_key TEXT PRIMARY KEY,
            receipt_id TEXT NOT NULL UNIQUE,
            ciphertext_sha256 TEXT NOT NULL,
            plaintext_sha256 TEXT NOT NULL,
            key_id TEXT NOT NULL,
            encrypted_bytes INTEGER NOT NULL,
            receipt_json TEXT NOT NULL,
            verified_at TEXT NOT NULL
        );
        """
        + immutable_triggers("objects"),
    ),
)


class Catalog(Protocol):
    def record(self, receipt: UploadReceipt) -> None:
        """Insert once; a repeat with the same receipt is a no-op, a different one fails."""
        ...

    def count(self) -> int: ...


def _row(receipt: UploadReceipt) -> tuple[Any, ...]:
    return (
        receipt.object_key,
        receipt.receipt_id,
        receipt.ciphertext_sha256,
        receipt.plaintext_sha256,
        receipt.key_id,
        receipt.encrypted_bytes,
        json.dumps(receipt.as_dict(), sort_keys=True),
        receipt.verified_at,
    )


class SQLiteCatalog:
    def __init__(self, path: Path) -> None:
        self.path = path
        with closing(connect(path)) as db:
            migrate(db, MIGRATIONS)

    def record(self, receipt: UploadReceipt) -> None:
        with closing(connect(self.path)) as db, transaction(db):
            db.execute("INSERT OR IGNORE INTO objects VALUES (?,?,?,?,?,?,?,?)", _row(receipt))
            stored = db.execute(
                "SELECT receipt_id FROM objects WHERE object_key=?", (receipt.object_key,)
            ).fetchone()
        if stored[0] != receipt.receipt_id:
            raise ConflictError(f"catalog already holds another object at {receipt.object_key}")

    def count(self) -> int:
        with closing(connect(self.path)) as db:
            return int(db.execute("SELECT COUNT(*) FROM objects").fetchone()[0])


class PostgresCatalog:
    """DB-API adapter; `connect` is any zero-argument callable returning a connection."""

    def __init__(self, connect_fn: Callable[[], Any], *, schema: str = "lakehouse") -> None:
        if not schema.isidentifier():
            raise ValueError("schema must be a plain identifier")
        self._connect = connect_fn
        self.schema = schema

    @classmethod
    def from_dsn(cls, dsn: str, *, schema: str = "lakehouse") -> PostgresCatalog:
        try:
            psycopg = importlib.import_module("psycopg")
        except ImportError as error:
            raise DependencyMissingError(
                "install the postgres extra: uv sync --extra postgres"
            ) from error
        return cls(lambda: psycopg.connect(dsn), schema=schema)

    def record(self, receipt: UploadReceipt) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {self.schema}.objects VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s)"
                    " ON CONFLICT (object_key) DO NOTHING",
                    _row(receipt),
                )
                cursor.execute(
                    f"SELECT receipt_id FROM {self.schema}.objects WHERE object_key=%s",
                    (receipt.object_key,),
                )
                stored = cursor.fetchone()
            if stored is None or stored[0] != receipt.receipt_id:
                raise ConflictError(f"catalog holds another object at {receipt.object_key}")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def count(self) -> int:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {self.schema}.objects")
                return int(cursor.fetchone()[0])
        finally:
            connection.close()
