"""SQLite connections, transactions and checksum-pinned migrations.

Every local store is a separate SQLite file opened in WAL mode with `synchronous=FULL`
(and `fullfsync` on macOS), so a committed transaction is on stable storage before
`COMMIT` returns. That is the durability primitive the rest of the pipeline builds on.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from .canonical import iso_utc, system_clock
from .errors import IntegrityError
from .fsutil import FULL_FSYNC, secure_dir


def connect(path: Path, *, auto_vacuum: bool = False) -> sqlite3.Connection:
    """Open a writer connection. Transactions are explicit (`isolation_level=None`)."""
    secure_dir(path.parent)
    fresh = not path.exists()
    connection = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    if fresh and auto_vacuum:
        # Must be set before the first table exists; lets reclamation return pages to the OS.
        connection.execute("PRAGMA auto_vacuum=INCREMENTAL")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    if FULL_FSYNC:  # macOS: have SQLite use F_FULLFSYNC too (see fsutil)
        connection.execute("PRAGMA fullfsync=ON")
        connection.execute("PRAGMA checkpoint_fullfsync=ON")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    path.chmod(0o600)
    return connection


def connect_readonly(path: Path) -> sqlite3.Connection:
    """Open a connection that cannot write, enforced twice: by the URI and by the pragma."""
    if not path.is_file():
        raise FileNotFoundError(path)
    uri = f"file:{quote(str(path))}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """`BEGIN IMMEDIATE` takes the write lock up front, so check-then-write is atomic."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")


@dataclass(frozen=True)
class Migration:
    migration_id: str
    sql: str

    @property
    def checksum(self) -> str:
        normalized = "\n".join(line.rstrip() for line in self.sql.strip().splitlines())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _statements(sql: str) -> Iterator[str]:
    # executescript() would COMMIT implicitly, letting a schema land without its ledger row.
    pending = ""
    for line in sql.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            pending = ""
            if statement:
                yield statement
    if pending.strip():
        raise ValueError("migration ends with an incomplete statement")


def migrate(connection: sqlite3.Connection, migrations: Sequence[Migration]) -> list[str]:
    """Apply pending migrations, each atomically with its ledger row. Returns applied ids.

    A recorded migration whose SQL has since changed fails closed: two databases with the
    same migration id must have the same schema.
    """
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " migration_id TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )
    applied: list[str] = []
    for migration in migrations:
        with transaction(connection):
            row = connection.execute(
                "SELECT checksum FROM schema_migrations WHERE migration_id=?",
                (migration.migration_id,),
            ).fetchone()
            if row is not None:
                if row["checksum"] != migration.checksum:
                    raise IntegrityError(f"migration {migration.migration_id} checksum changed")
                continue
            for statement in _statements(migration.sql):
                connection.execute(statement)
            connection.execute(
                "INSERT INTO schema_migrations VALUES (?,?,?)",
                (migration.migration_id, migration.checksum, iso_utc(system_clock())),
            )
        applied.append(migration.migration_id)
    return applied


def immutable_triggers(table: str) -> str:
    """SQL that makes a table append-only: the database itself rejects UPDATE and DELETE."""
    return (
        f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;\n"
        f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} "
        f"BEGIN SELECT RAISE(ABORT, '{table} is append-only'); END;\n"
    )
