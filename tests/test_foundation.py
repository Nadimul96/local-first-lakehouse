"""Building blocks: migrations, crash-safe file writes, the writer lock, fault injection."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from lakehouse.canonical import canonical_json, normalize_iso
from lakehouse.db import Migration, connect, connect_readonly, migrate
from lakehouse.errors import IntegrityError, WriterBusyError
from lakehouse.faults import Faults, InjectedCrash
from lakehouse.fsutil import WriterLock, atomic_replace, remove_partials, write_once


def test_migration_checksum_change_fails_closed(tmp_path: Path) -> None:
    db = connect(tmp_path / "m.sqlite")
    migrate(db, [Migration("001", "CREATE TABLE t (a INTEGER);")])
    assert migrate(db, [Migration("001", "CREATE TABLE t (a INTEGER);")]) == []
    with pytest.raises(IntegrityError, match="checksum"):
        migrate(db, [Migration("001", "CREATE TABLE t (a TEXT);")])


def test_failed_migration_leaves_no_partial_schema(tmp_path: Path) -> None:
    db = connect(tmp_path / "m.sqlite")
    broken = Migration("001", "CREATE TABLE ok (a INTEGER);\nCREATE TABLE ok (a INTEGER);")
    with pytest.raises(sqlite3.OperationalError):
        migrate(db, [broken])
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "ok" not in tables


def test_write_once_never_clobbers(tmp_path: Path) -> None:
    target = tmp_path / "a" / "file.bin"
    assert write_once(target, b"first")
    assert not write_once(target, b"second")
    assert target.read_bytes() == b"first"
    assert target.stat().st_mode & 0o777 == 0o600
    assert not list(target.parent.glob(".tmp-*"))


def test_atomic_replace_and_partial_cleanup(tmp_path: Path) -> None:
    target = tmp_path / "pointer.json"
    atomic_replace(target, b"one")
    atomic_replace(target, b"two")
    assert target.read_bytes() == b"two"
    (tmp_path / ".tmp-deadbeef.partial").write_bytes(b"torn")
    assert remove_partials(tmp_path) == 1


def test_writer_lock_is_exclusive(tmp_path: Path) -> None:
    with WriterLock(tmp_path), pytest.raises(WriterBusyError):
        WriterLock(tmp_path)
    WriterLock(tmp_path).release()  # free again after release


def test_readonly_connection_rejects_writes(tmp_path: Path) -> None:
    db = connect(tmp_path / "x.sqlite")
    db.execute("CREATE TABLE t (a INTEGER)")
    readonly = connect_readonly(tmp_path / "x.sqlite")
    with pytest.raises(sqlite3.OperationalError):
        readonly.execute("INSERT INTO t VALUES (1)")


def test_faults_fire_only_at_the_named_hit() -> None:
    faults = Faults("export.after_commit", mode="raise", on_hit=2)
    faults.checkpoint("export.after_commit")
    faults.checkpoint("sync.after_claim")
    with pytest.raises(InjectedCrash):
        faults.checkpoint("export.after_commit")
    with pytest.raises(ValueError):
        Faults("no.such.point")


def test_canonical_encodings_are_stable() -> None:
    assert canonical_json({"b": 1, "a": "é"}) == '{"a":"é","b":1}'.encode()
    assert normalize_iso("2026-10-01T02:00:00+02:00") == "2026-10-01T00:00:00.000000Z"
    with pytest.raises(ValueError):
        normalize_iso("2026-10-01T00:00:00")
