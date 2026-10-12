"""Crash-safe filesystem primitives.

A rename or link is only durable once the containing directory is fsynced, so every
helper here fsyncs the file and then its directory before returning.
"""

from __future__ import annotations

import fcntl
import os
import uuid
from pathlib import Path
from types import TracebackType

from .errors import WriterBusyError


def secure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


# On macOS plain fsync() leaves data in the drive's write cache; only F_FULLFSYNC asks the
# drive to persist it, at several milliseconds per call. LAKEHOUSE_FSYNC=os opts out: it
# still survives a process crash, not a power cut. The test suite uses it for speed.
# Read via getattr: F_FULLFSYNC exists only on macOS, and type stubs on Linux omit it.
_F_FULLFSYNC: int | None = getattr(fcntl, "F_FULLFSYNC", None)
FULL_FSYNC = os.environ.get("LAKEHOUSE_FSYNC", "full") != "os" and _F_FULLFSYNC is not None


def full_fsync(descriptor: int) -> None:
    """Flush a file descriptor to stable storage."""
    if FULL_FSYNC and _F_FULLFSYNC is not None:
        fcntl.fcntl(descriptor, _F_FULLFSYNC)
    else:
        os.fsync(descriptor)


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        full_fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_temp(directory: Path, data: bytes, *, suffix: str) -> Path:
    temporary = directory / f".tmp-{uuid.uuid4().hex}{suffix}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        full_fsync(handle.fileno())
    return temporary


def atomic_replace(path: Path, data: bytes) -> None:
    """Replace `path` with `data` so readers see either the old or the new bytes."""
    secure_dir(path.parent)
    temporary = _write_temp(path.parent, data, suffix=".partial")
    try:
        temporary.replace(path)
        fsync_dir(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def write_once(path: Path, data: bytes) -> bool:
    """Create `path` with `data` unless it already exists. Returns True if created.

    `os.link` fails if the target exists, which gives no-clobber semantics without a
    check-then-write race. Callers verify an existing file's content themselves.
    """
    secure_dir(path.parent)
    temporary = _write_temp(path.parent, data, suffix=".partial")
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        fsync_dir(path.parent)
        return True
    finally:
        temporary.unlink(missing_ok=True)


def remove_partials(root: Path) -> int:
    """Delete temp files left by a crash mid-write. They were never linked, so never acked."""
    removed = 0
    if not root.exists():
        return 0
    for candidate in root.rglob(".tmp-*.partial"):
        candidate.unlink(missing_ok=True)
        removed += 1
    return removed


def tree_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file() and not p.is_symlink())


class WriterLock:
    """Exclusive, non-blocking OS lock: one pipeline process per data root.

    Holding it also proves that any queue lease owned by a different process instance is
    orphaned (its owner is dead), which is what lets a restart recover leases immediately.
    """

    def __init__(self, root: Path) -> None:
        secure_dir(root)
        self.path = root / ".writer.lock"
        self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self._fd)
            self._fd = -1
            raise WriterBusyError(f"another pipeline process holds {self.path}") from error

    def release(self) -> None:
        if self._fd < 0:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = -1

    def __enter__(self) -> WriterLock:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()
