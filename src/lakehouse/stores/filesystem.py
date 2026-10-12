"""A local directory that behaves like an S3 bucket. The zero-setup default.

Objects live at `<root>/<key>`, user metadata in a sidecar under `<root>/.meta/`. Writes
are no-clobber (`os.link` onto the final name), so a second upload of the same key
reports `created=False` exactly like S3 answering `If-None-Match: *` with 412.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from ..canonical import sha256_hex
from ..errors import VerificationError
from ..fsutil import atomic_replace, secure_dir, write_once
from . import PutResult, StoredObject, check_key


class FilesystemObjectStore:
    provider = "filesystem"

    def __init__(self, root: Path, *, bucket: str = "lake") -> None:
        self.bucket = bucket
        self.root = secure_dir(root / bucket)
        self._meta = secure_dir(root / bucket / ".meta")

    def _paths(self, key: str) -> tuple[Path, Path]:
        check_key(key)
        return self.root / key, self._meta / f"{key}.json"

    def put_if_absent(self, key: str, data: bytes, *, metadata: Mapping[str, str]) -> PutResult:
        target, meta_path = self._paths(key)
        if not target.exists():
            # Metadata first: for an immutable key it is deterministic, so replacing it is
            # harmless, and the object becomes visible only once both are durable.
            atomic_replace(meta_path, json.dumps(dict(metadata), sort_keys=True).encode())
            if write_once(target, data):
                return PutResult(self._stored(key, target, meta_path), created=True)
        stored = self.head(key)
        if stored is None:
            raise VerificationError(f"object {key} disappeared after a refused write")
        return PutResult(stored, created=False)

    def get(self, key: str, *, max_bytes: int) -> bytes:
        target, _ = self._paths(key)
        if not target.exists():
            raise VerificationError(f"object {key} does not exist")
        if target.stat().st_size > max_bytes:
            raise VerificationError(f"object {key} is larger than expected")
        return target.read_bytes()

    def head(self, key: str) -> StoredObject | None:
        target, meta_path = self._paths(key)
        if not target.exists():
            return None
        return self._stored(key, target, meta_path)

    def _stored(self, key: str, target: Path, meta_path: Path) -> StoredObject:
        metadata = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        etag = sha256_hex(target.read_bytes())[:32]
        return StoredObject(key=key, size=target.stat().st_size, etag=etag, metadata=metadata)

    def object_path(self, key: str) -> Path:
        """Where the object lives on disk (used by the tamper drill and tests)."""
        return self._paths(key)[0]
