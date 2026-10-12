"""Object storage behind one small protocol: put-if-absent, get, head.

Stores are deliberately dumb. They never decide whether an object is "right"; the sync
engine does that by reading every object back and checking size, SHA-256 and the GCM tag.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=/-]{0,511}$")


def check_key(key: str) -> str:
    if not SAFE_KEY.fullmatch(key) or ".." in key.split("/"):
        raise ValueError(f"unsafe object key {key!r}")
    return key


@dataclass(frozen=True)
class StoredObject:
    key: str
    size: int
    etag: str | None
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PutResult:
    stored: StoredObject
    created: bool


class ObjectStore(Protocol):
    provider: str
    bucket: str

    def put_if_absent(self, key: str, data: bytes, *, metadata: Mapping[str, str]) -> PutResult:
        """Write unless the key exists; never overwrite. `created` says which happened."""
        ...

    def get(self, key: str, *, max_bytes: int) -> bytes:
        """The full object; refuses to read more than `max_bytes`."""
        ...

    def head(self, key: str) -> StoredObject | None: ...
