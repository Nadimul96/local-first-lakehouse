"""Event records and the hashes that identify them across every stage."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .canonical import canonical_json, sha256_hex

_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,63}$")


@dataclass(frozen=True)
class NewEvent:
    """What a producer submits. `producer_seq` is gap-free and monotonic per producer."""

    producer_id: str
    producer_seq: int
    event_id: str
    kind: str
    occurred_at: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.producer_id):
            raise ValueError(f"invalid producer_id {self.producer_id!r}")
        if not _IDENTIFIER.fullmatch(self.kind):
            raise ValueError(f"invalid kind {self.kind!r}")
        if self.producer_seq < 1:
            raise ValueError("producer_seq starts at 1")
        if not self.event_id or len(self.event_id) > 128:
            raise ValueError("event_id must be 1-128 characters")


@dataclass(frozen=True)
class PreparedEvent:
    """A redacted, normalized event with its content hash, ready to commit."""

    producer_id: str
    producer_seq: int
    event_id: str
    kind: str
    occurred_at: str
    payload_json: str
    content_sha256: str

    @property
    def key(self) -> tuple[str, int]:
        return (self.producer_id, self.producer_seq)


@dataclass(frozen=True)
class LoggedEvent:
    """A committed event at its position `seq` in the global log order."""

    seq: int
    producer_id: str
    producer_seq: int
    event_id: str
    kind: str
    occurred_at: str
    payload_json: str
    content_sha256: str


def content_sha256(
    *,
    producer_id: str,
    producer_seq: int,
    event_id: str,
    kind: str,
    occurred_at: str,
    payload_json: str,
) -> str:
    """Hash of everything a producer controls. Equal hashes mean a true duplicate."""
    return sha256_hex(
        canonical_json(
            {
                "event_id": event_id,
                "kind": kind,
                "occurred_at": occurred_at,
                "payload_json": payload_json,
                "producer_id": producer_id,
                "producer_seq": producer_seq,
            }
        )
    )


def verify_content(event: LoggedEvent) -> bool:
    return (
        content_sha256(
            producer_id=event.producer_id,
            producer_seq=event.producer_seq,
            event_id=event.event_id,
            kind=event.kind,
            occurred_at=event.occurred_at,
            payload_json=event.payload_json,
        )
        == event.content_sha256
    )


def segment_digest(rows: Iterable[tuple[int, str]]) -> str:
    """Digest of a sealed range: detects a row added, removed, reordered or altered."""
    digest = hashlib.sha256()
    for seq, row_sha in rows:
        digest.update(f"{seq}\t{row_sha}\n".encode("ascii"))
    return digest.hexdigest()
