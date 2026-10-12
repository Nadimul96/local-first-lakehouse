"""Canonical encodings and clocks shared by every stage.

Identities in this project are hashes of canonical JSON, so two processes that build the
same record always derive the same id. That is what makes every step safely retryable.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

Clock = Callable[[], float]
system_clock: Clock = time.time


def canonical_json(value: Mapping[str, Any] | list[Any]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_utc(epoch_seconds: float) -> str:
    """Microsecond ISO-8601 in UTC with a Z suffix; sortable as text."""
    return datetime.fromtimestamp(epoch_seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp has no timezone: {value!r}")
    return parsed.astimezone(UTC)


def normalize_iso(value: str) -> str:
    """Normalize any offset-aware ISO timestamp to the canonical UTC form."""
    return parse_iso(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
