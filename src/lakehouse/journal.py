"""Append-only run journal: one JSON line per pipeline step, used by status and report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .fsutil import full_fsync


class Journal:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, entry: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True, default=str) + "\n")
            handle.flush()
            full_fsync(handle.fileno())

    def entries(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        entries = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue  # a torn final line from a crash mid-append is not a step record
        return entries
