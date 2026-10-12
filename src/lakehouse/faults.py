"""Named crash points for the crash-injection test matrix.

Production code calls `faults.checkpoint(name)` right after each durable step. Normally
that is a no-op. In tests it either raises `InjectedCrash` (the stack unwinds, `finally`
blocks run) or calls `os._exit` (nothing runs, like `kill -9`).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

CHECKPOINTS: tuple[str, ...] = (
    "ingest.after_log_commit",
    "ingest.after_seal",
    "export.after_reserve",
    "export.after_parquet_write",
    "export.after_stage",
    "export.after_commit",
    "sync.after_claim",
    "sync.after_upload",
    "sync.after_receipt",
    "sync.after_catalog",
    "sync.after_complete",
    "model.after_silver",
    "model.after_gold",
    "tier.after_intent",
    "tier.after_delete",
    "tier.after_receipt",
)

CrashMode = Literal["raise", "kill"]
KILL_EXIT_CODE = 137


class InjectedCrash(BaseException):
    """Derives from BaseException so `except Exception` recovery paths never swallow it."""


@dataclass
class Faults:
    crash_at: str | None = None
    mode: CrashMode = "raise"
    on_hit: int = 1
    hits: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.crash_at is not None and self.crash_at not in CHECKPOINTS:
            raise ValueError(f"unknown checkpoint {self.crash_at!r}")
        if self.on_hit < 1:
            raise ValueError("on_hit must be >= 1")

    @classmethod
    def from_env(cls) -> Faults:
        crash_at = os.environ.get("LAKEHOUSE_CRASH_AT") or None
        mode = os.environ.get("LAKEHOUSE_CRASH_MODE", "kill")
        if mode not in ("raise", "kill"):
            raise ValueError(f"LAKEHOUSE_CRASH_MODE must be raise or kill, got {mode!r}")
        on_hit = int(os.environ.get("LAKEHOUSE_CRASH_ON_HIT", "1"))
        return cls(crash_at=crash_at, mode=mode, on_hit=on_hit)  # type: ignore[arg-type]

    def checkpoint(self, name: str) -> None:
        if name not in CHECKPOINTS:
            raise ValueError(f"unknown checkpoint {name!r}")
        if name != self.crash_at:
            return
        self.hits += 1
        if self.hits != self.on_hit:
            return
        if self.mode == "kill":
            os._exit(KILL_EXIT_CODE)
        raise InjectedCrash(name)
