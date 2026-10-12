"""Settings from environment variables, and the on-disk layout of a data root."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from .errors import ConfigError

StoreKind = Literal["filesystem", "s3"]
KeyProviderKind = Literal["file", "env", "keychain"]


def _int(env: Mapping[str, str], name: str, default: int) -> int:
    try:
        return int(env.get(name, default))
    except ValueError as error:
        raise ConfigError(f"{name} must be an integer") from error


def _float(env: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(env.get(name, default))
    except ValueError as error:
        raise ConfigError(f"{name} must be a number") from error


@dataclass(frozen=True)
class Settings:
    home: Path
    store: StoreKind = "filesystem"
    key_provider: KeyProviderKind = "file"
    catalog_dsn: str | None = None
    segment_rows: int = 2_000
    reorder_window_ms: int = 2_000
    queue_cap_bytes: int = 512 * 1024 * 1024
    upload_workers: int = 4
    retry_base_seconds: float = 1.0
    hot_seconds: float = 0.0

    @classmethod
    def from_env(
        cls, home: Path | None = None, environ: Mapping[str, str] | None = None
    ) -> Settings:
        env = os.environ if environ is None else environ
        store = env.get("LAKEHOUSE_STORE", "filesystem")
        if store not in ("filesystem", "s3"):
            raise ConfigError("LAKEHOUSE_STORE must be filesystem or s3")
        provider = env.get("LAKEHOUSE_KEY_PROVIDER", "file")
        if provider not in ("file", "env", "keychain"):
            raise ConfigError("LAKEHOUSE_KEY_PROVIDER must be file, env or keychain")
        settings = cls(
            home=(home or Path(env.get("LAKEHOUSE_HOME", ".lakehouse"))).expanduser().absolute(),
            store=store,  # type: ignore[arg-type]
            key_provider=provider,  # type: ignore[arg-type]
            catalog_dsn=env.get("LAKEHOUSE_CATALOG_DSN") or None,
            segment_rows=_int(env, "LAKEHOUSE_SEGMENT_ROWS", cls.segment_rows),
            reorder_window_ms=_int(env, "LAKEHOUSE_REORDER_WINDOW_MS", cls.reorder_window_ms),
            queue_cap_bytes=_int(env, "LAKEHOUSE_QUEUE_CAP_BYTES", cls.queue_cap_bytes),
            upload_workers=_int(env, "LAKEHOUSE_UPLOAD_WORKERS", cls.upload_workers),
            retry_base_seconds=_float(env, "LAKEHOUSE_RETRY_BASE_SECONDS", cls.retry_base_seconds),
            hot_seconds=_float(env, "LAKEHOUSE_HOT_SECONDS", cls.hot_seconds),
        )
        if settings.segment_rows < 1 or settings.upload_workers < 1:
            raise ConfigError("LAKEHOUSE_SEGMENT_ROWS and LAKEHOUSE_UPLOAD_WORKERS must be >= 1")
        return settings

    def with_home(self, home: Path) -> Settings:
        return replace(self, home=home.absolute())

    # --- layout ----------------------------------------------------------------------

    @property
    def log_db(self) -> Path:
        return self.home / "log" / "events.sqlite"

    @property
    def export_db(self) -> Path:
        return self.home / "state" / "export.sqlite"

    @property
    def queue_db(self) -> Path:
        return self.home / "state" / "queue.sqlite"

    @property
    def tier_db(self) -> Path:
        return self.home / "state" / "tiering.sqlite"

    @property
    def spool_dir(self) -> Path:
        return self.home / "spool"

    @property
    def lake_dir(self) -> Path:
        return self.home / "lake"

    @property
    def warehouse_dir(self) -> Path:
        return self.home / "warehouse"

    @property
    def keys_dir(self) -> Path:
        return self.home / "keys"

    @property
    def remote_dir(self) -> Path:
        """Stand-in for remote storage when LAKEHOUSE_STORE=filesystem."""
        return self.home / "remote"

    @property
    def journal_path(self) -> Path:
        return self.home / "journal.jsonl"
