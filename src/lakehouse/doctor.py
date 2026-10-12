"""`lakehouse doctor`: tells you exactly what is missing for the configured providers."""

from __future__ import annotations

import importlib
import os
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass

from .config import Settings
from .errors import LakehouseError


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def _probe(name: str, run: Callable[[], str]) -> Check:
    try:
        return Check(name, True, run())
    except (LakehouseError, ImportError, OSError, ValueError) as error:
        return Check(name, False, str(error))


def _module(name: str, hint: str) -> str:
    try:
        module = importlib.import_module(name)
    except ImportError as error:
        raise ImportError(hint) from error
    return f"{name} {getattr(module, '__version__', 'installed')}"


def _crypto() -> str:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = AESGCM.generate_key(bit_length=256)
    AESGCM(key).decrypt(b"\0" * 12, AESGCM(key).encrypt(b"\0" * 12, b"probe", None), None)
    return "AES-256-GCM round trip ok"


def _writable(settings: Settings) -> str:
    settings.home.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=settings.home):
        pass
    return str(settings.home)


def _keys(settings: Settings) -> str:
    if settings.key_provider == "env":
        missing = [
            v for v in ("LAKEHOUSE_DATA_KEY_ID", "LAKEHOUSE_DATA_KEY_B64") if not os.environ.get(v)
        ]
        if missing:
            raise ValueError(f"set {', '.join(missing)}")
        return "keys from environment"
    if settings.key_provider == "keychain":
        if sys.platform != "darwin":
            raise ValueError("the keychain provider needs macOS")
        return "macOS Keychain via /usr/bin/security"
    return f"key files under {settings.keys_dir / 'data'} (created on first run, mode 0600)"


def _store(settings: Settings) -> str:
    if settings.store == "filesystem":
        return f"local directory {settings.remote_dir} standing in for S3"
    _module("boto3", "LAKEHOUSE_STORE=s3 needs the s3 extra: uv sync --extra s3")
    if not os.environ.get("LAKEHOUSE_S3_BUCKET"):
        raise ValueError("set LAKEHOUSE_S3_BUCKET (and LAKEHOUSE_S3_ENDPOINT for MinIO/R2)")
    endpoint = os.environ.get("LAKEHOUSE_S3_ENDPOINT", "AWS default endpoint")
    return f"s3://{os.environ['LAKEHOUSE_S3_BUCKET']} via {endpoint}"


def _catalog(settings: Settings) -> str:
    if not settings.catalog_dsn:
        return f"SQLite catalog at {settings.remote_dir / 'catalog.sqlite'}"
    return _module("psycopg", "LAKEHOUSE_CATALOG_DSN needs the postgres extra") + " (Postgres)"


def run_checks(settings: Settings) -> list[Check]:
    return [
        Check("python", sys.version_info >= (3, 12), sys.version.split()[0]),
        _probe("duckdb", lambda: _module("duckdb", "uv sync")),
        _probe("cryptography", _crypto),
        _probe("data root", lambda: _writable(settings)),
        _probe("encryption keys", lambda: _keys(settings)),
        _probe("object store", lambda: _store(settings)),
        _probe("catalog", lambda: _catalog(settings)),
    ]
