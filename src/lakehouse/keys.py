"""Key providers. The default needs no setup: a local key directory with 0600 key files.

* `KeyFileKeyring` (default): one file per key, `CURRENT` names the active one. Rotation
  writes a new key and flips `CURRENT`; old keys stay so old objects remain readable.
* `EnvKeyring`: keys come from environment variables (CI, containers, a secrets manager).
* `MacOSKeychainKeyring` (optional): key material lives in the login Keychain and is read
  through the `security` CLI. Only the non-secret key id is kept on disk.
"""

from __future__ import annotations

import base64
import binascii
import os
import secrets
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path

from .crypto import KEY_BYTES, DataKey
from .errors import ConfigError
from .fsutil import atomic_replace, secure_dir, write_once

KEYCHAIN_SERVICE = "local-first-lakehouse"


class MissingKeyError(ConfigError):
    """The object was encrypted under a key this keyring no longer holds."""


def _new_key_id() -> str:
    return f"k{datetime.now(UTC):%Y%m%d}-{secrets.token_hex(3)}"


def _require_private(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ConfigError(f"{path} is readable by other users (mode {mode:o}); run chmod 600")


def _read_secret_file(path: Path, *, size: int) -> bytes:
    _require_private(path)
    material = path.read_bytes()
    if len(material) != size:
        raise ConfigError(f"{path} must contain exactly {size} bytes")
    return material


class KeyFileKeyring:
    def __init__(self, directory: Path) -> None:
        self.directory = secure_dir(directory)
        _require_private_dir(self.directory)
        self._current = self.directory / "CURRENT"
        if not self._current.exists():
            self.rotate()

    def current(self) -> DataKey:
        return self.get(self._current.read_text().strip())

    def get(self, key_id: str) -> DataKey:
        path = self.directory / f"{key_id}.key"
        if not key_id.isprintable() or "/" in key_id or not path.exists():
            raise MissingKeyError(f"data key {key_id!r} is not in {self.directory}")
        return DataKey(key_id, _read_secret_file(path, size=KEY_BYTES))

    def key_ids(self) -> list[str]:
        return sorted(path.stem for path in self.directory.glob("*.key"))

    def rotate(self) -> str:
        """Create a fresh key and make it current. Existing keys are kept for decryption."""
        key_id = _new_key_id()
        write_once(self.directory / f"{key_id}.key", secrets.token_bytes(KEY_BYTES))
        atomic_replace(self._current, key_id.encode("ascii"))
        return key_id


def _require_private_dir(path: Path) -> None:
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise ConfigError(f"{path} is accessible by other users (mode {mode:o}); run chmod 700")


def _decode_key(name: str, encoded: str) -> bytes:
    try:
        material = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as error:
        raise ConfigError(f"{name} is not valid base64") from error
    if len(material) != KEY_BYTES:
        raise ConfigError(f"{name} must decode to {KEY_BYTES} bytes")
    return material


class EnvKeyring:
    """`LAKEHOUSE_DATA_KEY_ID` + `LAKEHOUSE_DATA_KEY_B64`; retired keys for old objects in
    `LAKEHOUSE_DATA_KEYS_RETIRED` as comma-separated `key_id:base64` pairs."""

    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        env = os.environ if environ is None else environ
        key_id = env.get("LAKEHOUSE_DATA_KEY_ID", "")
        encoded = env.get("LAKEHOUSE_DATA_KEY_B64", "")
        if not key_id or not encoded:
            raise ConfigError("set LAKEHOUSE_DATA_KEY_ID and LAKEHOUSE_DATA_KEY_B64")
        self._current = DataKey(key_id, _decode_key("LAKEHOUSE_DATA_KEY_B64", encoded))
        self._keys = {key_id: self._current}
        for pair in filter(None, env.get("LAKEHOUSE_DATA_KEYS_RETIRED", "").split(",")):
            retired_id, _, retired = pair.strip().partition(":")
            self._keys[retired_id] = DataKey(retired_id, _decode_key(retired_id, retired))

    def current(self) -> DataKey:
        return self._current

    def get(self, key_id: str) -> DataKey:
        if key_id not in self._keys:
            raise MissingKeyError(f"data key {key_id!r} is not in the environment")
        return self._keys[key_id]


Runner = Callable[[list[str]], str]


def _run_security(arguments: list[str]) -> str:
    completed = subprocess.run(  # noqa: S603 - fixed binary, arguments are not shell-parsed
        ["/usr/bin/security", *arguments], capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise ConfigError(f"security {arguments[0]} failed: {completed.stderr.strip()[:200]}")
    return completed.stdout


class MacOSKeychainKeyring:
    """Keys are generic-password items (service `local-first-lakehouse`, account = key id),
    stored base64-encoded. `CURRENT` in `directory` names the active key id."""

    def __init__(self, directory: Path, *, runner: Runner | None = None) -> None:
        if runner is None and sys.platform != "darwin":
            raise ConfigError("the keychain key provider only works on macOS")
        self._run = runner or _run_security
        self.directory = secure_dir(directory)
        self._current = self.directory / "CURRENT"
        self._cache: dict[str, DataKey] = {}
        if not self._current.exists():
            self.rotate()

    def current(self) -> DataKey:
        return self.get(self._current.read_text().strip())

    def get(self, key_id: str) -> DataKey:
        if key_id not in self._cache:
            try:
                encoded = self._run(
                    ["find-generic-password", "-s", KEYCHAIN_SERVICE, "-a", key_id, "-w"]
                ).strip()
            except ConfigError as error:
                raise MissingKeyError(f"data key {key_id!r} is not in the keychain") from error
            self._cache[key_id] = DataKey(key_id, _decode_key(key_id, encoded))
        return self._cache[key_id]

    def rotate(self) -> str:
        key_id = _new_key_id()
        encoded = base64.b64encode(secrets.token_bytes(KEY_BYTES)).decode("ascii")
        # -U updates in place if present; -w passes the secret as an argument because the
        # security CLI has no stdin mode for it. It is visible to `ps` for that instant.
        self._run(
            ["add-generic-password", "-U", "-s", KEYCHAIN_SERVICE, "-a", key_id, "-w", encoded]
        )
        atomic_replace(self._current, key_id.encode("ascii"))
        return key_id


def load_redaction_key(directory: Path, environ: Mapping[str, str] | None = None) -> bytes:
    """Per-install HMAC key for redaction tags: env override, else a 0600 file."""
    env = os.environ if environ is None else environ
    if env.get("LAKEHOUSE_REDACTION_KEY_B64"):
        return _decode_key("LAKEHOUSE_REDACTION_KEY_B64", env["LAKEHOUSE_REDACTION_KEY_B64"])
    path = secure_dir(directory) / "redaction.key"
    write_once(path, secrets.token_bytes(KEY_BYTES))
    return _read_secret_file(path, size=KEY_BYTES)
