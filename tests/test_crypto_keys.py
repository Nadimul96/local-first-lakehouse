from __future__ import annotations

import base64
import json
import os
import struct
from pathlib import Path

import pytest

from lakehouse.crypto import MAGIC, open_sealed, read_header, seal
from lakehouse.errors import ConfigError, VerificationError
from lakehouse.keys import (
    KEYCHAIN_SERVICE,
    EnvKeyring,
    KeyFileKeyring,
    MacOSKeychainKeyring,
    MissingKeyError,
    load_redaction_key,
)


@pytest.fixture
def keyring(tmp_path: Path) -> KeyFileKeyring:
    return KeyFileKeyring(tmp_path / "keys")


def sealed_blob(keyring: KeyFileKeyring, data: bytes = b"parquet bytes" * 100) -> bytes:
    return seal(
        data, keyring=keyring, content_type="application/x-test", context={"object_key": "a/b.enc"}
    ).blob


def test_round_trip_and_header_contents(keyring: KeyFileKeyring) -> None:
    sealed = seal(b"hello", keyring=keyring, content_type="text/plain", context={"k": "v"})
    plaintext, header = open_sealed(sealed.blob, keyring=keyring)
    assert plaintext == b"hello"
    assert header["key_id"] == sealed.key_id == keyring.current().key_id
    assert header["context"] == {"k": "v"}
    assert sealed.blob.startswith(MAGIC)
    assert sealed.overhead_bytes == len(sealed.blob) - 5


def test_same_plaintext_encrypts_differently_each_time(keyring: KeyFileKeyring) -> None:
    assert sealed_blob(keyring) != sealed_blob(keyring)


def test_flipping_any_ciphertext_byte_fails_authentication(keyring: KeyFileKeyring) -> None:
    blob = bytearray(sealed_blob(keyring))
    blob[-20] ^= 0x01
    with pytest.raises(VerificationError, match="authentication failed"):
        open_sealed(bytes(blob), keyring=keyring)


def test_header_is_authenticated_as_associated_data(keyring: KeyFileKeyring) -> None:
    blob = sealed_blob(keyring)
    header, _, ciphertext = read_header(blob)
    header["content_type"] = "application/x-forged"
    forged = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
    tampered = MAGIC + struct.pack(">I", len(forged)) + forged + ciphertext
    with pytest.raises(VerificationError, match="authentication failed"):
        open_sealed(tampered, keyring=keyring)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda b: b"NOTMAGIC" + b[8:],
        lambda b: b[:12],
        lambda b: b[:8] + struct.pack(">I", 10**7) + b[12:],
    ],
)
def test_malformed_envelopes_are_rejected(keyring: KeyFileKeyring, mutate: object) -> None:
    with pytest.raises(VerificationError):
        open_sealed(mutate(sealed_blob(keyring)), keyring=keyring)  # type: ignore[operator]


def test_rotation_keeps_old_objects_readable(keyring: KeyFileKeyring) -> None:
    old = sealed_blob(keyring)
    old_id = keyring.current().key_id
    new_id = keyring.rotate()
    assert new_id != old_id and keyring.current().key_id == new_id
    assert open_sealed(old, keyring=keyring)[1]["key_id"] == old_id
    assert read_header(sealed_blob(keyring))[0]["key_id"] == new_id


def test_object_under_a_discarded_key_cannot_be_opened(tmp_path: Path) -> None:
    blob = sealed_blob(KeyFileKeyring(tmp_path / "one"))
    with pytest.raises(MissingKeyError):
        open_sealed(blob, keyring=KeyFileKeyring(tmp_path / "two"))


def test_key_files_are_private_and_loose_permissions_are_refused(tmp_path: Path) -> None:
    keyring = KeyFileKeyring(tmp_path / "keys")
    key_file = tmp_path / "keys" / f"{keyring.current().key_id}.key"
    assert key_file.stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "keys").stat().st_mode & 0o777 == 0o700
    key_file.chmod(0o644)
    with pytest.raises(ConfigError, match="chmod 600"):
        keyring.current()


def test_env_keyring_with_retired_keys() -> None:
    current, retired = os.urandom(32), os.urandom(32)
    env = {
        "LAKEHOUSE_DATA_KEY_ID": "k2",
        "LAKEHOUSE_DATA_KEY_B64": base64.b64encode(current).decode(),
        "LAKEHOUSE_DATA_KEYS_RETIRED": "k1:" + base64.b64encode(retired).decode(),
    }
    keyring = EnvKeyring(env)
    assert keyring.current().key_id == "k2"
    assert keyring.get("k1").material == retired
    with pytest.raises(MissingKeyError):
        keyring.get("k0")
    with pytest.raises(ConfigError):
        EnvKeyring({"LAKEHOUSE_DATA_KEY_ID": "k", "LAKEHOUSE_DATA_KEY_B64": "bm90IDMyIGJ5dGVz"})


def test_keychain_keyring_uses_the_security_cli(tmp_path: Path) -> None:
    store: dict[str, str] = {}
    calls: list[list[str]] = []

    def fake_security(arguments: list[str]) -> str:
        calls.append(arguments)
        account = arguments[arguments.index("-a") + 1]
        assert arguments[arguments.index("-s") + 1] == KEYCHAIN_SERVICE
        if arguments[0] == "add-generic-password":
            store[account] = arguments[arguments.index("-w") + 1]
            return ""
        if account not in store:
            raise ConfigError("item not found")
        return store[account] + "\n"

    keyring = MacOSKeychainKeyring(tmp_path / "kc", runner=fake_security)
    blob = sealed_blob(keyring)  # type: ignore[arg-type]
    assert open_sealed(blob, keyring=keyring)[0].startswith(b"parquet")
    assert not list((tmp_path / "kc").glob("*.key"))  # no key material on disk
    with pytest.raises(MissingKeyError):
        keyring.get("absent")
    assert calls[0][0] == "add-generic-password"


def test_redaction_key_is_created_once_with_private_mode(tmp_path: Path) -> None:
    first = load_redaction_key(tmp_path / "keys", environ={})
    assert load_redaction_key(tmp_path / "keys", environ={}) == first
    assert (tmp_path / "keys" / "redaction.key").stat().st_mode & 0o777 == 0o600
    override = base64.b64encode(b"o" * 32).decode()
    assert (
        load_redaction_key(tmp_path / "x", {"LAKEHOUSE_REDACTION_KEY_B64": override}) == b"o" * 32
    )
