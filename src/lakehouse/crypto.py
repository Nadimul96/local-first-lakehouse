"""Client-side AES-256-GCM envelope for every object that leaves the machine.

Object layout::

    MAGIC (8 bytes) | header length (u32, big endian) | header (canonical JSON) | ciphertext+tag

The canonical header is passed to GCM as associated data. It is not secret, but any
change to it (key id, content type, the plaintext hash, the context) fails the tag check
exactly like a change to the ciphertext. The magic carries a format version so a future
layout can be read side by side with this one.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .canonical import canonical_json, sha256_hex
from .errors import VerificationError

MAGIC = b"LKHENC\x00\x01"
SCHEMA = "lakehouse.encrypted-object.v1"
ALGORITHM = "AES-256-GCM"
NONCE_BYTES = 12
KEY_BYTES = 32
MAX_HEADER_BYTES = 64 * 1024
_PREFIX = len(MAGIC) + 4


@dataclass(frozen=True)
class DataKey:
    key_id: str
    material: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if not self.key_id or len(self.key_id) > 64:
            raise ValueError("key_id must be 1-64 characters")
        if len(self.material) != KEY_BYTES:
            raise ValueError("AES-256 keys are exactly 32 bytes")


class Keyring(Protocol):
    def current(self) -> DataKey:
        """The key new objects are encrypted with."""
        ...

    def get(self, key_id: str) -> DataKey:
        """Any key still held, so objects written before a rotation stay readable."""
        ...


@dataclass(frozen=True)
class SealedObject:
    blob: bytes = field(repr=False)
    key_id: str
    plaintext_sha256: str
    ciphertext_sha256: str
    plaintext_bytes: int

    @property
    def overhead_bytes(self) -> int:
        return len(self.blob) - self.plaintext_bytes


def seal(
    plaintext: bytes,
    *,
    keyring: Keyring,
    content_type: str,
    context: Mapping[str, str],
) -> SealedObject:
    key = keyring.current()
    nonce = os.urandom(NONCE_BYTES)  # 96-bit random nonce; one key never sees 2**32 objects
    plaintext_sha256 = sha256_hex(plaintext)
    header = {
        "schema": SCHEMA,
        "algorithm": ALGORITHM,
        "key_id": key.key_id,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "plaintext_sha256": plaintext_sha256,
        "content_type": content_type,
        "context": dict(sorted(context.items())),
    }
    header_bytes = canonical_json(header)
    ciphertext = AESGCM(key.material).encrypt(nonce, plaintext, header_bytes)
    blob = MAGIC + struct.pack(">I", len(header_bytes)) + header_bytes + ciphertext
    return SealedObject(
        blob=blob,
        key_id=key.key_id,
        plaintext_sha256=plaintext_sha256,
        ciphertext_sha256=sha256_hex(blob),
        plaintext_bytes=len(plaintext),
    )


def read_header(blob: bytes) -> tuple[dict[str, Any], bytes, bytes]:
    """Parse and validate the envelope. Returns (header, header bytes, ciphertext)."""
    if len(blob) < _PREFIX or not blob.startswith(MAGIC):
        raise VerificationError("not a lakehouse encrypted object (bad magic)")
    (header_size,) = struct.unpack(">I", blob[len(MAGIC) : _PREFIX])
    if not 2 <= header_size <= MAX_HEADER_BYTES or _PREFIX + header_size >= len(blob):
        raise VerificationError("encrypted object header length is invalid")
    header_bytes = blob[_PREFIX : _PREFIX + header_size]
    try:
        header = json.loads(header_bytes)
        base64.b64decode(header["nonce"], validate=True)
    except (KeyError, TypeError, ValueError) as error:
        raise VerificationError("encrypted object header is malformed") from error
    if header.get("schema") != SCHEMA or header.get("algorithm") != ALGORITHM:
        raise VerificationError("unsupported encrypted object schema or algorithm")
    return header, header_bytes, blob[_PREFIX + header_size :]


def open_sealed(blob: bytes, *, keyring: Keyring) -> tuple[bytes, dict[str, Any]]:
    """Authenticate and decrypt. Any flipped bit anywhere raises VerificationError."""
    header, header_bytes, ciphertext = read_header(blob)
    key = keyring.get(str(header["key_id"]))
    nonce = base64.b64decode(header["nonce"])
    try:
        plaintext = AESGCM(key.material).decrypt(nonce, ciphertext, header_bytes)
    except InvalidTag as error:
        raise VerificationError("AES-GCM authentication failed") from error
    if sha256_hex(plaintext) != header["plaintext_sha256"]:
        raise VerificationError("decrypted plaintext does not match its recorded hash")
    return plaintext, header
