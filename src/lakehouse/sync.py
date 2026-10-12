"""Staging (encrypt + enqueue) and uploading (upload, read back, verify, record).

An object is only ever called durable after it has been read back from the store and
checked four ways: byte length and SHA-256 equal the staged ciphertext, the AES-GCM tag
authenticates, the decrypted bytes hash to the Bronze file's SHA-256, and the header is
bound to this exact object key (so two swapped objects are caught).
"""

from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from .bronze import CONTENT_TYPE, BronzeFile, BronzeWriter
from .canonical import Clock, canonical_json, iso_utc, sha256_hex, system_clock
from .catalog import Catalog
from .crypto import Keyring, open_sealed, seal
from .errors import IntegrityError, LakehouseError, VerificationError
from .faults import Faults
from .fsutil import secure_dir, write_once
from .keys import MissingKeyError
from .queue import Lease, NewItem, QueueItem, UploadQueue, UploadReceipt
from .stores import ObjectStore

CHECKS = ("bytes_match", "gcm_tag", "plaintext_match", "bound_to_key")


def idempotency_key(file: BronzeFile) -> str:
    return sha256_hex(
        canonical_json({"kind": "bronze", "path": file.relative_path, "sha256": file.sha256})
    )


class Spool:
    """Encrypted copies waiting for upload, content-addressed by ciphertext SHA-256."""

    def __init__(self, root: Path) -> None:
        self.root = secure_dir(root)

    def put(self, blob: bytes) -> str:
        digest = sha256_hex(blob)
        relative = f"{digest[:2]}/{digest}.enc"
        path = self.root / relative
        if not write_once(path, blob) and sha256_hex(path.read_bytes()) != digest:
            raise IntegrityError(f"spool blob {relative} is corrupt")
        return relative

    def read(self, item: QueueItem) -> bytes:
        blob = (self.root / item.spool_path).read_bytes()
        if sha256_hex(blob) != item.ciphertext_sha256:
            raise IntegrityError(f"spool blob for {item.object_key} is corrupt")
        return blob

    def path(self, relative: str) -> Path:
        return self.root / relative


class Stager:
    def __init__(
        self, *, writer: BronzeWriter, spool: Spool, queue: UploadQueue, keyring: Keyring
    ) -> None:
        self.writer = writer
        self.spool = spool
        self.queue = queue
        self.keyring = keyring

    def stage(self, file: BronzeFile) -> str:
        key = idempotency_key(file)
        if self.queue.get(key) is not None:
            return key  # already admitted; never re-encrypt (the nonce would change)
        object_key = f"{file.relative_path}.enc"
        plaintext = self.writer.read_verified(file).read_bytes()
        sealed = seal(
            plaintext,
            keyring=self.keyring,
            content_type=CONTENT_TYPE,
            context={"object_key": object_key, "batch_id": file.batch_id},
        )
        self.queue.enqueue(
            NewItem(
                idempotency_key=key,
                object_key=object_key,
                source_path=file.relative_path,
                spool_path=self.spool.put(sealed.blob),
                plaintext_sha256=sealed.plaintext_sha256,
                ciphertext_sha256=sealed.ciphertext_sha256,
                key_id=sealed.key_id,
                plaintext_bytes=sealed.plaintext_bytes,
                encrypted_bytes=len(sealed.blob),
            )
        )
        return key


@dataclass(frozen=True)
class Verification:
    object_key: str
    ok: bool
    checks: dict[str, bool | None]
    error: str | None = None


def verify_object(store: ObjectStore, keyring: Keyring, item: QueueItem) -> Verification:
    """Read the object back and run every check. A check that cannot run (nothing to
    compare once decryption fails) is recorded as None rather than as a pass or fail."""
    checks: dict[str, bool | None] = dict.fromkeys(CHECKS)
    errors: list[str] = []
    try:
        blob = store.get(item.object_key, max_bytes=item.encrypted_bytes)
    except VerificationError as error:
        return Verification(item.object_key, False, checks, str(error))
    checks["bytes_match"] = (
        len(blob) == item.encrypted_bytes and sha256_hex(blob) == item.ciphertext_sha256
    )
    if not checks["bytes_match"]:
        errors.append("remote bytes differ from the staged ciphertext")
    try:
        plaintext, header = open_sealed(blob, keyring=keyring)
    except VerificationError as error:
        checks["gcm_tag"] = False
        errors.append(str(error))
    except MissingKeyError as error:
        errors.append(str(error))  # cannot check without the key: neither pass nor fail
    else:
        checks["gcm_tag"] = True
        checks["plaintext_match"] = sha256_hex(plaintext) == item.plaintext_sha256
        checks["bound_to_key"] = header.get("context", {}).get("object_key") == item.object_key
        if not checks["plaintext_match"]:
            errors.append("plaintext differs from the exported Bronze file")
        if not checks["bound_to_key"]:
            errors.append("object was encrypted for a different key")
    ok = all(value is True for value in checks.values())
    return Verification(item.object_key, ok, checks, None if ok else errors[0])


@dataclass
class SyncSummary:
    uploaded: int = 0
    already_present: int = 0
    resumed: int = 0
    failures: list[tuple[str, str]] = field(default_factory=list)
    failed_keys: list[tuple[str, str]] = field(default_factory=list)
    verifications: list[Verification] = field(default_factory=list)


class Uploader:
    def __init__(
        self,
        *,
        queue: UploadQueue,
        spool: Spool,
        store: ObjectStore,
        keyring: Keyring,
        catalog: Catalog,
        instance_id: str | None = None,
        retry_base_seconds: float = 1.0,
        retry_max_seconds: float = 300.0,
        faults: Faults | None = None,
        clock: Clock = system_clock,
    ) -> None:
        self.queue = queue
        self.spool = spool
        self.store = store
        self.keyring = keyring
        self.catalog = catalog
        self.instance_id = instance_id or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds
        self.faults = faults or Faults()
        self._clock = clock
        self._summary_lock = threading.Lock()

    def owner(self) -> str:
        return f"{self.instance_id}:{threading.current_thread().name}"

    def sync_once(self, summary: SyncSummary) -> bool:
        """Process one claimable item. Returns False when nothing is claimable.

        An item that failed earlier in this pass is skipped, so a permanently failing
        object (say, a corrupted remote copy) cannot spin a worker in place.
        """
        with self._summary_lock:
            failed = [key for key, _ in summary.failed_keys]
        lease = self.queue.claim(self.owner(), exclude=failed)
        if lease is None:
            return False
        self.faults.checkpoint("sync.after_claim")
        try:
            self._process(lease, summary)
        except LakehouseError as error:
            delay = min(
                self.retry_max_seconds, self.retry_base_seconds * 2 ** (lease.item.attempts - 1)
            )
            self.queue.release(lease, error=f"{type(error).__name__}: {error}", retry_after=delay)
            with self._summary_lock:
                summary.failed_keys.append((lease.item.idempotency_key, lease.item.object_key))
                summary.failures.append((lease.item.object_key, str(error)))
        return True

    def _process(self, lease: Lease, summary: SyncSummary) -> None:
        item = lease.item
        receipt = self.queue.receipt(item.idempotency_key)
        if receipt is None:
            put = self.store.put_if_absent(
                item.object_key,
                self.spool.read(item),
                metadata={"ciphertext-sha256": item.ciphertext_sha256, "key-id": item.key_id},
            )
            self.faults.checkpoint("sync.after_upload")
            verification = verify_object(self.store, self.keyring, item)
            with self._summary_lock:
                summary.verifications.append(verification)
            if not verification.ok:
                raise VerificationError(f"{item.object_key}: {verification.error}")
            receipt = self._receipt(item, etag=put.stored.etag, created=put.created)
            self.queue.record_receipt(lease, receipt)
            self.faults.checkpoint("sync.after_receipt")
            with self._summary_lock:
                if put.created:
                    summary.uploaded += 1
                else:
                    summary.already_present += 1
        else:
            with self._summary_lock:
                summary.resumed += 1
        self.catalog.record(receipt)
        self.faults.checkpoint("sync.after_catalog")
        self.queue.complete(lease)
        self.faults.checkpoint("sync.after_complete")

    def _receipt(self, item: QueueItem, *, etag: str | None, created: bool) -> UploadReceipt:
        identity = (
            item.idempotency_key,
            item.ciphertext_sha256,
            self.store.provider,
            self.store.bucket,
        )
        return UploadReceipt(
            receipt_id=f"upl-{sha256_hex(chr(0).join(identity).encode())[:16]}",
            idempotency_key=item.idempotency_key,
            object_key=item.object_key,
            provider=self.store.provider,
            bucket=self.store.bucket,
            etag=etag,
            ciphertext_sha256=item.ciphertext_sha256,
            plaintext_sha256=item.plaintext_sha256,
            key_id=item.key_id,
            encrypted_bytes=item.encrypted_bytes,
            created_object=created,
            verified_at=iso_utc(self._clock()),
        )

    def sync_all(self, *, workers: int = 4) -> SyncSummary:
        summary = SyncSummary()

        def drain() -> None:
            while self.sync_once(summary):
                pass

        if workers <= 1:
            drain()
            return summary
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="upload") as pool:
            for future in [pool.submit(drain) for _ in range(workers)]:
                future.result()
        return summary
