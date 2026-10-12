"""Wires the stages together over one data root and records each step in the journal.

Opening a `Pipeline` takes the single-writer lock, cleans up partial writes from a
crash, and frees queue leases held by dead processes. Every step is safe to re-run.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict
from types import TracebackType
from typing import Any

from .bronze import BronzeWriter
from .canonical import Clock, iso_utc, system_clock
from .catalog import Catalog, PostgresCatalog, SQLiteCatalog
from .config import Settings
from .crypto import Keyring
from .export import Exporter, ExportLedger
from .faults import Faults
from .fsutil import WriterLock, remove_partials
from .ingest.eventlog import EventLog
from .ingest.hub import IngestHub
from .ingest.producers import Workload, deliver
from .ingest.redaction import Redactor
from .journal import Journal
from .keys import EnvKeyring, KeyFileKeyring, MacOSKeychainKeyring, load_redaction_key
from .model import Warehouse
from .queue import UploadQueue
from .source import SQLiteEventSource
from .stores import ObjectStore
from .stores.filesystem import FilesystemObjectStore
from .stores.s3 import S3Config, S3ObjectStore
from .sync import Spool, Stager, Uploader, Verification, verify_object
from .tiering import RetentionPolicy, Tiering, TierLedger


def build_keyring(settings: Settings) -> Keyring:
    if settings.key_provider == "env":
        return EnvKeyring()
    if settings.key_provider == "keychain":
        return MacOSKeychainKeyring(settings.keys_dir / "keychain")
    return KeyFileKeyring(settings.keys_dir / "data")


def build_store(settings: Settings) -> ObjectStore:
    if settings.store == "s3":
        return S3ObjectStore(S3Config.from_env())
    return FilesystemObjectStore(settings.remote_dir)


def build_catalog(settings: Settings) -> Catalog:
    if settings.catalog_dsn:
        return PostgresCatalog.from_dsn(settings.catalog_dsn)
    return SQLiteCatalog(settings.remote_dir / "catalog.sqlite")


class Pipeline:
    def __init__(
        self,
        settings: Settings,
        *,
        faults: Faults | None = None,
        clock: Clock = system_clock,
        store: ObjectStore | None = None,
        keyring: Keyring | None = None,
    ) -> None:
        self.settings = settings
        self.faults = faults or Faults()
        self.clock = clock
        self.lock = WriterLock(settings.home)
        try:
            self._open(store, keyring)
        except BaseException:
            self.lock.release()
            raise

    def _open(self, store: ObjectStore | None, keyring: Keyring | None) -> None:
        s = self.settings
        self.instance_id = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        for directory in (s.lake_dir, s.spool_dir, s.remote_dir):
            remove_partials(directory)
        self.keyring = keyring or build_keyring(s)
        self.store = store or build_store(s)
        self.log = EventLog(s.log_db, clock=self.clock)
        self.hub = IngestHub(
            self.log,
            Redactor(load_redaction_key(s.keys_dir)),
            segment_rows=s.segment_rows,
            reorder_window_ms=s.reorder_window_ms,
            faults=self.faults,
        )
        self.lake = BronzeWriter(s.lake_dir)
        self.exports = ExportLedger(s.export_db, source="event-log", clock=self.clock)
        self.queue = UploadQueue(s.queue_db, max_pending_bytes=s.queue_cap_bytes, clock=self.clock)
        self.spool = Spool(s.spool_dir)
        self.exporter = Exporter(
            source=SQLiteEventSource(s.log_db),
            writer=self.lake,
            ledger=self.exports,
            stager=Stager(
                writer=self.lake, spool=self.spool, queue=self.queue, keyring=self.keyring
            ),
            faults=self.faults,
        )
        self.catalog = build_catalog(s)
        self.uploader = Uploader(
            queue=self.queue,
            spool=self.spool,
            store=self.store,
            keyring=self.keyring,
            catalog=self.catalog,
            instance_id=self.instance_id,
            retry_base_seconds=s.retry_base_seconds,
            faults=self.faults,
            clock=self.clock,
        )
        self.recovered_leases = self.queue.recover_orphans(self.instance_id)
        self.warehouse = Warehouse(
            s.warehouse_dir, lake=self.lake, faults=self.faults, clock=self.clock
        )
        self.tier_ledger = TierLedger(s.tier_db, clock=self.clock)
        self.tiering = Tiering(
            ledger=self.tier_ledger,
            queue=self.queue,
            spool=self.spool,
            log=self.log,
            exports=self.exports,
            store=self.store,
            keyring=self.keyring,
            policy=RetentionPolicy(hot_seconds=s.hot_seconds),
            faults=self.faults,
            clock=self.clock,
        )
        self.journal = Journal(s.journal_path)

    def close(self) -> None:
        self.log.close()
        self.exports.close()
        self.lock.release()

    def __enter__(self) -> Pipeline:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _step(self, name: str, run: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        started = self.clock()
        perf = time.perf_counter()
        result = run()
        entry = {
            "step": name,
            "started_at": iso_utc(started),
            "finished_at": iso_utc(self.clock()),
            "seconds": round(time.perf_counter() - perf, 4),
            "result": result,
        }
        self.journal.append(entry)
        return entry

    # --- steps -----------------------------------------------------------------------

    def ingest(
        self, workload: Workload, *, ack_latencies: list[float] | None = None
    ) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            stats = deliver(self.hub, workload, ack_latencies=ack_latencies)
            log = self.log.stats()
            return {
                **stats.as_dict(),
                "committed_total": log["committed"],
                "duplicates_total": log["duplicates"],
                "conflicts_total": log["conflicts"],
                "late_arrivals_total": log["late_arrivals"],
                "segments_total": log["segments"],
                "redactions": dict(self.hub.redactions),
                "forced_releases": self.hub.forced_releases,
            }

        return self._step("ingest", run)

    def export(self) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            receipts = self.exporter.export_all()
            files = [f for receipt in receipts for f in receipt.files]
            return {
                "segments": len(receipts),
                "files": len(files),
                "rows": sum(f.rows for f in files),
                "bytes": sum(f.bytes for f in files),
                "high_water": self.exports.high_water(),
            }

        return self._step("export", run)

    def sync(self) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            summary = self.uploader.sync_all(workers=self.settings.upload_workers)
            return {
                "uploaded": summary.uploaded,
                "already_present": summary.already_present,
                "resumed": summary.resumed,
                "verified": sum(v.ok for v in summary.verifications),
                "failures": [{"object_key": k, "error": e} for k, e in summary.failures],
            }

        return self._step("sync", run)

    def model(self) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            files = [f for receipt in self.exports.receipts() for f in receipt.files]
            manifest, built = self.warehouse.build(files)
            return {"built": built, "manifest": asdict(manifest) if manifest else None}

        return self._step("model", run)

    def tier(self) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            summary = self.tiering.run()
            return {
                "spool_reclaimed": summary.spool_reclaimed,
                "spool_bytes": summary.spool_bytes,
                "segments_reclaimed": summary.segments_reclaimed,
                "log_payload_bytes": summary.log_payload_bytes,
                "orphans_removed": summary.orphans_removed,
                "waiting": len(summary.waiting),
                "refused": [asdict(v) for v in summary.refused],
            }

        return self._step("tier", run)

    def verify(self) -> list[Verification]:
        """Re-read and re-check every completed object in remote storage."""
        return [
            verify_object(self.store, self.keyring, item)
            for item in self.queue.items()
            if item.status == "completed"
        ]

    def run_all(self, workload: Workload) -> list[dict[str, Any]]:
        return [
            self.ingest(workload),
            self.export(),
            self.sync(),
            self.model(),
            self.tier(),
        ]
