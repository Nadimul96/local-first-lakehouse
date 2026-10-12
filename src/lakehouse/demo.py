"""`lakehouse demo`: the whole pipeline on synthetic data, including one injected crash.

The run deliberately crashes the uploader right after an object reaches remote storage
but before its receipt is written, then restarts. The restart recovers the orphaned
lease, finds the object already present (the conditional PUT refuses to overwrite it),
verifies it and carries on. Finally a tamper drill flips one byte of a remote object in
memory and shows verification rejecting it.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable, Mapping
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from .canonical import iso_utc
from .config import Settings
from .errors import ConfigError
from .faults import Faults, InjectedCrash
from .ingest.producers import Workload
from .pipeline import Pipeline
from .report import render_report
from .snapshot import take
from .stores import ObjectStore, PutResult, StoredObject
from .sync import verify_object

MARKER = ".lakehouse-demo"
CRASH_POINT = "sync.after_upload"


class FlippedByteStore:
    """Wraps a store and flips one byte of one object on read. Nothing on disk changes."""

    def __init__(self, inner: ObjectStore, key: str) -> None:
        self.inner = inner
        self.key = key
        self.provider = inner.provider
        self.bucket = inner.bucket

    def put_if_absent(self, key: str, data: bytes, *, metadata: Mapping[str, str]) -> PutResult:
        return self.inner.put_if_absent(key, data, metadata=metadata)

    def get(self, key: str, *, max_bytes: int) -> bytes:
        blob = bytearray(self.inner.get(key, max_bytes=max_bytes))
        if key == self.key:
            blob[len(blob) // 2] ^= 0x01
        return bytes(blob)

    def head(self, key: str) -> StoredObject | None:
        return self.inner.head(key)


def prepare_home(home: Path) -> None:
    """Start from scratch, but only ever delete a directory this demo created."""
    if home.exists():
        if not (home / MARKER).exists():
            raise ConfigError(f"{home} exists and was not created by the demo; pick --home")
        shutil.rmtree(home)
    home.mkdir(parents=True, mode=0o700)
    (home / MARKER).write_text("created by `lakehouse demo`; safe to delete\n")


def tamper_drill(pipeline: Pipeline) -> dict[str, Any]:
    completed = [item for item in pipeline.queue.items() if item.status == "completed"]
    if not completed:
        return {"ran": False}
    target = completed[0]
    tampered = verify_object(
        FlippedByteStore(pipeline.store, target.object_key), pipeline.keyring, target
    )
    genuine = verify_object(pipeline.store, pipeline.keyring, target)
    return {"ran": True, "tampered": asdict(tampered), "genuine": asdict(genuine)}


def run_demo(
    settings: Settings,
    workload: Workload,
    *,
    inject_crash: bool = True,
    echo: Callable[[str], None] = print,
) -> tuple[dict[str, Any], Path]:
    prepare_home(settings.home)
    settings = replace(settings, retry_base_seconds=0.0)
    faults = Faults(crash_at=CRASH_POINT, mode="raise") if inject_crash else Faults()
    with Pipeline(settings, faults=faults) as pipeline:
        echo(_line("ingest", pipeline.ingest(workload)))
        echo(_line("export", pipeline.export()))
        try:
            echo(_line("sync", pipeline.sync()))
        except InjectedCrash as crash:
            counts = pipeline.queue.stats()["by_status"]
            pipeline.journal.append(
                {
                    "step": "crash",
                    "started_at": iso_utc(pipeline.clock()),
                    "result": {"checkpoint": str(crash), "queue": counts},
                }
            )
            echo(
                f"  {'crash':<8} injected at {crash} (simulated process death);"
                f" {counts['completed']} of {sum(counts.values())} objects already completed"
            )
    with Pipeline(settings) as pipeline:
        if inject_crash:
            pipeline.journal.append(
                {
                    "step": "restart",
                    "started_at": iso_utc(pipeline.clock()),
                    "result": {"recovered_leases": pipeline.recovered_leases},
                }
            )
            echo(f"  {'restart':<8} {pipeline.recovered_leases} orphaned lease(s) recovered")
            echo(_line("sync", pipeline.sync()))
        for step in (pipeline.model, pipeline.tier):
            echo(_line(step.__name__, step()))
        drill = tamper_drill(pipeline)
        pipeline.journal.append({"step": "tamper_drill", "result": drill})
        snapshot = take(pipeline, verify=True)
    report_path = settings.home / "report.html"
    report_path.write_text(render_report(snapshot), encoding="utf-8")
    if drill["ran"]:
        failed = [name for name, ok in drill["tampered"]["checks"].items() if ok is False]
        echo(f"  {'tamper':<8} 1 flipped byte -> rejected ({', '.join(failed)} failed)")
    accounting = snapshot["accounting"]
    echo("")
    echo(
        f"  {accounting['committed']:,} committed = {accounting['exported']:,} exported = "
        f"{accounting['silver_rows']:,} in Silver. {accounting['lost']} lost, "
        f"{accounting['duplicated']} duplicated."
    )
    return snapshot, report_path


def _line(name: str, entry: dict[str, Any]) -> str:
    r = entry["result"]
    detail = {
        "ingest": lambda: (
            f"{r['committed_total']:,} committed, {r['duplicates_total']:,} duplicates rejected,"
            f" {r['late_arrivals_total']:,} late arrivals"
        ),
        "export": lambda: (
            f"{r['segments']} segments -> {r['files']} Bronze files ({r['bytes'] / 1e6:.2f} MB)"
        ),
        "sync": lambda: (
            f"{r['verified']} verified this run ({r['uploaded']} new, {r['already_present']}"
            f" already present, {r['resumed']} resumed, {len(r['failures'])} failed)"
        ),
        "model": lambda: (
            f"Silver {r['manifest']['silver_rows']:,} rows, Gold {r['manifest']['gold_days']} days"
            if r["manifest"]
            else "nothing to model"
        ),
        "tier": lambda: (
            f"{r['spool_reclaimed']} staged copies + {r['segments_reclaimed']} log segments"
            f" reclaimed ({(r['spool_bytes'] + r['log_payload_bytes']) / 1e6:.2f} MB)"
        ),
    }[name]()
    return f"  {name:<8} {detail:<78} {entry['seconds']:.2f}s"
