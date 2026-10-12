"""`lakehouse bench`: throughput and overhead of every stage, measured on this machine.

Each repeat runs the full pipeline on a fresh temporary data root with the default
synthetic workload (2% duplicate deliveries, out-of-order and late events). Reported
values are medians across repeats.
"""

from __future__ import annotations

import contextlib
import os
import platform
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from .config import Settings
from .crypto import seal
from .ingest.producers import DEFAULT_PRODUCERS, Workload
from .keys import KeyFileKeyring
from .pipeline import Pipeline


def _machine() -> str:
    chip = platform.machine()
    if platform.system() == "Darwin":
        with contextlib.suppress(OSError, subprocess.CalledProcessError):
            chip = subprocess.run(
                ["/usr/sbin/sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
    return (
        f"{chip}, {os.cpu_count()} cores, {platform.system()} {platform.release()}, "
        f"Python {platform.python_version()}"
    )


def _aes_throughput(keys_dir: Path) -> float:
    """MB/s sealing a 32 MiB buffer (raw AES-256-GCM speed, excluding small-file overhead)."""
    keyring = KeyFileKeyring(keys_dir)
    payload = os.urandom(32 * 1024 * 1024)
    start = time.perf_counter()
    seal(payload, keyring=keyring, content_type="application/octet-stream", context={})
    return len(payload) / (time.perf_counter() - start) / 1e6


def _one_run(root: Path, workload: Workload) -> dict[str, float]:
    settings = Settings.from_env(home=root, environ={})
    latencies: list[float] = []
    with Pipeline(settings) as pipeline:
        ingest = pipeline.ingest(workload, ack_latencies=latencies)
        steps = {
            entry["step"]: entry
            for entry in (
                ingest,
                pipeline.export(),
                pipeline.sync(),
                pipeline.model(),
                pipeline.tier(),
            )
        }
        items = pipeline.queue.items()
        plaintext = sum(i.plaintext_bytes for i in items)
        encrypted = sum(i.encrypted_bytes for i in items)
        export = steps["export"]["result"]
        manifest = steps["model"]["result"]["manifest"]
        if manifest["silver_rows"] != workload.total_events:
            raise RuntimeError("benchmark run lost or duplicated events")
    latencies.sort()
    return {
        "ingest_events_per_s": workload.total_events / steps["ingest"]["seconds"],
        "ack_latency_p50_ms": latencies[len(latencies) // 2] * 1000,
        "ack_latency_p99_ms": latencies[int(len(latencies) * 0.99)] * 1000,
        "export_events_per_s": export["rows"] / steps["export"]["seconds"],
        "parquet_bytes_per_event": export["bytes"] / export["rows"],
        "encryption_overhead_pct": (encrypted - plaintext) / plaintext * 100,
        "encryption_overhead_bytes_per_object": (encrypted - plaintext) / len(items),
        "sync_objects_per_s": len(items) / steps["sync"]["seconds"],
        "model_seconds": steps["model"]["seconds"],
        "tier_seconds": steps["tier"]["seconds"],
        "end_to_end_seconds": sum(
            steps[name]["seconds"] for name in ("ingest", "export", "sync", "model")
        ),
    }


def run_benchmark(*, events: int, producers: int, repeats: int) -> dict[str, Any]:
    names = (
        DEFAULT_PRODUCERS[:producers]
        if producers <= len(DEFAULT_PRODUCERS)
        else tuple(f"producer-{n:02d}" for n in range(1, producers + 1))
    )
    workload = Workload(producers=tuple(names), events_per_producer=events // len(names))
    runs = []
    with tempfile.TemporaryDirectory(prefix="lakehouse-bench-") as scratch:
        for index in range(repeats):
            runs.append(_one_run(Path(scratch) / f"run-{index}", workload))
        aes = _aes_throughput(Path(scratch) / "aes-keys")
    medians = {key: statistics.median(run[key] for run in runs) for key in runs[0]}
    return {
        "machine": _machine(),
        "date": time.strftime("%Y-%m-%d"),
        "events": workload.total_events,
        "producers": len(names),
        "repeats": repeats,
        "aes_gcm_mb_per_s": aes,
        **medians,
    }


def format_table(result: dict[str, Any]) -> str:
    rows = [
        (
            "Ingest (redact, dedup, order, fsync commit)",
            f"{result['ingest_events_per_s']:,.0f} events/s",
        ),
        (
            "Durable ack latency per 250-event batch",
            f"p50 {result['ack_latency_p50_ms']:.1f} ms, p99 {result['ack_latency_p99_ms']:.1f} ms",
        ),
        ("Export to Bronze Parquet", f"{result['export_events_per_s']:,.0f} events/s"),
        ("Bronze Parquet size", f"{result['parquet_bytes_per_event']:.1f} bytes/event"),
        (
            "Encryption overhead",
            f"{result['encryption_overhead_pct']:.2f}% "
            f"({result['encryption_overhead_bytes_per_object']:.0f} B/object)",
        ),
        ("AES-256-GCM seal throughput", f"{result['aes_gcm_mb_per_s']:,.0f} MB/s"),
        (
            "Upload + read-back verify (local store)",
            f"{result['sync_objects_per_s']:,.0f} objects/s",
        ),
        ("Silver + Gold build", f"{result['model_seconds']:.2f} s"),
        ("End to end, ingest to Gold", f"{result['end_to_end_seconds']:.2f} s"),
    ]
    width = max(len(name) for name, _ in rows)
    lines = [
        f"{result['events']:,} events, {result['producers']} producers, median of "
        f"{result['repeats']} runs",
        f"{result['machine']} ({result['date']})",
        "",
    ]
    lines += [f"  {name:<{width}}  {value}" for name, value in rows]
    return "\n".join(lines)
