from __future__ import annotations

from lakehouse.bench import format_table, run_benchmark


def test_benchmark_reports_every_metric() -> None:
    result = run_benchmark(events=800, producers=4, repeats=1)
    for metric in (
        "ingest_events_per_s",
        "parquet_bytes_per_event",
        "encryption_overhead_pct",
        "aes_gcm_mb_per_s",
        "end_to_end_seconds",
        "ack_latency_p99_ms",
    ):
        assert result[metric] > 0
    assert "events/s" in format_table(result)
