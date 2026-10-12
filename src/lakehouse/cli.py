"""Command line: `lakehouse ingest|export|sync|model|tier|verify|status|report|demo|bench|...`."""

from __future__ import annotations

import argparse
import functools
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .config import Settings
from .errors import LakehouseError
from .faults import Faults
from .ingest.producers import DEFAULT_PRODUCERS, Workload
from .pipeline import Pipeline


def _settings(args: argparse.Namespace) -> Settings:
    return Settings.from_env(home=Path(args.home) if args.home else None)


def _workload(args: argparse.Namespace) -> Workload:
    producers = (
        tuple(DEFAULT_PRODUCERS[: args.producers])
        if args.producers <= 4
        else tuple(f"producer-{n:02d}" for n in range(1, args.producers + 1))
    )
    return Workload(
        producers=producers, events_per_producer=args.events, seed=args.seed, days=args.days
    )


def _print_step(entry: dict[str, Any]) -> None:
    print(f"{entry['step']} ({entry['seconds']:.2f}s)")
    print(json.dumps(entry["result"], indent=2, default=str))


def _run_step(args: argparse.Namespace, step: Callable[[Pipeline], dict[str, Any]]) -> int:
    with Pipeline(_settings(args), faults=Faults.from_env()) as pipeline:
        _print_step(step(pipeline))
    return 0


def _run_pipeline_step(
    args: argparse.Namespace, *, method: Callable[[Pipeline], dict[str, Any]]
) -> int:
    return _run_step(args, method)


def cmd_ingest(args: argparse.Namespace) -> int:
    return _run_step(args, lambda p: p.ingest(_workload(args)))


def cmd_verify(args: argparse.Namespace) -> int:
    with Pipeline(_settings(args)) as pipeline:
        results = pipeline.verify()
    for result in results:
        mark = "ok  " if result.ok else "FAIL"
        print(f"{mark} {result.object_key}" + (f"  ({result.error})" if result.error else ""))
    print(f"{sum(r.ok for r in results)}/{len(results)} objects verified")
    return 0 if all(r.ok for r in results) else 1


def cmd_status(args: argparse.Namespace) -> int:
    from .snapshot import take

    with Pipeline(_settings(args)) as pipeline:
        snapshot = take(pipeline)
    if args.json:
        print(json.dumps(snapshot, indent=2, default=str))
        return 0
    log, export, queue = snapshot["log"], snapshot["export"], snapshot["queue"]
    accounting = snapshot["accounting"]
    rows = [
        (
            "event log",
            f"{log['committed']:,} committed, {log['hot_rows']:,} hot rows,"
            f" {log['segments']} segments ({log['reclaimed_segments']} reclaimed),"
            f" {log['missing_now']} missing",
        ),
        ("export", f"high-water {export['high_water']:,}, {export['files']} Bronze files"),
        ("queue", ", ".join(f"{count} {status}" for status, count in queue["by_status"].items())),
        ("catalog", f"{snapshot['catalog']['objects']} objects"),
        (
            "warehouse",
            f"{accounting['silver_rows']:,} Silver rows, "
            f"{len(snapshot['warehouse']['gold_daily'])} Gold days",
        ),
        ("accounting", f"{accounting['lost']} lost, {accounting['duplicated']} duplicated"),
    ]
    for name, text in rows:
        print(f"  {name:<11} {text}")
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    from .report import render_report
    from .snapshot import take

    settings = _settings(args)
    with Pipeline(settings) as pipeline:
        snapshot = take(pipeline, verify=True)
    out = Path(args.out) if args.out else settings.home / "report.html"
    out.write_text(render_report(snapshot), encoding="utf-8")
    print(f"wrote {out}")
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .demo import run_demo

    home = Path(args.home) if args.home else Path(".lakehouse-demo")
    settings = Settings.from_env(home=home)
    workload = _workload(args)
    print(
        f"local-first-lakehouse demo: {len(workload.producers)} producers x "
        f"{workload.events_per_producer:,} synthetic events, data root {settings.home}\n"
    )
    _, report = run_demo(settings, workload, inject_crash=not args.no_crash)
    print(f"\n  report: {report}")
    return 0


def cmd_bench(args: argparse.Namespace) -> int:
    from .bench import format_table, run_benchmark

    result = run_benchmark(events=args.events, producers=args.producers, repeats=args.repeats)
    print(json.dumps(result, indent=2) if args.json else format_table(result))
    return 0


def cmd_rotate_key(args: argparse.Namespace) -> int:
    from .keys import KeyFileKeyring

    settings = _settings(args)
    if settings.key_provider != "file":
        print("rotate-key manages the local key directory; rotate env/keychain keys at the source")
        return 2
    key_id = KeyFileKeyring(settings.keys_dir / "data").rotate()
    print(f"new objects are now encrypted with {key_id}; older keys are kept for reading")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    from .doctor import run_checks

    checks = run_checks(_settings(args))
    for check in checks:
        print(f"  {'ok  ' if check.ok else 'FAIL'} {check.name:<22} {check.detail}")
    return 0 if all(c.ok for c in checks) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakehouse", description=__doc__)
    parser.add_argument("--home", help="data root (default: $LAKEHOUSE_HOME or ./.lakehouse)")
    commands = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    # SUPPRESS keeps a subcommand from overwriting `lakehouse --home X <command>` with None.
    common.add_argument("--home", default=argparse.SUPPRESS, help="data root")

    def add(name: str, text: str, run: Callable[[argparse.Namespace], int]) -> Any:
        command = commands.add_parser(name, parents=[common], help=text)
        command.set_defaults(run=run)
        return command

    def workload_args(command: argparse.ArgumentParser) -> None:
        command.add_argument("--producers", type=int, default=4)
        command.add_argument("--events", type=int, default=3_000, help="events per producer")
        command.add_argument("--days", type=int, default=3)
        command.add_argument("--seed", type=int, default=7)

    workload_args(add("ingest", "deliver synthetic producer events to the log", cmd_ingest))
    steps: dict[str, tuple[str, Callable[[Pipeline], dict[str, Any]]]] = {
        "export": ("write sealed segments to Bronze, encrypt, enqueue", Pipeline.export),
        "sync": ("upload, read back, verify, catalog", Pipeline.sync),
        "model": ("build Silver and Gold with DuckDB", Pipeline.model),
        "tier": ("reclaim local copies whose remote copy is proven", Pipeline.tier),
    }
    for name, (text, method) in steps.items():
        add(name, text, functools.partial(_run_pipeline_step, method=method))
    add("verify", "re-verify every remote object", cmd_verify)
    add("status", "summarize the data root", cmd_status).add_argument("--json", action="store_true")
    add("report", "write the static HTML run report", cmd_report).add_argument("--out")
    demo = add("demo", "run everything end to end on synthetic data", cmd_demo)
    workload_args(demo)
    demo.add_argument("--no-crash", action="store_true", help="skip the injected crash")
    bench = add("bench", "measure throughput and overheads", cmd_bench)
    bench.add_argument("--events", type=int, default=100_000, help="total events")
    bench.add_argument("--producers", type=int, default=4)
    bench.add_argument("--repeats", type=int, default=3)
    bench.add_argument("--json", action="store_true")
    add("rotate-key", "start encrypting new objects with a new data key", cmd_rotate_key)
    add("doctor", "check configuration and dependencies", cmd_doctor)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        code: int = args.run(args)
    except LakehouseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return code
