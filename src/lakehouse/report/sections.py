"""Report sections built from a snapshot: verdict, pipeline flow, run timeline, queue."""

from __future__ import annotations

from typing import Any

from .charts import Lane, Point, Span, lanes, legend
from .fmt import duration, epoch, esc, num, pct, short_object, size

ICON_CHECK = (
    '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M3.5 8.5l3 3 6-7" fill="none"'
    ' stroke="currentColor" stroke-width="1.8" stroke-linecap="round"'
    ' stroke-linejoin="round"/></svg>'
)
ICON_CROSS = (
    '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M4.5 4.5l7 7m0-7l-7 7" fill="none"'
    ' stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>'
)
ICON_ALERT = (
    '<svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 2.5l6 11H2z" fill="none"'
    ' stroke="currentColor" stroke-width="1.5" stroke-linejoin="round"/>'
    '<path d="M8 6.5v3.2M8 11.6v.1" stroke="currentColor" stroke-width="1.6"'
    ' stroke-linecap="round"/></svg>'
)


def last_step(snapshot: dict[str, Any], name: str) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = [e for e in snapshot["journal"] if e["step"] == name]
    return matches[-1] if matches else None


def steps(snapshot: dict[str, Any], name: str) -> list[dict[str, Any]]:
    return [entry for entry in snapshot["journal"] if entry["step"] == name]


def verification_summary(snapshot: dict[str, Any]) -> tuple[int, int]:
    checks = snapshot.get("verification", [])
    return sum(1 for v in checks if v["ok"]), len(checks)


def reclaimed_bytes(snapshot: dict[str, Any]) -> int:
    return sum(row["bytes"] or 0 for row in snapshot["tiering"]["timeline"])


def verdict(snapshot: dict[str, Any]) -> str:
    accounting = snapshot["accounting"]
    ok_count, total = verification_summary(snapshot)
    healthy = accounting["complete"] and ok_count == total
    ingest = last_step(snapshot, "ingest")
    run_seconds = sum(entry["seconds"] for entry in snapshot["journal"] if "seconds" in entry)
    producers = len(snapshot["log"]["producers"])
    pill = (
        f'<span class="pill good">{ICON_CHECK}Converged</span>'
        if healthy
        else f'<span class="pill critical">{ICON_ALERT}Needs attention</span>'
    )
    headline = (
        f"{num(accounting['committed'])} events from {producers} producers reached Gold"
        f" with {num(accounting['lost'])} lost and {num(accounting['duplicated'])} duplicated."
    )
    duplicates = ingest["result"]["duplicates_total"] if ingest else 0
    tiles = [
        (
            "Events committed",
            num(accounting["committed"]),
            f"{num(duplicates)} duplicate deliveries rejected",
        ),
        ("Objects verified", f"{ok_count}/{total}", "read back, SHA-256 + AES-GCM tag"),
        ("Local bytes reclaimed", size(reclaimed_bytes(snapshot)), "only after remote proof"),
        ("Pipeline time", duration(run_seconds), "all steps, this machine"),
    ]
    tile_html = "".join(
        f'<div class="tile"><div class="tile-label">{esc(label)}</div>'
        f'<div class="tile-value">{esc(value)}</div><div class="tile-sub">{esc(sub)}</div></div>'
        for label, value, sub in tiles
    )
    return (
        f'<section class="verdict">{pill}<h1>{esc(headline)}</h1>'
        '<p class="lede">A durable local log feeds immutable Bronze Parquet, which is sealed'
        " with AES-256-GCM, uploaded through a leased queue, read back and verified, and"
        " modeled into Silver and Gold with DuckDB. Local copies are reclaimed only after"
        f' the remote copy is proven.</p><div class="tiles">{tile_html}</div></section>'
    )


def _node(index: int, label: str, value: str, detail: str, note: str = "") -> str:
    badge = f'<span class="node-note">{esc(note)}</span>' if note else ""
    return (
        f'<li class="node"><span class="node-index">{index:02d}</span>'
        f'<span class="node-label">{esc(label)}</span>'
        f'<span class="node-value">{esc(value)}</span>'
        f'<span class="node-detail">{esc(detail)}</span>{badge}</li>'
    )


def pipeline(snapshot: dict[str, Any]) -> str:
    log, export, queue = snapshot["log"], snapshot["export"], snapshot["queue"]
    ingest = last_step(snapshot, "ingest")
    delivered = ingest["result"] if ingest else {"sent": 0, "redelivered": 0}
    items = queue["items"]
    plaintext = sum(i["plaintext_bytes"] for i in items)
    overhead = (
        (sum(i["encrypted_bytes"] for i in items) - plaintext) / plaintext if plaintext else None
    )
    ok_count, total = verification_summary(snapshot)
    bytes_per_event = export["bytes"] / max(export["rows"], 1)
    resumed = sum(
        e["result"]["already_present"] + e["result"]["resumed"] for e in steps(snapshot, "sync")
    )
    manifest = snapshot["warehouse"]["manifest"] or {}
    crashed = last_step(snapshot, "crash")
    nodes = [
        _node(
            1,
            "Producers",
            num(len(log["producers"])),
            f"{num(delivered['sent'])} deliveries, {num(delivered['redelivered'])} re-sent",
        ),
        _node(
            2,
            "Event log",
            num(log["committed"]),
            f"{num(log['duplicates'])} duplicates, {num(log['late_arrivals'])} late arrivals",
            "fsync before ack",
        ),
        _node(
            3,
            "Segments",
            num(log["segments"]),
            f"sealed, {num(snapshot['settings']['segment_rows'])} rows max, SHA-256",
        ),
        _node(
            4,
            "Bronze",
            f"{num(export['files'])} files",
            f"Parquet, {size(export['bytes'])}, {bytes_per_event:.0f} B/event",
        ),
        _node(
            5,
            "Encrypted",
            f"{num(len(items))} objects",
            f"AES-256-GCM + queued, {pct(overhead, 2)} overhead",
        ),
        _node(
            6,
            "Verified",
            f"{ok_count}/{total}",
            f"read back; {num(resumed)} recovered after crash"
            if crashed
            else "read back after upload",
            "crash injected" if crashed else "",
        ),
        _node(
            7,
            "Silver",
            num(manifest.get("silver_rows")),
            f"typed, {num(manifest.get('duplicates_removed'))} duplicates removed",
        ),
        _node(8, "Gold", f"{num(manifest.get('gold_days'))} days", "daily aggregates"),
    ]
    tiers = snapshot["tiering"]["timeline"]
    spool = [t for t in tiers if t["kind"] == "spool" and t["reclaimed_at"]]
    segments = [t for t in tiers if t["kind"] == "log_segment" and t["reclaimed_at"]]
    return (
        '<section class="card"><header class="card-head"><h2>Pipeline</h2>'
        "<p>Counts at each stage of this run. Numbered in data order.</p></header>"
        f'<ol class="flow">{"".join(nodes)}</ol>'
        f'<div class="tier-strip"><span class="key bar m-reclaimed"></span><strong>Tiering</strong>'
        f"<span>reclaimed {len(segments)} log segments and {len(spool)} staged copies"
        f" ({size(reclaimed_bytes(snapshot))}) after re-verifying every remote object</span>"
        "</div></section>"
    )


def _time_bounds(snapshot: dict[str, Any]) -> tuple[float, float]:
    times = [epoch(e.get("started_at")) for e in snapshot["journal"]]
    times += [epoch(e.get("finished_at")) for e in snapshot["journal"]]
    times += [h["at"] for h in snapshot["queue"]["history"]]
    found = [t for t in times if t is not None]
    return (min(found), max(found)) if found else (0.0, 1.0)


def timeline(snapshot: dict[str, Any]) -> str:
    t0, t1 = _time_bounds(snapshot)
    step_lane = Lane("Pipeline steps")
    for entry in snapshot["journal"]:
        start, end = epoch(entry.get("started_at")), epoch(entry.get("finished_at"))
        if start is not None and entry["step"] in ("crash", "restart"):
            step_lane.points.append(Point(start, "m-crash", _event_text(entry)))
        elif start is not None and end is not None:
            step_lane.spans.append(
                Span(
                    start,
                    end,
                    "m-step",
                    f"{entry['step']} · {duration(entry['seconds'])}",
                    entry["step"],
                )
            )
    rows = [step_lane]
    milestones = (
        ("sealed_at", "m-sealed", "sealed"),
        ("exported_at", "m-exported", "exported + queued"),
        ("verified_at", "m-verified", "remote verified"),
        ("reclaimed_at", "m-reclaimed", "local rows reclaimed"),
    )
    for segment in snapshot["segments"]:
        lane = Lane(segment["name"], f"seq {num(segment['first_seq'])}–{num(segment['last_seq'])}")
        for key, series, text in milestones:
            at = epoch(segment[key])
            if at is not None:
                lane.points.append(Point(at, series, f"{segment['name']} {text}"))
        rows.append(lane)
    keys = legend(
        [
            ("m-step", "pipeline step", "bar"),
            ("m-sealed", "sealed (local only)", "dot"),
            ("m-exported", "exported to Bronze + queued", "dot"),
            ("m-verified", "remote copy verified", "dot"),
            ("m-reclaimed", "local rows reclaimed", "dot"),
        ]
    )
    events = [e for e in snapshot["journal"] if e["step"] in ("crash", "restart")]
    notes = "".join(
        f'<li><span class="pill critical small">{ICON_ALERT}{esc(e["step"])}</span>'
        f"<span>{esc(_event_text(e))}</span></li>"
        for e in events
    )
    note_html = f'<ul class="events">{notes}</ul>' if notes else ""
    table_rows = "".join(
        f"<tr><td>{esc(s['name'])}</td><td class=num>{num(s['rows'])}</td>"
        + "".join(f"<td>{esc((s[k] or '–')[11:23])}</td>" for k, _, _ in milestones)
        + "</tr>"
        for s in snapshot["segments"]
    )
    return (
        '<section class="card"><header class="card-head"><h2>Run timeline</h2>'
        "<p>Each sealed segment moves left to right: sealed, exported, verified remotely,"
        " then reclaimed locally. Reclamation never precedes verification.</p></header>"
        f"{keys}{lanes(rows, t0, t1, label='Segment milestones over time')}{note_html}"
        '<details class="table-view"><summary>Show as table (UTC times)</summary>'
        '<div class="scroll"><table><thead><tr><th>Segment</th><th class=num>Rows</th>'
        "<th>Sealed</th><th>Exported</th><th>Verified</th><th>Reclaimed</th></tr></thead>"
        f"<tbody>{table_rows}</tbody></table></div></details></section>"
    )


def _event_text(entry: dict[str, Any]) -> str:
    if entry["step"] == "crash":
        return f"process killed at {entry['result']['checkpoint']} (injected)"
    return f"new process recovered {entry['result']['recovered_leases']} orphaned lease(s)"


STATUS_SERIES = {
    "pending": ("q-pending", "pending"),
    "uploading": ("q-uploading", "uploading"),
    "object_durable": ("q-durable", "object durable"),
    "completed": ("q-completed", "completed"),
}


def queue(snapshot: dict[str, Any]) -> str:
    moments = [row["at"] for row in snapshot["queue"]["history"]]
    if not moments:
        return '<section class="card"><h2>Upload queue</h2><p class="empty">Empty.</p></section>'
    # The queue gets its own window (first enqueue to last transition) so states are legible.
    t0, t1 = min(moments), max(moments) + (max(moments) - min(moments)) * 0.04
    history: dict[str, list[dict[str, Any]]] = {}
    for row in snapshot["queue"]["history"]:
        history.setdefault(row["idempotency_key"], []).append(row)
    rows = []
    for item in snapshot["queue"]["items"]:
        lane = Lane(short_object(item["object_key"]), f"{item['attempts']} attempt(s)")
        events = history.get(item["idempotency_key"], [])
        for current, following in zip(events, [*events[1:], None], strict=True):
            end = following["at"] if following else t1
            series, text = STATUS_SERIES[current["status"]]
            lane.spans.append(
                Span(
                    current["at"],
                    end,
                    series,
                    f"{text} · {duration(end - current['at'])} · {current['detail']}",
                )
            )
        rows.append(lane)
    counts = snapshot["queue"]["by_status"]
    count_html = "".join(
        f'<li><span class="key bar {STATUS_SERIES[s][0]}"></span>'
        f"<strong>{num(counts[s])}</strong> {esc(STATUS_SERIES[s][1])}</li>"
        for s in STATUS_SERIES
    )
    return (
        '<section class="card"><header class="card-head"><h2>Upload queue</h2>'
        "<p>State of every object from enqueue to completion. Claims take a lease under"
        " BEGIN IMMEDIATE; a crashed worker's lease is recovered by the next process.</p>"
        "</header>"
        f'<ul class="legend counts">{count_html}</ul>'
        f"{lanes(rows, t0, t1, label='Queue state per object over time')}</section>"
    )
