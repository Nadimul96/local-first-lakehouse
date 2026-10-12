"""Report sections that are mostly tables: verification, ingest, tiering, Gold."""

from __future__ import annotations

from typing import Any

from .fmt import esc, money, num, pct, short_object, size
from .sections import ICON_CHECK, ICON_CROSS, last_step

CHECK_LABELS = (
    ("bytes_match", "Bytes + SHA-256"),
    ("gcm_tag", "GCM tag"),
    ("plaintext_match", "Plaintext SHA-256"),
    ("bound_to_key", "Bound to key"),
)


def _check(ok: bool | None) -> str:
    if ok is None:
        return (
            '<td class="check skip" title="not reached">–<span class="sr">not reached</span></td>'
        )
    if ok:
        return f'<td class="check ok">{ICON_CHECK}<span class="sr">pass</span></td>'
    return f'<td class="check bad">{ICON_CROSS}<span class="sr">fail</span></td>'


def _verification_row(result: dict[str, Any], item: dict[str, Any], note: str = "") -> str:
    cells = "".join(_check(result["checks"][key]) for key, _ in CHECK_LABELS)
    label = short_object(result["object_key"])
    extra = f"<small>{esc(note)}</small>" if note else ""
    return (
        f'<tr class="{"drill" if note else ""}"><td><span class="mono">{esc(label)}</span>{extra}'
        f"</td><td class=num>{size(item.get('encrypted_bytes'))}</td>"
        f"<td class=mono>{esc(item.get('key_id', '–'))}</td>{cells}</tr>"
    )


def verification(snapshot: dict[str, Any]) -> str:
    items = {i["object_key"]: i for i in snapshot["queue"]["items"]}
    rows = [
        _verification_row(v, items.get(v["object_key"], {}))
        for v in snapshot.get("verification", [])
    ]
    drill = last_step(snapshot, "tamper_drill")
    drill_html = ""
    if drill and drill["result"].get("ran"):
        tampered = drill["result"]["tampered"]
        rows.insert(
            0,
            _verification_row(
                tampered, items.get(tampered["object_key"], {}), "tamper drill: 1 byte flipped"
            ),
        )
        drill_html = (
            f'<p class="callout"><strong>Tamper drill.</strong> One ciphertext byte of'
            f' <span class="mono">{esc(short_object(tampered["object_key"]))}</span> was flipped'
            f" on read. Verification rejected it ({esc(tampered['error'])}). The stored object"
            " still verifies, so nothing was ever reclaimed against a bad copy. A dash marks a"
            " check that cannot run once authentication fails.</p>"
        )
    head = "".join(f"<th>{esc(label)}</th>" for _, label in CHECK_LABELS)
    return (
        '<section class="card"><header class="card-head"><h2>Read-back verification</h2>'
        "<p>Every remote object, downloaded again and checked four ways.</p></header>"
        f"{drill_html}"
        '<div class="scroll"><table class="checks"><thead><tr><th>Object</th>'
        f"<th class=num>Encrypted size</th><th>Data key</th>{head}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div></section>"
    )


REDACTION_LABELS = {
    "openai-key": "API keys (sk-…)",
    "card-number": "card numbers",
    "email": "email addresses",
    "credential-field": "credential fields",
}


def ingest(snapshot: dict[str, Any]) -> str:
    log = snapshot["log"]
    step = last_step(snapshot, "ingest")
    redactions = step["result"]["redactions"] if step else {}
    producer_rows = "".join(
        f"<tr><td class=mono>{esc(p['producer_id'])}</td><td class=num>{num(p['committed'])}</td>"
        f"<td class=num>{num(p['max_seen'])}</td><td class=num>{num(p['missing'])}</td></tr>"
        for p in log["producers"]
    )
    chips = "".join(
        f"<li><strong>{num(count)}</strong> {esc(REDACTION_LABELS.get(name, name))}</li>"
        for name, count in sorted(redactions.items(), key=lambda kv: -kv[1])
    )
    return (
        '<section class="card"><header class="card-head"><h2>Ingest</h2>'
        "<p>Per-producer watermarks prove which sequence numbers arrived; a gap is a"
        " missing event, not an assumption.</p></header>"
        '<div class="scroll"><table><thead><tr><th>Producer</th><th class=num>Committed</th>'
        "<th class=num>Highest seq</th><th class=num>Missing now</th></tr></thead>"
        f"<tbody>{producer_rows}</tbody></table></div>"
        '<dl class="facts"><div><dt>Late arrivals (gaps filled)</dt>'
        f"<dd>{num(log['late_arrivals'])}</dd></div>"
        f"<div><dt>Peak missing at once</dt><dd>{num(log['peak_missing'])}</dd></div>"
        f"<div><dt>Conflicts rejected</dt><dd>{num(log['conflicts'])}</dd></div></dl>"
        '<h3>Redacted before write</h3><p class="hint">HMAC-tagged with a per-install key,'
        " so equal values still group without being reversible.</p>"
        f'<ul class="chips">{chips or "<li>none</li>"}</ul></section>'
    )


def tiering(snapshot: dict[str, Any]) -> str:
    timeline = snapshot["tiering"]["timeline"]
    spool = [t for t in timeline if t["kind"] == "spool" and t["reclaimed_at"]]
    segments = [t for t in timeline if t["kind"] == "log_segment" and t["reclaimed_at"]]
    disk = {k: v for k, v in snapshot["disk"].items() if v is not None}
    largest = max(disk.values() or [1]) or 1
    labels = {
        "log": "Event log",
        "spool": "Staged ciphertext",
        "bronze": "Bronze Parquet",
        "warehouse": "Silver + Gold",
        "state": "Ledgers + queue",
        "remote": "Remote (local stand-in)",
    }
    bars = "".join(
        f'<li><span class="bar-label">{esc(labels.get(k, k))}</span><span class="bar-track">'
        f'<span class="bar-fill" style="width:{v / largest * 100:.2f}%"></span></span>'
        f'<span class="bar-value">{size(v)}</span></li>'
        for k, v in disk.items()
    )
    return (
        '<section class="card"><header class="card-head"><h2>Tiering</h2>'
        "<p>Intent, delete, receipt. The intent records its proof first, so a crash"
        " mid-reclaim is finished by the next run.</p></header>"
        f'<dl class="facts"><div><dt>Log segments reclaimed</dt><dd>{len(segments)}</dd>'
        f"<small>{size(sum(t['bytes'] for t in segments))} of event payload</small></div>"
        f"<div><dt>Staged copies reclaimed</dt><dd>{len(spool)}</dd>"
        f"<small>{size(sum(t['bytes'] for t in spool))} of ciphertext</small></div></dl>"
        f'<h3>Local footprint now</h3><ul class="bars">{bars}</ul></section>'
    )


def gold(snapshot: dict[str, Any]) -> str:
    rows = snapshot["warehouse"]["gold_daily"]
    if not rows:
        return '<section class="card"><h2>Gold</h2><p class="empty">Not modeled yet.</p></section>'
    peak = max(r["net_cents"] for r in rows) or 1
    body = "".join(
        f"<tr><td class=mono>{esc(r['event_date'])}</td><td class=num>{num(r['events'])}</td>"
        f"<td class=num>{num(r['active_users'])}</td><td class=num>{num(r['checkouts'])}</td>"
        f"<td class=num>{num(r['payments'])}</td><td class=num>{pct(r['checkout_conversion'])}</td>"
        f"<td class=num>{money(r['gross_cents'])}</td>"
        f"<td class=num>{money(r['refunded_cents'])}</td>"
        f'<td><div class="inline-bar"><span class="bar-fill" style="width:'
        f'{max(r["net_cents"], 0) / peak * 110:.0f}px"></span>'
        f"<span class=num>{money(r['net_cents'])}</span></div></td></tr>"
        for r in rows
    )
    manifest = snapshot["warehouse"]["manifest"]
    return (
        '<section class="card"><header class="card-head"><h2>Gold: daily summary</h2>'
        f"<p>DuckDB over Silver (version <span class=mono>{esc(manifest['version'])}</span>,"
        f" built from {manifest['bronze_files']} Bronze files). Synthetic data.</p></header>"
        '<div class="scroll"><table class="gold"><thead><tr><th>Date</th><th class=num>Events</th>'
        "<th class=num>Active users</th><th class=num>Checkouts</th><th class=num>Payments</th>"
        "<th class=num>Conversion</th><th class=num>Gross</th><th class=num>Refunded</th>"
        f"<th>Net revenue</th></tr></thead><tbody>{body}</tbody></table></div></section>"
    )
