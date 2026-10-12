"""Small, escaping-safe formatting helpers for the report."""

from __future__ import annotations

from html import escape
from typing import Any

from ..canonical import parse_iso


def esc(value: Any) -> str:
    return escape(str(value), quote=True)


def num(value: float | int | None) -> str:
    return "–" if value is None else f"{value:,.0f}"


def size(value: float | int | None) -> str:
    if value is None:
        return "–"
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if value >= scale:
            return f"{value / scale:.1f} {unit}"
    return f"{value:.0f} B"


def pct(value: float | None, digits: int = 1) -> str:
    return "–" if value is None else f"{value * 100:.{digits}f}%"


def money(cents: float | int | None) -> str:
    return "–" if cents is None else f"${cents / 100:,.0f}"


def epoch(iso: str | None) -> float | None:
    return None if iso is None else parse_iso(iso).timestamp()


def duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    return f"{seconds:.2f} s"


def short_object(key: str) -> str:
    """`.../event_date=2026-10-01/part-seg-000003.parquet.enc` -> `10-01 · seg-000003`."""
    date = key.rsplit("event_date=", maxsplit=1)[-1][:10] if "event_date=" in key else ""
    segment = key.rsplit("part-", maxsplit=1)[-1].split(".", maxsplit=1)[0]
    return f"{date[5:]} · {segment}" if date else segment
