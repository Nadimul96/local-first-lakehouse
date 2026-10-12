"""Timeline lanes rendered as plain HTML (percent-positioned), so text stays crisp and the
layout reflows at phone width. Every mark carries a focusable tooltip; every lane chart
has a table twin elsewhere in the report."""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from .fmt import esc


@dataclass(frozen=True)
class Span:
    start: float
    end: float
    series: str
    tip: str
    text: str = ""  # shown inside the span only when it is wide enough to hold it


@dataclass(frozen=True)
class Point:
    at: float
    series: str
    tip: str


@dataclass
class Lane:
    label: str
    sublabel: str = ""
    spans: list[Span] = field(default_factory=list)
    points: list[Point] = field(default_factory=list)


def _nice_step(span: float) -> float:
    raw = span / 5
    magnitude = 10.0 ** math.floor(math.log10(raw))
    for multiple in (1, 2, 2.5, 5, 10):
        if raw <= multiple * magnitude:
            return multiple * magnitude
    return 10 * magnitude


def _tick_label(seconds: float) -> str:
    if seconds == 0:
        return "0"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    return f"{seconds:.1f} s".replace(".0 s", " s")


def ticks(t0: float, t1: float) -> list[tuple[float, str]]:
    span = max(t1 - t0, 1e-3)
    step = _nice_step(span)
    out = []
    value = 0.0
    while value <= span + 1e-9:
        out.append((value / span * 100, _tick_label(value)))
        value += step
    return out


def lanes(rows: list[Lane], t0: float, t1: float, *, label: str) -> str:
    span = max(t1 - t0, 1e-3)

    def pos(t: float) -> float:
        return max(0.0, min(100.0, (t - t0) / span * 100))

    body = []
    for row in rows:
        marks = []
        if len(row.points) > 1 and not row.spans:
            first, last = min(p.at for p in row.points), max(p.at for p in row.points)
            marks.append(
                f'<span class="link" style="left:{pos(first):.3f}%;'
                f'width:{pos(last) - pos(first):.3f}%"></span>'
            )
        for s in row.spans:
            left, right = pos(s.start), pos(s.end)
            # ~6.5px per character at 11px against a ~900px desktop track; mobile hides text.
            fits = s.text and (right - left) / 100 * 900 > len(s.text) * 6.5 + 14
            inner = f'<span class="span-text">{esc(s.text)}</span>' if fits else ""
            marks.append(
                f'<span class="span {esc(s.series)}" tabindex="0" data-tip="{esc(s.tip)}"'
                f' style="left:{left:.3f}%;width:{max(right - left, 0):.3f}%">{inner}</span>'
            )
        for p in row.points:
            marks.append(
                f'<span class="pt {esc(p.series)}" tabindex="0" data-tip="{esc(p.tip)}"'
                f' style="left:{pos(p.at):.3f}%"></span>'
            )
        sub = f"<small>{esc(row.sublabel)}</small>" if row.sublabel else ""
        body.append(
            f'<div class="lane"><div class="lane-label">{esc(row.label)}{sub}</div>'
            f'<div class="lane-track">{"".join(marks)}</div></div>'
        )
    axis = "".join(
        f'<span class="tick" style="left:{left:.3f}%">{esc(text)}</span>'
        for left, text in ticks(t0, t1)
    )
    grid = "".join(
        f'<span class="gridline" style="left:{left:.3f}%"></span>' for left, _ in ticks(t0, t1)
    )
    return (
        f'<div class="lanes" role="group" aria-label="{esc(label)}">'
        f'<div class="lanes-grid" aria-hidden="true">{grid}</div>'
        f"{''.join(body)}"
        f'<div class="lane axis" aria-hidden="true"><div class="lane-label"></div>'
        f'<div class="lane-track">{axis}</div></div></div>'
    )


def legend(entries: list[tuple[str, str, str]]) -> str:
    """entries: (css series class, label, shape 'bar' | 'dot')."""
    items = "".join(
        f'<li><span class="key {shape} {esc(series)}"></span>{esc(text)}</li>'
        for series, text, shape in entries
    )
    return f'<ul class="legend">{items}</ul>'
