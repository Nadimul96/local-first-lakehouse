from __future__ import annotations

import re
from typing import Any

import pytest

from lakehouse.pipeline import Pipeline
from lakehouse.report import render_report
from lakehouse.snapshot import take
from tests.conftest import SMALL


@pytest.fixture
def snapshot(pipeline: Pipeline) -> dict[str, Any]:
    pipeline.run_all(SMALL)
    return take(pipeline, verify=True)


def test_snapshot_accounts_for_every_event(snapshot: dict[str, Any]) -> None:
    accounting = snapshot["accounting"]
    assert accounting == {
        "committed": SMALL.total_events,
        "exported": SMALL.total_events,
        "silver_rows": SMALL.total_events,
        "lost": 0,
        "duplicated": 0,
        "complete": True,
    }
    lanes = snapshot["segments"]
    assert all(
        lane["sealed_at"] <= lane["exported_at"] <= lane["verified_at"] <= lane["reclaimed_at"]
        for lane in lanes
    ), "a local copy was reclaimed before its remote copy was verified"


def test_report_is_self_contained_and_has_every_section(snapshot: dict[str, Any]) -> None:
    html = render_report(snapshot)
    for section in (
        "Pipeline",
        "Run timeline",
        "Upload queue",
        "Read-back verification",
        "Ingest",
        "Tiering",
        "Gold: daily summary",
    ):
        assert f">{section}<" in html
    assert not re.search(r"<(script|link)[^>]+(src|href)=", html)


def test_report_escapes_untrusted_strings(snapshot: dict[str, Any]) -> None:
    snapshot["queue"]["items"][0]["key_id"] = "<script>alert(1)</script>"
    snapshot["queue"]["history"][0]["detail"] = '" onmouseover="alert(1)'  # attribute context
    html = render_report(snapshot)
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert 'onmouseover="alert(1)' not in html
    assert "&quot; onmouseover=&quot;alert(1)" in html
