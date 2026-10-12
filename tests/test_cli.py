from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakehouse.cli import main
from lakehouse.errors import ConfigError


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> tuple[int, str]:
    code = main(list(argv))
    return code, capsys.readouterr().out


def test_demo_runs_end_to_end_and_writes_a_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = tmp_path / "demo"
    code, out = run(capsys, "demo", "--home", str(home), "--events", "150")
    assert code == 0
    assert "injected at sync.after_upload" in out
    assert "600 committed = 600 exported = 600 in Silver. 0 lost, 0 duplicated." in out
    html = (home / "report.html").read_text()
    for section in (
        "Pipeline",
        "Run timeline",
        "Upload queue",
        "Read-back verification",
        "Tiering",
        "Gold: daily summary",
        "Tamper drill",
    ):
        assert section in html
    assert "<script src=" not in html and "<link" not in html  # fully self-contained


def test_demo_refuses_to_delete_a_directory_it_did_not_create(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "precious").mkdir()
    (tmp_path / "precious" / "notes.txt").write_text("keep me")
    code = main(["demo", "--home", str(tmp_path / "precious"), "--events", "10"])
    assert code == 1
    assert (tmp_path / "precious" / "notes.txt").exists()
    assert "not created by the demo" in capsys.readouterr().err


def test_individual_steps_status_verify_and_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    home = str(tmp_path / "home")
    for step in (["ingest", "--events", "100"], ["export"], ["sync"], ["model"], ["tier"]):
        assert run(capsys, "--home", home, *step)[0] == 0
    code, out = run(capsys, "--home", home, "verify")
    assert code == 0 and "objects verified" in out
    code, out = run(capsys, "--home", home, "status", "--json")
    snapshot = json.loads(out)
    assert snapshot["accounting"]["complete"] is True
    assert snapshot["accounting"]["committed"] == 400
    code, out = run(capsys, "--home", home, "status")
    assert "0 lost, 0 duplicated" in out
    out_file = tmp_path / "r.html"
    assert run(capsys, "--home", home, "report", "--out", str(out_file))[0] == 0
    assert out_file.read_text().startswith("<!doctype html>")


def test_rotate_key_and_doctor(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    home = str(tmp_path / "home")
    code, out = run(capsys, "--home", home, "rotate-key")
    assert code == 0 and "new objects are now encrypted with" in out
    code, out = run(capsys, "--home", home, "doctor")
    assert code == 0 and "FAIL" not in out


def test_config_validation() -> None:
    from lakehouse.config import Settings

    with pytest.raises(ConfigError):
        Settings.from_env(environ={"LAKEHOUSE_STORE": "ftp"})
    with pytest.raises(ConfigError):
        Settings.from_env(environ={"LAKEHOUSE_SEGMENT_ROWS": "lots"})
