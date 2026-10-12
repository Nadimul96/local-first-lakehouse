"""Fake secrets are assembled at runtime so no literal resembles a real credential."""

from __future__ import annotations

import hashlib
import os

import pytest

from lakehouse.ingest.redaction import TAG_PREFIX, Redactor

KEY = b"k" * 32
FAKE_API_KEY = "sk-" + "test-" + "Ab3" * 10
FAKE_AWS = "AKIA" + "Z" * 16
FAKE_GITHUB = "ghp_" + "a1" * 18
FAKE_JWT = "eyJ" + "a" * 10 + "." + "b" * 10 + "." + "c" * 10
EMAIL = "jamie" + "@" + "example.com"


@pytest.mark.parametrize(
    ("text", "rule"),
    [
        (f"token {FAKE_API_KEY} here", "openai-key"),
        (f"aws {FAKE_AWS}", "aws-access-key"),
        (f"gh {FAKE_GITHUB}", "github-token"),
        (f"auth {FAKE_JWT}", "jwt"),
        (f"mail {EMAIL}", "email"),
        ("card 4242 4242 4242 4242", "card-number"),
        ("ssn 123-45-6789", "ssn"),
        ("postgres://app:" + "hunter22" + "@db/x", "url-credentials"),
        ("password=" + "Tr0ub4dor3", "assigned-secret"),
    ],
)
def test_detects_each_secret_class(text: str, rule: str) -> None:
    clean, counts = Redactor(KEY).redact_text(text)
    assert counts[rule] == 1
    assert TAG_PREFIX + rule in clean


def test_tag_is_keyed_hmac_not_a_plain_hash() -> None:
    tag_a = Redactor(KEY).tag("email", EMAIL)
    tag_b = Redactor(b"q" * 32).tag("email", EMAIL)
    assert tag_a == Redactor(KEY).tag("email", EMAIL)  # stable per install
    assert tag_a != tag_b  # unlinkable across installs
    plain = hashlib.sha256(EMAIL.encode()).hexdigest()[:8]
    assert plain not in tag_a


def test_redaction_is_idempotent() -> None:
    redactor = Redactor(os.urandom(32))
    once, _ = redactor.redact_text(f"{EMAIL} {FAKE_API_KEY} card 4242 4242 4242 4242")
    twice, counts = redactor.redact_text(once)
    assert twice == once
    assert not counts


def test_numbers_that_fail_luhn_are_kept() -> None:
    clean, counts = Redactor(KEY).redact_text("order 1234 5678 9012 3456 shipped")
    assert counts["card-number"] == 0
    assert "1234 5678 9012 3456" in clean


def test_hex_digests_and_uuids_survive_the_entropy_net() -> None:
    digest = hashlib.sha256(b"x").hexdigest()
    uuid = "6f1c0d3e-8b7a-4f60-9a51-2f5d0c9e4b11"
    clean, _ = Redactor(KEY).redact_text(f"{digest} {uuid}")
    assert digest in clean and uuid in clean


def test_values_under_credential_keys_are_always_hidden() -> None:
    clean, counts = Redactor(KEY).redact_value(
        {"user_id": "user-1", "password": "short", "nested": [{"api_key": "abc"}]}
    )
    assert clean["user_id"] == "user-1"
    assert clean["password"].startswith(TAG_PREFIX + "credential-field")
    assert clean["nested"][0]["api_key"].startswith(TAG_PREFIX)
    assert counts["credential-field"] == 2


def test_short_keys_are_rejected() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        Redactor(b"short")
