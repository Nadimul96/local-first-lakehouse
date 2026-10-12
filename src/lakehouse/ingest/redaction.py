"""Secret and PII redaction applied before anything is persisted. Stdlib only.

Replacement tags are stable per value, `⟨REDACTED:email:1f2e3d4c⟩`, so a value that recurs
still groups together downstream. The tag is a truncated HMAC-SHA256 under a per-install
key: an unsalted hash of a short secret (a PIN, a phone number) could be brute-forced
back from the tag by anyone who reads the lake; an HMAC cannot without the key.

Rules run most-specific first; the high-entropy net runs last. Ambiguous numeric shapes
need a checksum (Luhn) so ids and timestamps are not destroyed by false positives.
"""

from __future__ import annotations

import functools
import hashlib
import hmac
import math
import re
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

POLICY_VERSION = "redaction.hmac.v1"
TAG_PREFIX = "⟨REDACTED:"

CREDENTIAL_KEY = re.compile(
    r"(pass(word|phrase|code)?|secret|api[_-]?key|private[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|auth[_-]?token|bearer|authorization|cookie|credential|"
    r"(^|[_-])(token|otp|pin|ssn|cvv|cvc)($|[_-]))",
    re.IGNORECASE,
)


def _luhn_ok(value: str) -> bool:
    digits = re.sub(r"\D", "", value)
    if not 13 <= len(digits) <= 19 or len(set(digits)) < 2:
        return False
    total = 0
    for index, char in enumerate(reversed(digits)):
        digit = int(char) * (2 if index % 2 else 1)
        total += digit - 9 if digit > 9 else digit
    return total % 10 == 0


def _assignment_ok(value: str) -> bool:
    return any(ch.isdigit() for ch in value) or (len(value) >= 16 and "/" not in value)


def _entropy_bits(value: str) -> float:
    counts = Counter(value)
    return -sum(c / len(value) * math.log2(c / len(value)) for c in counts.values())


def _high_entropy_ok(value: str) -> bool:
    if re.fullmatch(r"[0-9a-fA-F]+", value) or re.fullmatch(r"[0-9a-fA-F-]{36}", value):
        return False  # hex digests and UUIDs are identifiers, not secrets
    classes = sum(bool(re.search(p, value)) for p in (r"[a-z]", r"[A-Z]", r"[0-9]"))
    wordlike = any(len(part) >= 4 and part.isalpha() for part in re.split(r"[-_+=]", value))
    return classes == 3 and not wordlike and _entropy_bits(value) >= 4.0


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    group: int = 0
    validate: Callable[[str], bool] | None = None


# `⟨` is excluded from value classes so a tag can never be re-matched (idempotence).
RULES: tuple[Rule, ...] = (
    Rule(
        "private-key",
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
    ),
    Rule("anthropic-key", re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}")),
    Rule("openai-key", re.compile(r"sk-[A-Za-z0-9_-]{20,}")),
    Rule("github-token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_\w{22,})")),
    Rule("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    Rule("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}")),
    Rule("stripe-key", re.compile(r"\b[rs]k_(?:live|test)_[A-Za-z0-9]{10,}")),
    Rule("slack-token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    Rule("jwt", re.compile(r"\beyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]{8,}")),
    Rule("url-credentials", re.compile(r"://[^/\s:@⟨]{1,64}:([^@\s/⟨]{4,})@"), group=1),
    Rule("email", re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")),
    Rule("ssn", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    Rule("card-number", re.compile(r"\b(?:\d[ -]?){13,19}\b"), validate=_luhn_ok),
    Rule("bearer-token", re.compile(r"(?i)\bbearer\s+([A-Za-z0-9._~+/=-]{16,})"), group=1),
    Rule(
        "assigned-secret",
        re.compile(
            r"(?i)\b(?:api[_-]?key|secret|password|passwd|token|access[_-]?token)\b"
            r"[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9_\-./+=]{8,})"
        ),
        group=1,
        validate=_assignment_ok,
    ),
    Rule("high-entropy", re.compile(r"\b[A-Za-z0-9_+=-]{24,}\b"), validate=_high_entropy_ok),
)


class Redactor:
    def __init__(self, key: bytes) -> None:
        if len(key) < 32:
            raise ValueError("redaction key must be at least 32 bytes")
        self._key = key

    def tag(self, rule: str, value: str) -> str:
        mac = hmac.new(self._key, value.encode("utf-8", "replace"), hashlib.sha256)
        return f"{TAG_PREFIX}{rule}:{mac.hexdigest()[:8]}⟩"

    def redact_text(self, text: str) -> tuple[str, Counter[str]]:
        counts: Counter[str] = Counter()
        for rule in RULES:
            text = rule.pattern.sub(
                functools.partial(self._replace, rule=rule, counts=counts), text
            )
        return text, counts

    def _replace(self, match: re.Match[str], rule: Rule, counts: Counter[str]) -> str:
        value = match.group(rule.group)
        if rule.validate is not None and not rule.validate(value):
            return match.group(0)
        counts[rule.name] += 1
        if rule.group == 0:
            return self.tag(rule.name, value)
        start, end = match.span(rule.group)
        offset = match.start(0)
        whole = match.group(0)
        return whole[: start - offset] + self.tag(rule.name, value) + whole[end - offset :]

    def redact_value(self, value: Any) -> tuple[Any, Counter[str]]:
        """Recursively redact strings. Values under credential-named keys are always hidden."""
        counts: Counter[str] = Counter()
        if isinstance(value, str):
            return self.redact_text(value)
        if isinstance(value, list):
            cleaned_items = []
            for item in value:
                cleaned, item_counts = self.redact_value(item)
                counts += item_counts
                cleaned_items.append(cleaned)
            return cleaned_items, counts
        if isinstance(value, dict):
            cleaned_map: dict[str, Any] = {}
            for key, item in value.items():
                if isinstance(key, str) and CREDENTIAL_KEY.search(key):
                    counts["credential-field"] += 1
                    cleaned_map[key] = self.tag("credential-field", repr(item))
                    continue
                cleaned, item_counts = self.redact_value(item)
                counts += item_counts
                cleaned_map[key] = cleaned
            return cleaned_map, counts
        return value, counts
