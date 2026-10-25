"""Host-injected institution identity and its normalization.

The platform injects the caller's UAuth institution id *raw* value, e.g.
``0800001000`` (two length digits ``08`` + the eight-digit code). Some sources
drop the length prefix. Authorization always compares the normalized code,
using exactly the platform rule (``agent/src/domain/identity/institute-code.ts``):
all ASCII digits, length >= 3, the first two digits L >= 1 and total length
exactly 2 + L -> keep the last L digits; otherwise the value is the code.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import INVALID_ARGUMENT, BizDbError

MAX_INSTITUTE_CODE_LENGTH = 64
# A legal two-digit length prefix may precede a maximum-length code.
MAX_INSTITUTE_RAW_LENGTH = MAX_INSTITUTE_CODE_LENGTH + 2

_DIGITS_RE = re.compile(r"^[0-9]{3,}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")


def institute_code_of(raw: str) -> str:
    """Return the normalized institution code for an already-trimmed value."""
    if not _DIGITS_RE.fullmatch(raw):
        return raw
    length = int(raw[:2])
    return raw[2:] if length >= 1 and len(raw) == 2 + length else raw


@dataclass(frozen=True)
class Institution:
    """One call's institution: the platform raw value and its normalized code."""

    raw: str
    code: str


def parse_institution(value: object) -> Institution:
    """Validate the host argument and normalize it; never guess a repair."""
    if not isinstance(value, str):
        raise BizDbError(INVALID_ARGUMENT, "instituteid must be a string")
    raw = value.strip()
    if not raw:
        raise BizDbError(INVALID_ARGUMENT, "instituteid is required")
    if len(raw) > MAX_INSTITUTE_RAW_LENGTH or _CONTROL_RE.search(raw):
        raise BizDbError(INVALID_ARGUMENT, "instituteid is malformed")
    code = institute_code_of(raw)
    if len(code) > MAX_INSTITUTE_CODE_LENGTH:
        raise BizDbError(INVALID_ARGUMENT, "instituteid is malformed")
    return Institution(raw=raw, code=code)
