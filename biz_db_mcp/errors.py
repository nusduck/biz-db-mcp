"""Stable, redacted error codes returned to MCP callers.

Every failure a tool reports to the model is a ``BizDbError`` carrying one of
the codes below. Messages must never contain DSNs, passwords, SQL text,
parameters, or business values; internal detail belongs in the audit log as
an error category only.
"""

from __future__ import annotations

INVALID_ARGUMENT = "INVALID_ARGUMENT"
FORBIDDEN_INSTITUTION = "FORBIDDEN_INSTITUTION"
# Cross-institution denials and "does not exist" share this code on purpose.
RESOURCE_NOT_ACCESSIBLE = "RESOURCE_NOT_ACCESSIBLE"
SQL_REJECTED = "SQL_REJECTED"
READS_DISABLED = "READS_DISABLED"
WRITES_DISABLED = "WRITES_DISABLED"
IMPORTS_DISABLED = "IMPORTS_DISABLED"
TABLE_NOT_IMPORTABLE = "TABLE_NOT_IMPORTABLE"
CONTENT_DIGEST_MISMATCH = "CONTENT_DIGEST_MISMATCH"
INVALID_FILE = "INVALID_FILE"
VALIDATION_FAILED = "VALIDATION_FAILED"
TOO_LARGE = "TOO_LARGE"
RATE_LIMITED = "RATE_LIMITED"
BUSY = "BUSY"
DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
IMPORT_OUTCOME_UNKNOWN = "IMPORT_OUTCOME_UNKNOWN"
DATABASE_ERROR = "DATABASE_ERROR"
INTERNAL_ERROR = "INTERNAL_ERROR"


class BizDbError(Exception):
    """A caller-visible failure with a stable code and a redacted message."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def not_accessible(kind: str = "resource") -> BizDbError:
    """The single response for both cross-institution and missing resources."""
    return BizDbError(RESOURCE_NOT_ACCESSIBLE, f"{kind} does not exist or is not accessible")
