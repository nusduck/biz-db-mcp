"""Defense-in-depth guards for single-statement reads and DML writes."""

from __future__ import annotations

import re

import sqlparse
from sqlparse.sql import Where
from sqlparse.tokens import Comment

# Only constructs that can actually appear *inside* an otherwise-valid single
# SELECT/DML statement are checked here. Statement-leading operations (GRANT,
# CREATE, DROP, ALTER, TRUNCATE, REVOKE, CALL, INSERT, UPDATE, DELETE,
# REPLACE) are already rejected by the leading-keyword/statement-type checks
# below, so matching them here too only produces false positives against
# identifiers that happen to share a keyword's spelling (e.g. a `grant` or
# `call` column).
_FORBIDDEN_KEYWORD_RE = re.compile(
    r"\bLOAD_FILE\b|\bINTO\s+(OUTFILE|DUMPFILE)\b",
    re.IGNORECASE,
)
_FORBIDDEN_WRITE_RE = _FORBIDDEN_KEYWORD_RE
_WRITE_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "REPLACE"}


def _normalized_sql(statement) -> str:
    """Flatten *statement* back to text with comments replaced by whitespace.

    This closes obfuscation tricks like ``INTO/**/OUTFILE`` where a comment
    stands in for whitespace: regex matching against the raw SQL text would
    otherwise miss the forbidden phrase because no literal whitespace
    separates the two words.
    """
    return "".join(
        " " if tok.ttype in Comment else tok.value for tok in statement.flatten()
    )


class SqlGuardError(ValueError):
    """Raised when a SQL string fails the read-only SELECT check."""


def _single_statement(sql: str):
    if not isinstance(sql, str) or not sql.strip():
        raise SqlGuardError("empty SQL statement")
    statements = [
        s for s in sqlparse.parse(sql) if s.token_first(skip_cm=True) is not None
    ]
    if len(statements) != 1:
        raise SqlGuardError("exactly one SQL statement is allowed (no batching)")
    return statements[0]


def _leading_keyword(statement) -> str:
    first_token = statement.token_first(skip_cm=True)
    return (first_token.value or "").strip().upper()


def _has_top_level_where(statement) -> bool:
    return any(isinstance(token, Where) for token in statement.tokens)


def assert_select_only(sql: str) -> None:
    """Raise SqlGuardError unless *sql* is exactly one read-only SELECT."""
    statement = _single_statement(sql)
    stmt_type = statement.get_type()
    if stmt_type not in ("SELECT", "UNKNOWN"):
        # sqlparse classifies a leading `WITH ... SELECT` as UNKNOWN.
        raise SqlGuardError(f"only SELECT statements are allowed (got {stmt_type})")

    leading_keyword = _leading_keyword(statement)
    if leading_keyword not in ("SELECT", "WITH"):
        raise SqlGuardError(
            f"statement must start with SELECT or WITH (got {leading_keyword!r})"
        )

    if _FORBIDDEN_KEYWORD_RE.search(_normalized_sql(statement)):
        raise SqlGuardError("statement contains a forbidden keyword")


def assert_write_only(sql: str, *, require_where: bool = True) -> None:
    """Allow one bounded DML statement and reject schema/admin operations."""
    statement = _single_statement(sql)
    leading_keyword = _leading_keyword(statement)
    if leading_keyword not in _WRITE_KEYWORDS:
        raise SqlGuardError(
            "only INSERT, UPDATE, DELETE, or REPLACE statements are allowed "
            f"(got {leading_keyword!r})"
        )
    if _FORBIDDEN_WRITE_RE.search(_normalized_sql(statement)):
        raise SqlGuardError("statement contains a forbidden write operation")
    if require_where and leading_keyword in {"UPDATE", "DELETE"}:
        if not _has_top_level_where(statement):
            raise SqlGuardError(f"{leading_keyword} requires a WHERE clause")
