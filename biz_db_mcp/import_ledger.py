"""Persistent idempotency ledger for whitelisted artifact imports.

The ledger is an ordinary InnoDB table in the same schema as the whitelisted
business tables. A DBA creates it from ``migrations/001_import_ledger.sql``;
the service never runs DDL and never creates the table on its own.

Two invariants matter:

* The idempotency key is the SHA-256 of the normalized institution code, the
  configured database id, the canonical ``schema.table``, the content digest,
  and the import mode. The unique index on that column makes "the same file,
  same target, same mode" a single side effect even across processes.
* The ledger row is written in the same transaction as the business rows. A
  duplicate key therefore means another transaction already committed the
  whole import, so the caller gets the committed result instead of a second
  write.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from .errors import IMPORTS_DISABLED, BizDbError

LEDGER_MISSING_MESSAGE = (
    "import ledger is missing or malformed; apply migrations/001_import_ledger.sql"
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_$]{1,64}$")
_IDEMPOTENCY_COLUMN = "idempotency_key"
_DUPLICATE_KEY_CODE = 1062


def idempotency_key(
    institution_code: str,
    database_id: str,
    table: str,
    content_sha256: str,
    mode: str,
) -> str:
    """Return the stable ledger key for one import request.

    *table* is the canonical ``schema.table``; it is lower-cased so a request
    that differs only in case still maps to the same ledger row.
    """
    parts = (institution_code, database_id, table.lower(), content_sha256, mode)
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def insert_entry(
    cursor: Any,
    *,
    ledger_table: str,
    key: str,
    batch_id: str,
    institution_code: str,
    database_id: str,
    table: str,
    content_sha256: str,
    mode: str,
    row_count: int,
) -> bool:
    """Insert the in-flight ledger row.

    Returns ``False`` when another transaction already owns *key* (MySQL
    error 1062). Other database errors are re-raised unchanged so the caller
    can roll back and report a redacted ``DATABASE_ERROR``.
    """
    sql = (
        f"INSERT INTO {_quote_table(ledger_table)} "
        "(`idempotency_key`, `batch_id`, `institution_code`, `database_id`, "
        "`table_name`, `content_sha256`, `mode`, `row_count`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    params = (
        key,
        batch_id,
        institution_code,
        database_id,
        table.lower(),
        content_sha256,
        mode,
        row_count,
    )
    try:
        cursor.execute(sql, params)
    except Exception as exc:  # noqa: BLE001 - classified by MySQL error code
        if _mysql_error_code(exc) == _DUPLICATE_KEY_CODE:
            return False
        raise
    return True


def finalize_entry(
    cursor: Any,
    *,
    ledger_table: str,
    batch_id: str,
    inserted: int,
    updated: int,
) -> None:
    """Record the final row counts on an in-flight ledger row."""
    sql = (
        f"UPDATE {_quote_table(ledger_table)} "
        "SET `inserted_rows` = %s, `updated_rows` = %s "
        "WHERE `batch_id` = %s"
    )
    cursor.execute(sql, (inserted, updated, batch_id))


def find_entry(
    cursor: Any,
    *,
    ledger_table: str,
    key: str,
    institution_code: str,
) -> dict[str, Any] | None:
    """Return the committed ledger row for *key*, or ``None``.

    The institution filter is defense in depth: even a key collision must
    never expose another institution's batch.
    """
    sql = (
        f"SELECT `batch_id`, `institution_code`, `database_id`, `table_name`, "
        "`content_sha256`, `mode`, `row_count`, `inserted_rows`, `updated_rows` "
        f"FROM {_quote_table(ledger_table)} "
        "WHERE `idempotency_key` = %s AND `institution_code` = %s LIMIT 1"
    )
    cursor.execute(sql, (key, institution_code))
    row = cursor.fetchone()
    if row is None:
        return None
    return {str(name).lower(): value for name, value in dict(row).items()}


def check_ledger(conn: Any, *, schema: str, ledger_table: str) -> None:
    """Verify the startup prerequisite for imports.

    The ledger must exist as an InnoDB table with a unique index exactly on
    ``idempotency_key``. Anything else fails closed with ``IMPORTS_DISABLED``
    and the migration instruction; the service does not attempt DDL.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT TABLE_TYPE, ENGINE FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (schema, ledger_table),
            )
            table = cursor.fetchone()
            cursor.execute(
                "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE "
                "FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND NON_UNIQUE = 0 "
                "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
                (schema, ledger_table),
            )
            index_rows = cursor.fetchall() or ()
    except BizDbError:
        raise
    except Exception as exc:  # noqa: BLE001 - fail closed, redacted
        # Startup cannot prove the ledger is usable, so imports must stay
        # disabled. The message names the migration and never echoes the
        # database error text.
        raise _missing() from exc

    if table is None:
        raise _missing()
    table_type = _field(table, "TABLE_TYPE")
    if table_type is not None and str(table_type).upper() != "BASE TABLE":
        raise _missing()
    if str(_field(table, "ENGINE") or "").upper() != "INNODB":
        raise _missing()
    if not _has_idempotency_unique_index(index_rows):
        raise _missing()


def _has_idempotency_unique_index(rows: Any) -> bool:
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        if _as_int(_field(row, "NON_UNIQUE")) not in (0, None):
            continue
        name = str(_field(row, "INDEX_NAME") or "")
        column = _field(row, "COLUMN_NAME")
        if not name or column is None:
            continue
        sequence = _as_int(_field(row, "SEQ_IN_INDEX")) or len(grouped.get(name, ())) + 1
        grouped.setdefault(name, []).append((sequence, str(column)))
    for parts in grouped.values():
        columns = [column for _, column in sorted(parts)]
        if len(columns) == 1 and columns[0].lower() == _IDEMPOTENCY_COLUMN:
            return True
    return False


def _missing() -> BizDbError:
    return BizDbError(IMPORTS_DISABLED, LEDGER_MISSING_MESSAGE)


def _quote_table(ledger_table: str) -> str:
    """Quote a validated ``table`` or ``schema.table`` ledger name."""
    if not isinstance(ledger_table, str):
        raise BizDbError(IMPORTS_DISABLED, "import ledger table name is invalid")
    parts = ledger_table.split(".")
    if len(parts) > 2 or not all(_IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise BizDbError(IMPORTS_DISABLED, "import ledger table name is invalid")
    return ".".join(f"`{part}`" for part in parts)


def _mysql_error_code(exc: BaseException) -> int | None:
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int):
        return args[0]
    return None


def _field(row: Any, name: str) -> Any:
    if not isinstance(row, dict):
        return None
    for key, value in row.items():
        if str(key).lower() == name.lower():
            return value
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
