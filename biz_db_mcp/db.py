"""Database access: isolated reads, metadata, and the dev-only DML path."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

import pymysql
import pymysql.cursors

from .config import Settings, table_key
from .credentials import CredentialStore
from .errors import (
    DATABASE_ERROR,
    DEADLINE_EXCEEDED,
    INVALID_ARGUMENT,
    BizDbError,
    not_accessible,
)
from .policy import AccessPolicy, RowFilter, TableInfo, TableRef
from .sql_guard import assert_write_only, rewrite_select


class QueryError(RuntimeError):
    """Raised when a dev-only write violates a guardrail."""


_store: CredentialStore | None = None


def configure(store: CredentialStore) -> None:
    """Install the startup-resolved credentials used by every connection."""
    global _store
    _store = store


def _parse_url(dsn: str):
    try:
        parsed = urlparse(dsn)
        port = parsed.port or 3306
    except ValueError as exc:
        raise QueryError("invalid MySQL DSN") from exc
    database = unquote((parsed.path or "").lstrip("/"))
    return parsed, port, database


def _parse_dsn(dsn: str, *, password: str) -> dict[str, Any]:
    """Parse a passwordless mysql://user@host:port/db into pymysql kwargs."""
    parsed, port, database = _parse_url(dsn)
    return {
        "host": parsed.hostname,
        "port": port,
        "user": unquote(parsed.username or ""),
        "password": password,
        "database": database,
    }


def _connection_kwargs(settings: Settings, database_id: str, purpose: str) -> dict[str, Any]:
    if _store is None:
        raise BizDbError(DATABASE_ERROR, "credentials are not initialized")
    # The dev-only write path reuses the read credential without READ ONLY.
    credential = "import" if purpose == "import" else "read"
    return _store.connect_kwargs(settings.database(database_id).id, credential)  # type: ignore[arg-type]


@contextmanager
def _connection(
    settings: Settings,
    database_id: str | None,
    *,
    timeout_seconds: int,
    streaming: bool = False,
    autocommit: bool = True,
    purpose: str = "read",
) -> Iterator[Any]:
    database = settings.database(database_id)
    conn = pymysql.connect(
        **_connection_kwargs(settings, database.id, purpose),
        cursorclass=(
            pymysql.cursors.SSDictCursor if streaming else pymysql.cursors.DictCursor
        ),
        read_timeout=timeout_seconds,
        write_timeout=timeout_seconds,
        connect_timeout=10,
        autocommit=autocommit,
    )
    try:
        with conn.cursor() as cursor:
            # Server-side bound for SELECT; the client read timeout is a
            # second, independent safeguard.
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={int(timeout_seconds) * 1000}")
            if purpose == "read":
                cursor.execute("SET SESSION TRANSACTION READ ONLY")
                # Rewritten SQL is generated assuming backslash escapes and
                # backtick identifiers; pin the mode instead of trusting it.
                cursor.execute(
                    "SET SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, "
                    "'NO_BACKSLASH_ESCAPES', ''), 'ANSI_QUOTES', '')"
                )
        yield conn
    except Exception:
        if not autocommit:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        conn.close()


_IDENT_RE = re.compile(r"^[A-Za-z0-9_$]{1,64}$")


def _validate_identifier(name: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.fullmatch(name):
        raise BizDbError(INVALID_ARGUMENT, "invalid identifier")
    return name


def _split_table(table: str, default_schema: str) -> tuple[str, str]:
    if not isinstance(table, str):
        raise BizDbError(INVALID_ARGUMENT, "invalid table name")
    parts = [part.strip().strip("`") for part in table.strip().split(".")]
    if len(parts) == 1:
        return default_schema, _validate_identifier(parts[0])
    if len(parts) == 2:
        return _validate_identifier(parts[0]), _validate_identifier(parts[1])
    raise BizDbError(INVALID_ARGUMENT, "expected table or schema.table")


# ---------------------------------------------------------------------------
# Metadata and policy glue
# ---------------------------------------------------------------------------


def _table_info(conn: Any, schema: str, table: str) -> TableInfo | None:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT TABLE_TYPE AS kind FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (schema, table),
        )
        row = cursor.fetchone()
        if not row:
            return None
        cursor.execute(
            "SELECT COLUMN_NAME AS name FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (schema, table),
        )
        columns = frozenset(str(r["name"]).lower() for r in cursor.fetchall())
    return TableInfo(kind=str(row["kind"]), columns=columns)


def _resolver(
    settings: Settings, policy: AccessPolicy, database_id: str, conn: Any
) -> Callable[[str, str], RowFilter | None]:
    """table_filter callback for rewrite_select bound to one connection."""
    home = settings.database(database_id).schema.lower()

    def resolve(schema: str, table: str) -> RowFilter | None:
        explicit = table_key(database_id, schema, table) in settings.isolation.tables
        # Other schemas on the same server are reachable only by explicit policy.
        if schema.lower() != home and not explicit:
            raise not_accessible("table")
        ref = TableRef(database=database_id, schema=schema, table=table)
        return policy.table_filter(ref, _table_info(conn, schema, table))

    return resolve


def _is_accessible(
    settings: Settings, policy: AccessPolicy, database_id: str, conn: Any, schema: str, table: str
) -> bool:
    try:
        _resolver(settings, policy, database_id, conn)(schema, table)
    except BizDbError:
        return False
    return True


def _translate_db_error(exc: Exception) -> BizDbError:
    """Map a driver error to a stable code without its (possibly data-bearing) text."""
    errno = exc.args[0] if exc.args and isinstance(exc.args[0], int) else None
    if errno in (3024, 1317, 2013):
        return BizDbError(DEADLINE_EXCEEDED, "query exceeded its time limit")
    if errno in (1146, 1142, 1143, 1049):
        return not_accessible("table")
    hints = {
        1054: "unknown column",
        1060: "duplicate column name in the result; alias the columns",
        1064: "SQL syntax error",
        1052: "ambiguous column reference",
        1248: "every derived table needs an alias",
    }
    hint = hints.get(errno, "database error")
    suffix = f" (MySQL {errno})" if errno else ""
    return BizDbError(DATABASE_ERROR, f"{hint}{suffix}")


@contextmanager
def _db_errors() -> Iterator[None]:
    try:
        yield
    except BizDbError:
        raise
    except pymysql.MySQLError as exc:
        raise _translate_db_error(exc) from None


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------


def list_databases(settings: Settings, policy: AccessPolicy) -> list[dict[str, Any]]:
    out = []
    for database in settings.databases:
        try:
            policy.check_database(database.id)
        except BizDbError:
            continue
        out.append(
            {
                "id": database.id,
                "default": database.id == settings.default_database,
                "imports_enabled": bool(
                    settings.imports.enabled and database.allow_imports and policy.scope.allow_imports
                ),
            }
        )
    return out


def list_tables(
    settings: Settings,
    policy: AccessPolicy,
    *,
    database: str | None = None,
    pattern: str | None = None,
) -> list[dict[str, Any]]:
    """Tables and views of the database's schema that this institution may read."""
    selected = policy.check_database(database)
    if pattern is not None and (
        not isinstance(pattern, str)
        or len(pattern) > 64
        or not re.fullmatch(r"[A-Za-z0-9_%$]*", pattern)
    ):
        raise BizDbError(INVALID_ARGUMENT, "pattern may only contain letters, digits, _, $ and %")
    sql = (
        "SELECT TABLE_NAME AS name, TABLE_TYPE AS type, TABLE_COMMENT AS comment "
        "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s"
    )
    params: list[Any] = [selected.schema]
    if pattern:
        sql += " AND TABLE_NAME LIKE %s"
        params.append(pattern)
    sql += " ORDER BY TABLE_NAME LIMIT 2000"
    timeout = settings.effective_query_timeout(selected.id)
    with _db_errors(), _connection(settings, selected.id, timeout_seconds=timeout) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            candidates = list(cursor.fetchall())
        return [
            {"name": row["name"], "type": row["type"], "comment": row["comment"]}
            for row in candidates
            if _is_accessible(settings, policy, selected.id, conn, selected.schema, row["name"])
        ]


def describe_table(
    settings: Settings, policy: AccessPolicy, table: str, *, database: str | None = None
) -> list[dict[str, Any]]:
    selected = policy.check_database(database)
    schema, name = _split_table(table, selected.schema)
    timeout = settings.effective_query_timeout(selected.id)
    with _db_errors(), _connection(settings, selected.id, timeout_seconds=timeout) as conn:
        _resolver(settings, policy, selected.id, conn)(schema, name)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COLUMN_NAME AS name, COLUMN_TYPE AS type, IS_NULLABLE AS nullable, "
                "COLUMN_KEY AS `key`, COLUMN_DEFAULT AS `default`, COLUMN_COMMENT AS comment "
                "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                "ORDER BY ORDINAL_POSITION",
                (schema, name),
            )
            return list(cursor.fetchall())


def _query_limit(settings: Settings, limit: int | None, database: str | None = None) -> int:
    cap = settings.effective_query_max_rows(database)
    if limit is None:
        return cap
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise BizDbError(INVALID_ARGUMENT, "limit must be a positive integer")
    return min(limit, cap)


MAX_OFFSET = 100_000


def _json_size(row: dict[str, Any]) -> int:
    return len(json.dumps(row, ensure_ascii=False, default=str).encode("utf-8")) + 1


def run_query(
    settings: Settings,
    policy: AccessPolicy,
    sql: str,
    params: list[Any] | None,
    limit: int | None,
    *,
    database: str | None = None,
    offset: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run one isolated SELECT bounded by rows, serialized bytes, and time.

    dry_run validates the SQL and resolves table access (which needs
    metadata) without executing the statement.
    """
    selected = policy.check_database(database)
    if offset is not None and (
        isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset <= MAX_OFFSET
    ):
        raise BizDbError(INVALID_ARGUMENT, f"offset must be an integer between 0 and {MAX_OFFSET}")
    row_limit = _query_limit(settings, limit, selected.id)
    max_bytes = settings.limits.query.max_bytes
    timeout = settings.effective_query_timeout(selected.id)
    with _db_errors(), _connection(
        settings, selected.id, timeout_seconds=timeout, streaming=True
    ) as conn:
        # limit + 1 rows tells a full page apart from a truncated one.
        rewritten, args = rewrite_select(
            sql,
            params,
            default_schema=selected.schema,
            table_filter=_resolver(settings, policy, selected.id, conn),
            row_limit=row_limit + 1,
            offset=offset or 0,
        )
        if dry_run:
            return {
                "dry_run": True,
                "database": selected.id,
                "effective_limit": row_limit,
                "effective_timeout": timeout,
                "offset": offset or 0,
            }
        rows: list[dict[str, Any]] = []
        size = 0
        truncated_reason: str | None = None
        with conn.cursor() as cursor:
            cursor.execute(rewritten, args)
            for row in cursor:
                if len(rows) >= row_limit:
                    truncated_reason = "row_limit"
                    break
                row_size = _json_size(row)
                if size + row_size > max_bytes:
                    truncated_reason = "byte_limit"
                    break
                size += row_size
                rows.append(row)
    return {
        "database": selected.id,
        "row_count": len(rows),
        "offset": offset or 0,
        "limit": row_limit,
        "truncated": truncated_reason is not None,
        "truncated_reason": truncated_reason,
        "rows": rows,
    }


def explain_query(
    settings: Settings,
    policy: AccessPolicy,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """EXPLAIN the isolated form of a SELECT; the plan reflects the real filters."""
    selected = policy.check_database(database)
    timeout = settings.effective_query_timeout(selected.id)
    with _db_errors(), _connection(settings, selected.id, timeout_seconds=timeout) as conn:
        rewritten, args = rewrite_select(
            sql,
            params,
            default_schema=selected.schema,
            table_filter=_resolver(settings, policy, selected.id, conn),
        )
        with conn.cursor() as cursor:
            cursor.execute(f"EXPLAIN {rewritten}", args)
            return list(cursor.fetchall())


@dataclass
class ExportSource:
    """Rows of one isolated export plus the column metadata to type the file."""

    rows: Iterator[dict[str, Any]]
    columns: list[Any]


@contextmanager
def export_rows(
    settings: Settings,
    policy: AccessPolicy,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
) -> Iterator[ExportSource]:
    """Stream an isolated SELECT, bounded server-side to max_rows + 1 rows."""
    from .export import column_meta_from_cursor

    selected = policy.check_database(database)
    timeout = settings.effective_export_timeout(selected.id)
    max_rows = settings.effective_export_max_rows(selected.id)
    with _db_errors(), _connection(
        settings, selected.id, timeout_seconds=timeout, streaming=True
    ) as conn:
        rewritten, args = rewrite_select(
            sql,
            params,
            default_schema=selected.schema,
            table_filter=_resolver(settings, policy, selected.id, conn),
            row_limit=max_rows + 1,
        )
        with conn.cursor() as cursor:
            cursor.execute(rewritten, args)
            yield ExportSource(rows=iter(cursor), columns=column_meta_from_cursor(cursor))


@contextmanager
def import_connection(settings: Settings, database_id: str) -> Iterator[Any]:
    """A non-autocommit connection with the restricted import account."""
    with _connection(
        settings,
        database_id,
        timeout_seconds=settings.limits.import_.timeout_seconds,
        autocommit=False,
        purpose="import",
    ) as conn:
        yield conn


# ---------------------------------------------------------------------------
# Dev-only generic DML (registered only for stdio with writes.enabled)
# ---------------------------------------------------------------------------


def _extract_dml_table(sql: str) -> str | None:
    """Extract single-table target for UPDATE/DELETE, or None for multi-table/complex DML.

    Returns the table name only for simple single-table statements.  Multi-table
    forms like ``UPDATE t JOIN ...`` or ``DELETE t1, t2 FROM ...`` return None
    so the WHERE-exempt allowlist is not applied.
    """
    try:
        import sqlparse
        from sqlparse.tokens import Keyword
        parsed = sqlparse.parse(sql)
        if not parsed:
            return None
        stmt = parsed[0]
        # Quick reject: JOIN or comma in the statement generally means multi-table
        normalized = " ".join(t.value for t in stmt.flatten()).upper()
        # Use regex-free check: look for JOIN keyword or comma before SET/WHERE
        tokens = [t for t in stmt.tokens if not t.is_whitespace]
        if not tokens:
            return None
        first = (tokens[0].value if tokens else "").upper()
        if first == "UPDATE":
            # Find SET keyword position
            set_idx = None
            for idx, tok in enumerate(tokens):
                if tok.ttype is Keyword and tok.value.upper() == "SET":
                    set_idx = idx
                    break
                # sqlparse may classify SET as DML, check string
                if tok.value.upper() == "SET":
                    set_idx = idx
                    break
            if set_idx is None or set_idx < 2:
                return None
            # Everything between table and SET must be a single identifier (no JOIN, comma)
            between = tokens[1:set_idx]
            if len(between) != 1:
                return None
            # If the single token contains JOIN or comma, it's multi-table
            raw = between[0].value.strip()
            if "," in raw or "JOIN" in raw.upper():
                return None
            # Also check normalized contains JOIN before SET
            set_pos = normalized.find(" SET ")
            before_set = normalized[:set_pos] if set_pos != -1 else ""
            if " JOIN " in before_set or "," in before_set:
                return None
            cand = raw.split()[0].strip("`")
            return cand.split(".")[-1] if cand else None
        if first == "DELETE":
            # DELETE has forms: DELETE FROM tbl WHERE ... (single) vs DELETE t1, t2 FROM ... (multi)
            # Find FROM
            from_idx = None
            for idx, tok in enumerate(tokens):
                if tok.value.upper() == "FROM":
                    from_idx = idx
                    break
            if from_idx is None:
                return None
            # If there's anything between DELETE and FROM besides optional whitespace, it's multi-target
            # e.g. DELETE t1, t2 FROM ... -> tokens[1] would be "t1, t2"
            if from_idx != 1:
                # tokens[1] exists and is not FROM -> multi-table delete
                return None
            if from_idx + 1 >= len(tokens):
                return None
            cand_token = tokens[from_idx + 1]
            raw = cand_token.value.strip()
            if "," in raw or "JOIN" in raw.upper():
                return None
            # Check normalized after FROM before WHERE for JOIN/comma (multi-table)
            from_pos = normalized.find(" FROM ")
            where_pos = normalized.find(" WHERE ")
            after_from = normalized[from_pos:where_pos if where_pos != -1 else len(normalized)]
            if " JOIN " in after_from:
                return None
            if "," in after_from.replace(" FROM ", "", 1):
                return None
            cand = raw.split()[0].strip("`")
            return cand.split(".")[-1] if cand else None
    except Exception:
        pass
    return None


def _resolve_write_require_where(settings: Settings, sql: str, database: str | None) -> bool:
    base = settings.effective_write_require_where(database)
    if not base:
        return False
    # Only single-table DML may use the exempt allowlist; multi-table DML always requires WHERE
    table = _extract_dml_table(sql)
    if table and settings.is_write_where_exempt(table):
        return False
    return True


def execute_write(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute one guarded DML statement in a transaction.

    When dry_run=True, runs EXPLAIN (if supported) or a ROLLBACK transaction
    to return the would-be affected_rows without committing.
    """
    if not settings.write_enabled:
        raise QueryError("write operations are disabled (set BIZDB_WRITE_ENABLED=true)")
    selected = settings.database(database)
    if not selected.allow_writes:
        raise QueryError(f"writes are disabled for database {selected.id!r}")

    require_where = _resolve_write_require_where(settings, sql, database)
    assert_write_only(sql, require_where=require_where)
    max_rows = settings.effective_write_max_affected_rows(selected.id)
    if dry_run:
        # dry_run must never mutate data. Use EXPLAIN only; do not fall back to
        # executing the DML and rolling back (unsafe for MyISAM / triggers).
        try:
            timeout = settings.effective_query_timeout(selected.id)
            with _connection(settings, selected.id, timeout_seconds=timeout, purpose="write") as conn:
                with conn.cursor() as cursor:
                    # EXPLAIN on DML works on MySQL 8.0.19+; on older versions this raises
                    cursor.execute(f"EXPLAIN {sql}", params or [])
                    plan = list(cursor.fetchall())
                    return {
                        "database": selected.id,
                        "dry_run": True,
                        "estimated_plan": plan,
                        "max_affected_rows": max_rows,
                        "require_where": require_where,
                    }
        except Exception as exc:
            raise QueryError(
                f"dry_run not supported for this statement (EXPLAIN failed): {exc}"
            ) from exc
    with _connection(
        settings,
        selected.id,
        timeout_seconds=settings.write_timeout_seconds,
        autocommit=False,
        purpose="write",
    ) as conn:
        with conn.cursor() as cursor:
            conn.begin()
            cursor.execute(sql, params or [])
            affected_rows = cursor.rowcount
            if affected_rows >= 0 and affected_rows > max_rows:
                conn.rollback()
                raise QueryError(
                    f"write affects {affected_rows} rows, exceeding "
                    f"limit {max_rows} (effective_write_max_affected_rows)"
                )
            conn.commit()
            return {
                "database": selected.id,
                "affected_rows": affected_rows,
                "last_insert_id": cursor.lastrowid,
            }


def execute_many(
    settings: Settings,
    statements: list[dict[str, Any]],
    *,
    database: str | None = None,
    atomic: bool = True,
) -> dict[str, Any]:
    """Execute multiple DML statements in a single transaction.

    Each item is {sql: str, params?: list}. All are validated before any execution.
    When atomic=True (default), either all commit or all rollback.
    """
    if not settings.write_enabled:
        raise QueryError("write operations are disabled (set BIZDB_WRITE_ENABLED=true)")
    selected = settings.database(database)
    if not selected.allow_writes:
        raise QueryError(f"writes are disabled for database {selected.id!r}")
    if not statements or not isinstance(statements, list):
        raise QueryError("statements must be a non-empty list")
    if len(statements) > 100:
        raise QueryError("too many statements (max 100 per call)")
    # Pre-validate all
    for idx, item in enumerate(statements):
        if not isinstance(item, dict) or "sql" not in item:
            raise QueryError(f"statements[{idx}] must be {{sql, params}}")
        sql = item["sql"]
        if not isinstance(sql, str) or not sql.strip():
            raise QueryError(f"statements[{idx}].sql must be a non-empty string")
        require_where = _resolve_write_require_where(settings, sql, selected.id)
        assert_write_only(sql, require_where=require_where)
    max_rows = settings.effective_write_max_affected_rows(selected.id)
    total_affected = 0
    results: list[dict[str, Any]] = []
    with _connection(
        settings,
        selected.id,
        timeout_seconds=settings.write_timeout_seconds,
        autocommit=False,
        purpose="write",
    ) as conn:
        with conn.cursor() as cursor:
            conn.begin()
            try:
                for item in statements:
                    cursor.execute(item["sql"], item.get("params") or [])
                    affected = cursor.rowcount
                    if affected >= 0:
                        total_affected += affected
                        if total_affected > max_rows:
                            # Safety limit breach must always rollback, even when atomic=False
                            conn.rollback()
                            raise QueryError(
                                f"batch write affects {total_affected} rows, exceeding limit {max_rows}"
                            )
                    results.append({
                        "affected_rows": affected,
                        "last_insert_id": cursor.lastrowid,
                    })
                conn.commit()
            except QueryError:
                # QueryError from limit breach already rolled back above; ensure other
                # QueryErrors also rollback when atomic, but limit breach is always rollback
                # Check if we already rolled back (total_affected > max_rows case)
                # For limit breach we already rolled back, don't double-commit
                try:
                    # If we are here due to limit breach, the connection is already rolled back
                    # For other QueryErrors, respect atomic flag
                    if total_affected > max_rows:
                        # already rolled back
                        pass
                    elif atomic:
                        conn.rollback()
                    else:
                        conn.commit()
                except Exception:
                    pass
                raise
            except Exception:
                if atomic:
                    conn.rollback()
                else:
                    conn.commit()
                raise
    return {
        "database": selected.id,
        "total_affected_rows": total_affected,
        "statement_count": len(results),
        "results": results,
    }
