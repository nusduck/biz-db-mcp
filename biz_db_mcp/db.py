"""Database access for multiple configured MySQL databases."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache
from typing import Any
from urllib.parse import unquote, urlparse

import pymysql
import pymysql.cursors

from .config import DatabaseConfig, DbpmSettings, Settings
from .dbpm import CommandPasswordProvider, PasswordProvider
from .sql_guard import assert_select_only, assert_write_only


class QueryError(RuntimeError):
    """Raised when a query fails or violates a guardrail."""


def _parse_url(dsn: str):
    try:
        parsed = urlparse(dsn)
        port = parsed.port or 3306
    except ValueError as exc:
        raise QueryError("invalid MySQL DSN") from exc
    if parsed.scheme not in ("mysql", "mysql+pymysql"):
        raise QueryError(f"unsupported DSN scheme: {parsed.scheme!r}")
    if not parsed.hostname or not parsed.username:
        raise QueryError("MySQL DSN must include user and host")
    database = unquote((parsed.path or "").lstrip("/"))
    if not database:
        raise QueryError("MySQL DSN must include a database name")
    return parsed, port, database


def _parse_dsn(dsn: str, *, password_override: str | None = None) -> dict[str, Any]:
    """Parse mysql://user:pass@host:port/db into pymysql kwargs."""
    parsed, port, database = _parse_url(dsn)
    password = password_override
    if password is None and parsed.password is not None:
        password = unquote(parsed.password)
    if password is None:
        raise QueryError("MySQL DSN requires a password when DBPM is disabled")
    return {
        "host": parsed.hostname,
        "port": port,
        "user": unquote(parsed.username or ""),
        "password": password,
        "database": database,
    }


def _dbpm_identity(database: DatabaseConfig) -> tuple[str, str]:
    parsed, _, dsn_database = _parse_url(database.dsn)
    return (
        database.dbpm_database or dsn_database,
        database.dbpm_user or unquote(parsed.username or ""),
    )


@lru_cache(maxsize=1)
def _dbpm_provider(settings: DbpmSettings) -> PasswordProvider:
    return CommandPasswordProvider(settings)


def _connection_kwargs(settings: Settings, database: DatabaseConfig) -> dict[str, Any]:
    password_override: str | None = None
    if settings.dbpm.enabled:
        db_name, db_user = _dbpm_identity(database)
        password_override = _dbpm_provider(settings.dbpm).get_password(db_name, db_user)
    return _parse_dsn(database.dsn, password_override=password_override)


@contextmanager
def _connection(
    settings: Settings,
    database_id: str | None,
    *,
    timeout_seconds: int,
    streaming: bool = False,
    autocommit: bool = True,
) -> Iterator[Any]:
    database = settings.database(database_id)
    conn = pymysql.connect(
        **_connection_kwargs(settings, database),
        cursorclass=(
            pymysql.cursors.SSDictCursor if streaming else pymysql.cursors.DictCursor
        ),
        read_timeout=timeout_seconds,
        connect_timeout=10,
        autocommit=autocommit,
    )
    try:
        with conn.cursor() as cursor:
            # This is a server-side bound for SELECT statements. The client
            # read timeout remains an independent connection safeguard.
            cursor.execute(f"SET SESSION MAX_EXECUTION_TIME={timeout_seconds * 1000}")
        yield conn
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


_IDENT_RE = __import__('re').compile(r"^[A-Za-z0-9_]+$")


def _validate_identifier(name: str) -> str:
    if not name or not _IDENT_RE.fullmatch(name):
        raise QueryError(f"invalid identifier {name!r}")
    return name


def _quoted_table(table: str) -> str:
    """Validate and quote a possibly qualified table name `schema.table`.

    Allows `table` or `schema.table`, each part must be [A-Za-z0-9_].
    Returns a backtick-quoted SQL fragment like `"`schema`"."`table`"`.
    """
    if not table or not isinstance(table, str):
        raise QueryError("invalid table name")
    raw = table.strip().strip("`").strip()
    if not raw:
        raise QueryError("invalid table name")
    parts = raw.split(".")
    if len(parts) > 2:
        raise QueryError(f"invalid table name {table!r}: expected [schema.]table")
    quoted = ".".join(f"`{_validate_identifier(p.strip().strip('`'))}`" for p in parts)
    return quoted


def describe_table(
    settings: Settings, table: str, *, database: str | None = None
) -> list[dict[str, Any]]:
    quoted = _quoted_table(table)
    timeout = settings.effective_query_timeout(database)
    with _connection(settings, database, timeout_seconds=timeout) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {quoted}")
            return list(cursor.fetchall())


def list_tables(
    settings: Settings,
    *,
    database: str | None = None,
    schema: str | None = None,
    pattern: str | None = None,
    include_views: bool = True,
) -> list[dict[str, Any]]:
    """List tables/views visible in the current MySQL database.

    Uses INFORMATION_SCHEMA so it works across MySQL versions without
    relying on SHOW TABLES parsing. Filtering is done in SQL via LIKE.
    """
    timeout = settings.effective_query_timeout(database)
    # Resolve default schema from DSN when not given
    if schema is not None:
        _validate_identifier(schema)
        schema_filter = schema
    else:
        _, _, dsn_db = _parse_url(settings.database(database).dsn)
        schema_filter = dsn_db

    sql = (
        "SELECT TABLE_NAME as name, TABLE_TYPE as type, ENGINE as engine, "
        "TABLE_ROWS as approx_rows, CREATE_TIME as create_time, TABLE_COMMENT as comment "
        "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s"
    )
    params: list[Any] = [schema_filter]
    if not include_views:
        sql += " AND TABLE_TYPE = 'BASE TABLE'"
    if pattern:
        # Validate pattern is safe for LIKE: allow % _ and alnum
        if len(pattern) > 64 or any(c in pattern for c in ("'", '"', ";", "\\")):
            raise QueryError("invalid pattern")
        sql += " AND TABLE_NAME LIKE %s"
        params.append(pattern)
    sql += " ORDER BY TABLE_NAME LIMIT 1000"
    with _connection(settings, database, timeout_seconds=timeout) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())


def explain_query(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """Return MySQL EXPLAIN output for a SELECT without executing it fully.

    Only SELECT/WITH is allowed; the EXPLAIN itself is bounded by the query timeout.
    """
    assert_select_only(sql)
    timeout = settings.effective_query_timeout(database)
    with _connection(settings, database, timeout_seconds=timeout) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"EXPLAIN {sql}", params or [])
            return list(cursor.fetchall())


def _query_limit(settings: Settings, limit: int | None, database: str | None = None) -> int:
    cap = settings.effective_query_max_rows(database)
    if limit is None:
        return cap
    if isinstance(limit, bool) or limit < 1:
        raise QueryError("limit must be a positive integer")
    return min(limit, cap)


def _extract_dml_table(sql: str) -> str | None:
    """Best-effort table name extraction for UPDATE/DELETE to support WHERE-exempt allowlist."""
    try:
        import sqlparse
        parsed = sqlparse.parse(sql)
        if not parsed:
            return None
        stmt = parsed[0]
        tokens = [t for t in stmt.tokens if not t.is_whitespace]
        # UPDATE <table> SET ...  or DELETE FROM <table> WHERE ...
        first = (tokens[0].value if tokens else "").upper()
        if first == "UPDATE" and len(tokens) >= 2:
            # second token is table (may be `db.table`)
            cand = tokens[1].value.strip().split()[0].strip("`")
            return cand.split(".")[-1]
        if first == "DELETE":
            for i, tok in enumerate(tokens):
                if tok.value.upper() == "FROM" and i + 1 < len(tokens):
                    cand = tokens[i + 1].value.strip().split()[0].strip("`")
                    return cand.split(".")[-1]
    except Exception:
        pass
    return None


def _resolve_write_require_where(settings: Settings, sql: str, database: str | None) -> bool:
    base = settings.effective_write_require_where(database)
    if not base:
        return False
    # check allowlist
    table = _extract_dml_table(sql)
    if table and settings.is_write_where_exempt(table):
        return False
    return True


def run_query(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    limit: int | None,
    *,
    database: str | None = None,
    offset: int | None = None,
    dry_run: bool = False,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Run a bounded exploratory query using a server-side cursor.

    When dry_run=True, returns {dry_run: True, sql, params, effective_limit, effective_timeout}
    without hitting the database, useful for preflight.
    """
    assert_select_only(sql)
    row_limit = _query_limit(settings, limit, database)
    timeout = settings.effective_query_timeout(database)
    if dry_run:
        return {
            "dry_run": True,
            "sql": sql,
            "params": params or [],
            "effective_limit": row_limit,
            "effective_timeout": timeout,
            "offset": offset or 0,
            "database": settings.database(database).id,
        }
    if offset is not None:
        if isinstance(offset, bool) or offset < 0:
            raise QueryError("offset must be a non-negative integer")
    # DictCursor buffers the complete result during execute(). Streaming here
    # makes the output limit a client-memory bound as well as a response bound.
    with _connection(
        settings,
        database,
        timeout_seconds=timeout,
        streaming=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            rows: list[dict[str, Any]] = []
            skipped = 0
            target_offset = offset or 0
            for row in cursor:
                if skipped < target_offset:
                    skipped += 1
                    continue
                rows.append(row)
                if len(rows) >= row_limit:
                    break
            return rows


def stream_query(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
    on_description: Any = None,
) -> Iterator[dict[str, Any]]:
    """Yield rows one at a time via a server-side cursor.

    If given, *on_description* is called once with the DB-API cursor
    description (column name/type metadata) right after the query executes
    and before the first row is yielded, so callers can derive a fixed
    output schema instead of inferring one from streamed row batches.
    """
    assert_select_only(sql)
    with _connection(
        settings,
        database,
        timeout_seconds=settings.effective_export_timeout(database),
        streaming=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            if on_description is not None:
                on_description(cursor.description)
            yield from cursor


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
        # Try EXPLAIN first for a cheap estimate, fallback to rollback execution
        try:
            timeout = settings.effective_query_timeout(selected.id)
            with _connection(settings, selected.id, timeout_seconds=timeout) as conn:
                with conn.cursor() as cursor:
                    # EXPLAIN on DML works on MySQL 8.0.19+
                    cursor.execute(f"EXPLAIN {sql}", params or [])
                    plan = list(cursor.fetchall())
                    return {
                        "database": selected.id,
                        "dry_run": True,
                        "estimated_plan": plan,
                        "max_affected_rows": max_rows,
                        "require_where": require_where,
                    }
        except Exception:
            pass
        with _connection(
            settings,
            selected.id,
            timeout_seconds=settings.write_timeout_seconds,
            autocommit=False,
        ) as conn:
            with conn.cursor() as cursor:
                conn.begin()
                cursor.execute(sql, params or [])
                affected_rows = cursor.rowcount
                conn.rollback()
                return {
                    "database": selected.id,
                    "dry_run": True,
                    "affected_rows": affected_rows,
                    "would_exceed_limit": affected_rows >= 0 and affected_rows > max_rows,
                    "max_affected_rows": max_rows,
                }
    with _connection(
        settings,
        selected.id,
        timeout_seconds=settings.write_timeout_seconds,
        autocommit=False,
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
                            raise QueryError(
                                f"batch write affects {total_affected} rows, exceeding limit {max_rows}"
                            )
                    results.append({
                        "affected_rows": affected,
                        "last_insert_id": cursor.lastrowid,
                    })
                conn.commit()
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
