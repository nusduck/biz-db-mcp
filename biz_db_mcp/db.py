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


def describe_table(
    settings: Settings, table: str, *, database: str | None = None
) -> list[dict[str, Any]]:
    if not table or not table.replace("_", "").isalnum():
        raise QueryError("invalid table name")
    with _connection(settings, database, timeout_seconds=settings.query_timeout_seconds) as conn:
        with conn.cursor() as cursor:
            # `table` is allowlist-validated above; identifiers cannot be bound
            # as MySQL parameters.
            cursor.execute(f"DESCRIBE `{table}`")
            return list(cursor.fetchall())


def _query_limit(settings: Settings, limit: int | None) -> int:
    if limit is None:
        return settings.query_max_rows
    if isinstance(limit, bool) or limit < 1:
        raise QueryError("limit must be a positive integer")
    return min(limit, settings.query_max_rows)


def run_query(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    limit: int | None,
    *,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """Run a bounded exploratory query using a server-side cursor."""
    assert_select_only(sql)
    row_limit = _query_limit(settings, limit)

    # DictCursor buffers the complete result during execute(). Streaming here
    # makes the output limit a client-memory bound as well as a response bound.
    with _connection(
        settings,
        database,
        timeout_seconds=settings.query_timeout_seconds,
        streaming=True,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql, params or [])
            rows: list[dict[str, Any]] = []
            for row in cursor:
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
        timeout_seconds=settings.export_timeout_seconds,
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
) -> dict[str, Any]:
    """Execute one guarded DML statement in a transaction."""
    if not settings.write_enabled:
        raise QueryError("write operations are disabled (set BIZDB_WRITE_ENABLED=true)")
    selected = settings.database(database)
    if not selected.allow_writes:
        raise QueryError(f"writes are disabled for database {selected.id!r}")

    assert_write_only(sql, require_where=settings.write_require_where)
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
            if (
                affected_rows >= 0
                and affected_rows > settings.write_max_affected_rows
            ):
                conn.rollback()
                raise QueryError(
                    f"write affects {affected_rows} rows, exceeding "
                    f"BIZDB_WRITE_MAX_AFFECTED_ROWS={settings.write_max_affected_rows}"
                )
            conn.commit()
            return {
                "database": selected.id,
                "affected_rows": affected_rows,
                "last_insert_id": cursor.lastrowid,
            }
