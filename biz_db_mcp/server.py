"""MCP delivery layer for the configured MySQL databases."""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from . import db, sandbox_client
from .audit import log_call
from .config import load_export_identity, load_settings
from .export import export_to_parquet


mcp = FastMCP("biz-db-mcp")
_settings = None


def _get_settings():
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


@mcp.tool()
def list_databases() -> list[dict[str, Any]]:
    """List configured database ids without exposing DSNs or credentials."""
    settings = _get_settings()
    with log_call("list_databases"):
        return [
            {
                "id": database.id,
                "default": database.id == settings.default_database,
                "writes_enabled": settings.write_enabled and database.allow_writes,
            }
            for database in settings.databases
        ]


@mcp.tool()
def describe_table(table: str, database: str | None = None) -> list[dict[str, Any]]:
    """Return column metadata for a table. Supports `table` or `schema.table`."""
    with log_call("describe_table", database=database, table=table):
        return db.describe_table(_get_settings(), table, database=database)


@mcp.tool()
def list_tables(
    database: str | None = None,
    schema: str | None = None,
    pattern: str | None = None,
    include_views: bool = True,
) -> list[dict[str, Any]]:
    """List tables/views in a database. Use pattern like 'user%' to filter."""
    with log_call("list_tables", database=database, schema=schema, pattern=pattern):
        return db.list_tables(
            _get_settings(),
            database=database,
            schema=schema,
            pattern=pattern,
            include_views=include_views,
        )


@mcp.tool()
def explain(
    sql: str,
    params: list[Any] | None = None,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """Return MySQL EXPLAIN plan for a SELECT without executing it."""
    with log_call("explain", database=database, sql=sql):
        return db.explain_query(_get_settings(), sql, params, database=database)


@mcp.tool()
def query(
    sql: str,
    params: list[Any] | None = None,
    limit: int | None = None,
    database: str | None = None,
    offset: int | None = None,
    dry_run: bool = False,
) -> Any:
    """Run a bounded read-only SELECT. Use export for bulk pulls.

    Set dry_run=True to validate and return effective limits without hitting DB.
    Use offset for pagination; limit is capped by per-database or global max.
    """
    with log_call("query", database=database, sql=sql, limit=limit, offset=offset, dry_run=dry_run):
        return db.run_query(
            _get_settings(), sql, params, limit, database=database, offset=offset, dry_run=dry_run
        )


@mcp.tool()
def execute(
    sql: str,
    params: list[Any] | None = None,
    database: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute one guarded DML statement when writes are explicitly enabled.

    Set dry_run=True to return EXPLAIN / rollback estimate without committing.
    """
    with log_call("execute", database=database, sql=sql, dry_run=dry_run):
        return db.execute_write(
            _get_settings(), sql, params, database=database, dry_run=dry_run
        )


@mcp.tool()
def execute_many(
    statements: list[dict[str, Any]],
    database: str | None = None,
    atomic: bool = True,
) -> dict[str, Any]:
    """Execute multiple DML statements in one transaction.

    Each item is {sql: str, params?: list}. Max 100 statements; total affected
    rows must stay within the (per-database) write limit. When atomic=True,
    any failure rolls back the whole batch.
    """
    with log_call("execute_many", database=database, atomic=atomic):
        return db.execute_many(_get_settings(), statements, database=database, atomic=atomic)


@mcp.tool()
def export(
    sql: str,
    dataset_name: str,
    params: list[Any] | None = None,
    database: str | None = None,
    format: str = "parquet",
) -> dict[str, Any]:
    """Stream a SELECT to Parquet/CSV/JSONL and register it as a Sandbox Dataset.

    format: parquet (default), csv, or jsonl.
    The destination session and acting identity come from process-level
    environment variables, never from model-supplied tool arguments.
    """
    fmt = format.lower().strip() if isinstance(format, str) else "parquet"
    if fmt not in ("parquet", "csv", "jsonl"):
        raise ValueError(f"unsupported format {fmt!r}; use parquet, csv, or jsonl")
    with log_call("export", database=database, sql=sql, dataset_name=dataset_name, format=fmt):
        sandbox_client._dataset_filename(dataset_name, fmt)  # fail fast, before the export runs
        settings = _get_settings()
        identity = load_export_identity()
        tmp_path, row_count = export_to_parquet(
            settings, sql, params, database=database, format=fmt  # type: ignore[arg-type]
        )
        result = sandbox_client.push_dataset(
            settings,
            identity=identity,
            dataset_name=dataset_name,
            file_path=tmp_path,
            format=fmt,
        )
        result["database"] = settings.database(database).id
        result["row_count"] = row_count
        result["format"] = fmt
        return result


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
