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
    """Return column metadata for a table in one configured database."""
    with log_call("describe_table", database=database, table=table):
        return db.describe_table(_get_settings(), table, database=database)


@mcp.tool()
def query(
    sql: str,
    params: list[Any] | None = None,
    limit: int | None = None,
    database: str | None = None,
) -> list[dict[str, Any]]:
    """Run a bounded read-only SELECT. Use export for bulk pulls."""
    with log_call("query", database=database, sql=sql, limit=limit):
        return db.run_query(
            _get_settings(), sql, params, limit, database=database
        )


@mcp.tool()
def execute(
    sql: str,
    params: list[Any] | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    """Execute one guarded DML statement when writes are explicitly enabled."""
    with log_call("execute", database=database, sql=sql):
        return db.execute_write(
            _get_settings(), sql, params, database=database
        )


@mcp.tool()
def export(
    sql: str,
    dataset_name: str,
    params: list[Any] | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    """Stream a SELECT to Parquet and register it as a Sandbox Dataset.

    The destination session and acting identity come from process-level
    environment variables, never from model-supplied tool arguments.
    """
    with log_call("export", database=database, sql=sql, dataset_name=dataset_name):
        sandbox_client._dataset_filename(dataset_name)  # fail fast, before the export runs
        settings = _get_settings()
        identity = load_export_identity()
        tmp_path, row_count = export_to_parquet(
            settings, sql, params, database=database
        )
        result = sandbox_client.push_dataset(
            settings,
            identity=identity,
            dataset_name=dataset_name,
            file_path=tmp_path,
        )
        result["database"] = settings.database(database).id
        result["row_count"] = row_count
        return result


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
