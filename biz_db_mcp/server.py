"""MCP delivery layer: tool contracts, admission, deadlines, and transports.

Every tool takes a top-level, required string ``instituteid``. On the platform
it is a host argument: the platform removes any model-supplied value and
injects the caller's institution, and only the platform holds the bearer
token. The server still validates it and refuses unknown institutions.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import decimal
import json
import sys
import time
from collections.abc import Callable
from typing import Annotated, Any

import anyio
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field

from . import db
from .audit import log_call
from .config import ConfigError, Settings, auth_token, load_settings
from .credentials import CredentialStore
from .errors import (
    DEADLINE_EXCEEDED,
    IMPORT_OUTCOME_UNKNOWN,
    INTERNAL_ERROR,
    INVALID_ARGUMENT,
    BizDbError,
)
from .export import export_filename, write_export
from .import_ledger import check_ledger
from .import_rows import ImportRequest, run_import
from .institution import Institution, parse_institution
from .limits import CallGate
from .mcp_files import export_content
from .policy import AccessPolicy

InstituteId = Annotated[
    str,
    Field(description="Caller institution id. Injected by the platform; never chosen by the model."),
]
SqlParams = list[str | int | float | bool | None] | None
DatabaseId = Annotated[str | None, Field(description="Database id from list_databases; default if omitted.")]

INSTRUCTIONS = (
    "Read-only access to business MySQL databases, isolated to the caller's institution. "
    "Use list_databases, list_tables and describe_table to explore, query for small bounded "
    "results, and export for files. Placeholders are %s with params."
)


def _json_default(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_json_default)


class ToolRunner:
    """Runs blocking tool bodies off the event loop under admission and a deadline."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.gate = CallGate(settings.limits)

    async def run(
        self,
        tool: str,
        instituteid: object,
        body: Callable[[AccessPolicy, Institution, float], Any],
        *,
        kind: str = "read",
        **audit: object,
    ) -> Any:
        with log_call(tool, **audit) as record:
            institution = parse_institution(instituteid)
            record["institution"] = institution.code
            policy = AccessPolicy(self.settings, institution)
            deadline_seconds = self.settings.server.tool_deadline_seconds
            deadline = time.monotonic() + deadline_seconds

            def work() -> Any:
                with self.gate.admit(institution.code, kind):
                    return body(policy, institution, deadline)

            try:
                with anyio.fail_after(deadline_seconds):
                    # The worker thread keeps its admission slot until it
                    # really finishes; database timeouts bound that time.
                    return await anyio.to_thread.run_sync(work, abandon_on_cancel=True)
            except TimeoutError:
                if kind == "import":
                    raise BizDbError(
                        IMPORT_OUTCOME_UNKNOWN,
                        "the import did not finish in time; resubmitting the same file is safe",
                    ) from None
                raise BizDbError(DEADLINE_EXCEEDED, "the call exceeded its time limit") from None
            except BizDbError:
                raise
            except ConfigError as exc:
                raise BizDbError(INVALID_ARGUMENT, str(exc)) from None
            except Exception as exc:
                record["error_type"] = type(exc).__name__
                raise BizDbError(INTERNAL_ERROR, "internal error") from None


def _transport_security(settings: Settings) -> TransportSecuritySettings:
    hosts = list(settings.server.allowed_hosts)
    if settings.server.transport == "stdio" or not hosts:
        # Bearer authentication is the boundary; Host checks only add value
        # when the deployed hostnames are known.
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True, allowed_hosts=hosts, allowed_origins=[]
    )


def create_server(settings: Settings) -> FastMCP:
    mcp = FastMCP(
        "biz-db-mcp",
        instructions=INSTRUCTIONS,
        host=settings.server.host,
        port=settings.server.port,
        streamable_http_path="/mcp",
        stateless_http=True,
        transport_security=_transport_security(settings),
    )
    runner = ToolRunner(settings)
    _register_read_tools(mcp, settings, runner)
    _register_export_tool(mcp, settings, runner)
    if settings.imports.enabled:
        _register_import_tool(mcp, settings, runner)
    if settings.writes.enabled:
        _register_dev_write_tools(mcp, settings, runner)
    return mcp


def _register_read_tools(mcp: FastMCP, settings: Settings, runner: ToolRunner) -> None:
    @mcp.tool(structured_output=False)
    async def list_databases(instituteid: InstituteId) -> str:
        """List database ids this institution may read (no DSNs or credentials)."""
        return _text(
            await runner.run(
                "list_databases", instituteid, lambda policy, *_: db.list_databases(settings, policy)
            )
        )

    @mcp.tool(structured_output=False)
    async def list_tables(
        instituteid: InstituteId,
        database: DatabaseId = None,
        pattern: Annotated[str | None, Field(description="MySQL LIKE pattern, e.g. order%")] = None,
    ) -> str:
        """List tables and views this institution may read."""
        return _text(
            await runner.run(
                "list_tables",
                instituteid,
                lambda policy, *_: db.list_tables(settings, policy, database=database, pattern=pattern),
                database=database,
            )
        )

    @mcp.tool(structured_output=False)
    async def describe_table(
        table: Annotated[str, Field(description="table or schema.table")],
        instituteid: InstituteId,
        database: DatabaseId = None,
    ) -> str:
        """Return column metadata for a readable table."""
        return _text(
            await runner.run(
                "describe_table",
                instituteid,
                lambda policy, *_: db.describe_table(settings, policy, table, database=database),
                database=database,
                table=table,
            )
        )

    @mcp.tool(structured_output=False)
    async def query(
        sql: Annotated[str, Field(description="One SELECT (WITH/UNION allowed). No comments.")],
        instituteid: InstituteId,
        params: SqlParams = None,
        limit: Annotated[int | None, Field(description="Rows to return; capped by the server.")] = None,
        offset: Annotated[int | None, Field(description="Rows to skip for pagination.")] = None,
        database: DatabaseId = None,
        dry_run: Annotated[bool, Field(description="Validate and resolve access without running.")] = False,
    ) -> str:
        """Run a bounded read-only SELECT restricted to this institution's rows.

        Results are capped by rows, serialized bytes and time; `truncated` and
        `truncated_reason` say when a cap was hit. Use export for bulk data.
        """
        return _text(
            await runner.run(
                "query",
                instituteid,
                lambda policy, *_: db.run_query(
                    settings,
                    policy,
                    sql,
                    params,
                    limit,
                    database=database,
                    offset=offset,
                    dry_run=dry_run,
                ),
                database=database,
                sql=sql,
                limit=limit,
                offset=offset,
                dry_run=dry_run,
            )
        )

    @mcp.tool(structured_output=False)
    async def explain(
        sql: Annotated[str, Field(description="One SELECT to explain.")],
        instituteid: InstituteId,
        params: SqlParams = None,
        database: DatabaseId = None,
    ) -> str:
        """Return the MySQL EXPLAIN plan of the institution-filtered SELECT."""
        return _text(
            await runner.run(
                "explain",
                instituteid,
                lambda policy, *_: db.explain_query(settings, policy, sql, params, database=database),
                database=database,
                sql=sql,
            )
        )


def _register_export_tool(mcp: FastMCP, settings: Settings, runner: ToolRunner) -> None:
    max_mib = settings.limits.export.max_bytes / (1024 * 1024)

    @mcp.tool(structured_output=False)
    async def export(
        sql: Annotated[str, Field(description="One SELECT whose rows become the file.")],
        dataset_name: Annotated[str, Field(description="File name without directories.")],
        instituteid: InstituteId,
        params: SqlParams = None,
        database: DatabaseId = None,
        format: Annotated[str, Field(description="parquet (default), csv or jsonl")] = "parquet",
    ) -> list[Any]:
        """Export an institution-filtered SELECT as a file returned in the result.

        The file is returned as an embedded resource that the platform saves to
        the workspace. Exports over the row or byte limit fail as TOO_LARGE
        rather than being truncated.
        """
        fmt = format.lower().strip() if isinstance(format, str) else ""
        if fmt not in ("parquet", "csv", "jsonl"):
            raise BizDbError(INVALID_ARGUMENT, "format must be parquet, csv, or jsonl")

        def body(policy: AccessPolicy, _institution: Institution, deadline: float) -> list[Any]:
            filename = export_filename(dataset_name, fmt)  # type: ignore[arg-type]
            selected = policy.check_database(database)
            export_deadline = min(
                deadline, time.monotonic() + settings.effective_export_timeout(selected.id)
            )
            with db.export_rows(settings, policy, sql, params, database=selected.id) as source:
                exported = write_export(
                    source.rows,
                    source.columns,
                    fmt=fmt,  # type: ignore[arg-type]
                    filename=filename,
                    max_rows=settings.effective_export_max_rows(selected.id),
                    max_bytes=settings.limits.export.max_bytes,
                    batch_size=settings.limits.export.row_batch_size,
                    deadline=export_deadline,
                )
            return export_content(exported, database=selected.id)

        return await runner.run(
            "export", instituteid, body, kind="export", database=database, sql=sql, format=fmt
        )

    export.__doc__ = (export.__doc__ or "") + f"\n        Maximum file size: {max_mib:g} MiB."


def _register_import_tool(mcp: FastMCP, settings: Settings, runner: ToolRunner) -> None:
    limit = settings.limits.import_.max_bytes

    @mcp.tool(structured_output=False)
    async def import_rows(
        instituteid: InstituteId,
        database: Annotated[str, Field(description="Database id")],
        table: Annotated[str, Field(description="Whitelisted schema.table")],
        file_base64: Annotated[str, Field(description="Standard base64 file content (platform-filled)")],
        filename: Annotated[str, Field(description="File name (platform-filled)")],
        content_sha256: Annotated[str, Field(description="Lower-case hex SHA-256 of the decoded file")],
        mode: Annotated[str, Field(description="insert or upsert, as allowed for the table")],
        format: Annotated[str | None, Field(description="csv, jsonl or parquet; inferred from filename")] = None,
        dry_run: Annotated[bool, Field(description="Validate only; set false to write")] = True,
    ) -> str:
        """Import a CSV/JSONL/Parquet file into a whitelisted table for this institution.

        Rows are written for the caller's institution only, in one transaction.
        Resubmitting the same file returns status=duplicate instead of writing
        twice. dry_run (default true) validates without writing.
        """

        def body(_policy: AccessPolicy, institution: Institution, deadline: float) -> Any:
            request = ImportRequest(
                institution=institution,
                database=database,
                table=table,
                file_base64=file_base64,
                filename=filename,
                content_sha256=content_sha256,
                format=format,
                mode=mode,
                dry_run=dry_run,
            )
            import_deadline = min(
                deadline, time.monotonic() + settings.limits.import_.timeout_seconds
            )
            # run_import authorizes the database before it ever connects.
            return run_import(
                settings,
                request,
                connect=lambda: db.import_connection(settings, database),
                deadline=import_deadline,
            )

        result = await runner.run(
            "import_rows",
            instituteid,
            body,
            kind="import",
            database=database,
            table=table,
            content_sha256=content_sha256,
            mode=mode,
            dry_run=dry_run,
            file_length=len(file_base64) if isinstance(file_base64, str) else None,
        )
        return _text(_import_payload(result))

    import_rows.__doc__ = (import_rows.__doc__ or "") + (
        f"\n        Maximum decoded file size: {limit} bytes "
        f"(base64 length up to {4 * -(-limit // 3)} characters)."
    )


def _import_payload(result: Any) -> dict[str, Any]:
    return {
        "status": result.status,
        "database": result.database,
        "table": result.table,
        "row_count": result.row_count,
        "inserted": result.inserted,
        "updated": result.updated,
        "batch_id": result.batch_id,
        "columns": list(result.columns),
        "problems": [
            {"row": p.row, "column": p.column, "reason": p.reason} for p in result.problems
        ],
    }


def _register_dev_write_tools(mcp: FastMCP, settings: Settings, runner: ToolRunner) -> None:
    """Generic DML for local stdio development only; never row-isolated."""

    @mcp.tool(structured_output=False)
    async def execute(
        sql: str,
        instituteid: InstituteId,
        params: SqlParams = None,
        database: DatabaseId = None,
        dry_run: bool = False,
    ) -> str:
        """DEV ONLY: execute one guarded DML statement (not institution-isolated)."""
        return _text(
            await runner.run(
                "execute",
                instituteid,
                lambda *_: db.execute_write(settings, sql, params, database=database, dry_run=dry_run),
                database=database,
                sql=sql,
            )
        )

    @mcp.tool(structured_output=False)
    async def execute_many(
        statements: list[dict[str, Any]],
        instituteid: InstituteId,
        database: DatabaseId = None,
        atomic: bool = True,
    ) -> str:
        """DEV ONLY: execute up to 100 DML statements in one transaction."""
        return _text(
            await runner.run(
                "execute_many",
                instituteid,
                lambda *_: db.execute_many(settings, statements, database=database, atomic=atomic),
                database=database,
            )
        )


def _check_import_ledgers(settings: Settings) -> None:
    if not settings.imports.enabled:
        return
    for database in settings.databases:
        if not database.allow_imports:
            continue
        with db.import_connection(settings, database.id) as conn:
            check_ledger(conn, schema=database.schema, ledger_table=database.ledger_table)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="biz-db-mcp")
    parser.add_argument("--config", help="path to config.yaml (default: $BIZDB_CONFIG or ./config.yaml)")
    parser.add_argument("--transport", choices=["stdio", "streamable-http"])
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    return parser.parse_args(argv)


def _with_cli_overrides(settings: Settings, args: argparse.Namespace) -> Settings:
    from dataclasses import replace

    from .config import _check_consistency

    server = replace(
        settings.server,
        transport=args.transport or settings.server.transport,
        host=args.host or settings.server.host,
        port=args.port or settings.server.port,
    )
    updated = replace(settings, server=server)
    _check_consistency(updated)
    return updated


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    try:
        settings = _with_cli_overrides(load_settings(args.config), args)
        token = auth_token(settings) if settings.server.transport == "streamable-http" else None
        db.configure(CredentialStore.resolve(settings))
        _check_import_ledgers(settings)
    except Exception as exc:  # startup must fail closed with a redacted reason
        print(f"biz-db-mcp: startup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2) from None

    mcp = create_server(settings)
    if settings.server.transport == "stdio":
        mcp.run(transport="stdio")
        return

    import uvicorn

    from .transport import build_http_app

    assert token is not None
    app = build_http_app(mcp, token=token, max_request_bytes=settings.server.max_request_bytes)
    uvicorn.run(
        app,
        host=settings.server.host,
        port=settings.server.port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
