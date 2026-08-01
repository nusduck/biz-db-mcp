# Repository Guidelines

## Project Structure

The installable Python package is `biz_db_mcp/`: `server.py` registers MCP tools, `config.py` loads environment configuration, `db.py` owns MySQL access, `sql_guard.py` validates SQL, `dbpm.py` adapts the private DBPM client through a bridge command, and `export.py`/`sandbox_client.py` handle Parquet Dataset uploads. Unit tests live in `tests/` and should mirror public behavior rather than private implementation details.

## Development Commands

Install the package and test dependencies with:

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest
biz-db-mcp
```

The server uses stdio transport. Do not run integration tests against production databases or Sandbox sessions; use disposable credentials and endpoints.

## Configuration and Security

The default is read-only. `BIZDB_WRITE_ENABLED=true` and a per-database `allow_writes: true` are both required before `execute` can commit DML. `UPDATE` and `DELETE` require `WHERE` by default, and writes are bounded by `BIZDB_WRITE_MAX_AFFECTED_ROWS`.

Use `BIZDB_DATABASES_JSON` for multiple database entries; `BIZDB_MYSQL_DSN` remains the single-database fallback. Never commit DSNs, DBPM keys, tokens, real business data, or passwords. With `BIZDB_DBPM_ENABLED=true`, credentials must be obtained through the configured DBPM bridge; do not add a plaintext fallback. Export identity comes from `BIZDB_SESSION_ID`, `BIZDB_ORG_ID`, and `BIZDB_USER_ID`, not MCP tool arguments.

## Coding and Testing Conventions

Use Python 4-space indentation, type annotations, `snake_case` names, and small functions with explicit error messages. Keep database selection, SQL validation, credential resolution, and transport code separate. Name tests `test_*.py`; cover rejected SQL, configuration boundaries, write guardrails, DBPM failures, streaming exports, and Sandbox cleanup. Audit logs must not contain raw SQL, parameters, credentials, or returned business data.

## Commits and Pull Requests

This checkout has no Git history to establish a convention. Use short imperative subjects such as `db: add bounded write execution`. Pull requests should describe tool-contract or security changes, list test commands, document every new environment variable, and include a migration note when changing MCP arguments.
