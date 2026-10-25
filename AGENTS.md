# Repository Guidelines

## Project Structure

The installable Python package is `biz_db_mcp/`: `server.py` registers MCP tools and transports, `transport.py` enforces HTTP bearer auth, `config.py` loads `config.yaml`, `institution.py` and `policy.py` resolve institution scope and table policy, `sql_guard.py` validates and rewrites SQL for row isolation, `db.py` owns MySQL access, `credentials.py`/`dbpm.py` resolve passwords over the DBPM TCP protocol at startup, `export.py`/`mcp_files.py` return export files as MCP embedded resources, and `import_rows.py`/`import_ledger.py` implement the whitelisted import. `migrations/` holds DDL the DBA applies; the service never runs DDL. Unit tests live in `tests/` and should mirror public behavior rather than private implementation details.

## Development Commands

Install the package and test dependencies with:

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest
biz-db-mcp
```

Production uses `--transport streamable-http` (endpoint `/mcp`); stdio is for local development. Do not run integration tests against production databases or Sandbox sessions; use disposable credentials and endpoints.

## Configuration and Security

All trusted configuration lives in `config.yaml` (see `config.example.yaml`); unknown keys are rejected and legacy `BIZDB_*` variables refuse startup. The file never holds secrets: the bearer token comes from the variable named by `server.auth_token_env`, and database passwords come only from DBPM. `password_env` is allowed solely for local stdio with DBPM disabled; do not add any other plaintext fallback.

Every tool takes a top-level required string `instituteid`, injected by the platform; normalize it with `institution.parse_institution` and refuse unknown institutions. Never widen access on missing or unknown values. Reads are row-isolated by rewriting each base table into an institution-filtered derived table; cross-institution denials and "not found" must look identical. Generic DML (`execute`, `execute_many`) is a stdio-only development aid and is refused over HTTP. `import_rows` is off by default and writes only whitelisted tables, with the institution column assigned by the server.

Never commit DSNs with passwords, DBPM entries for real systems, tokens, real business data, or `config.yaml`. Export identity and workspace placement belong to the platform, not this service.

## Coding and Testing Conventions

Use Python 4-space indentation, type annotations, `snake_case` names, and small functions with explicit error messages. Keep database selection, SQL validation, credential resolution, and transport code separate. Name tests `test_*.py`; cover rejected SQL, isolation rewrites, configuration boundaries, institution normalization, DBPM failures, export limits, import validation and idempotency, and HTTP authentication. Audit logs and error messages must not contain raw SQL, parameters, credentials, file contents, or returned business data. Every new configuration key must be documented in `config.example.yaml` and the README.

## Commits and Pull Requests

This checkout has no Git history to establish a convention. Use short imperative subjects such as `db: add bounded write execution`. Pull requests should describe tool-contract or security changes, list test commands, document every new environment variable, and include a migration note when changing MCP arguments.
