"""YAML configuration for the database MCP server.

All trusted configuration lives in one ``config.yaml`` that is loaded once at
startup; changes take effect on restart. The file names environment variables
for secrets but never holds secrets itself: the MCP bearer token comes from
``server.auth_token_env`` and database passwords come from DBPM (or, for
local stdio development only, from a ``password_env`` variable).

Every mapping is checked for unknown keys so a typo fails closed instead of
silently falling back to a default.
"""

from __future__ import annotations

import fnmatch
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import yaml

DEFAULT_CONFIG_PATH = "config.yaml"
CONFIG_PATH_ENV = "BIZDB_CONFIG"

MIB = 1024 * 1024
# Platform hard limits (pi-enterprise-sandbox docs/mcp-server-guide.md §6).
PLATFORM_FILE_RESULT_MAX_BYTES = 20 * MIB
PLATFORM_ARTIFACT_MAX_BYTES = 10 * MIB

# Environment variables from the previous env-driven configuration. Their
# values are never echoed; we only name them so operators can migrate.
_LEGACY_ENV = (
    "BIZDB_MYSQL_DSN",
    "BIZDB_DATABASES_JSON",
    "BIZDB_DATABASES",
    "BIZDB_DATABASES_FILE",
    "BIZDB_DEFAULT_DATABASE",
    "BIZDB_WRITE_ENABLED",
    "BIZDB_DBPM_ENABLED",
    "BIZDB_DBPM_COMMAND",
    "BIZDB_DBPM_SERVERS",
    "BIZDB_DBPM_APP_NAME",
    "BIZDB_DBPM_APP_KEY",
    "BIZDB_DBPM_APP_IP",
    "BIZDB_SANDBOX_BASE_URL",
    "BIZDB_SANDBOX_API_TOKEN",
    "BIZDB_SANDBOX_API_TOKEN_HEADER",
    "BIZDB_SANDBOX_UPLOAD_TIMEOUT_SECONDS",
    "BIZDB_SESSION_ID",
    "BIZDB_ORG_ID",
    "BIZDB_USER_ID",
    "BIZDB_CONVERSATION_ID",
)


class ConfigError(RuntimeError):
    """Raised when the configuration file is missing or invalid."""


_DATABASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_$]{1,64}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_KEY_RE = re.compile(r"^([^:]+):([^.]+)\.([^.]+)$")


# ---------------------------------------------------------------------------
# Settings dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ServerSettings:
    transport: str = "stdio"
    host: str = "127.0.0.1"
    port: int = 8080
    auth_token_env: str = "BIZDB_MCP_TOKEN"
    # Host header allowlist for HTTP deployments behind a proxy or gateway.
    allowed_hosts: tuple[str, ...] = ()
    max_request_bytes: int = 16 * MIB
    tool_deadline_seconds: int = 150


@dataclass(frozen=True)
class QueryLimits:
    max_rows: int = 200
    max_bytes: int = 1 * MIB
    timeout_seconds: int = 10


@dataclass(frozen=True)
class ExportLimits:
    max_rows: int = 100_000
    max_bytes: int = 20 * MIB
    timeout_seconds: int = 120
    row_batch_size: int = 20_000


@dataclass(frozen=True)
class ImportLimits:
    max_bytes: int = 10 * MIB
    max_rows: int = 50_000
    max_columns: int = 256
    max_field_bytes: int = 64 * 1024
    timeout_seconds: int = 120


@dataclass(frozen=True)
class ConcurrencyLimits:
    total: int = 4
    per_institution: int = 2
    export: int = 1
    import_: int = 1


@dataclass(frozen=True)
class RateLimits:
    per_minute: int = 60
    burst: int = 10


@dataclass(frozen=True)
class Limits:
    query: QueryLimits = field(default_factory=QueryLimits)
    export: ExportLimits = field(default_factory=ExportLimits)
    import_: ImportLimits = field(default_factory=ImportLimits)
    concurrency: ConcurrencyLimits = field(default_factory=ConcurrencyLimits)
    rate: RateLimits = field(default_factory=RateLimits)


@dataclass(frozen=True)
class DbpmEndpoint:
    host: str
    port: int


@dataclass(frozen=True)
class DbpmSettings:
    enabled: bool = False
    servers: tuple[DbpmEndpoint, ...] = ()
    connect_timeout_ms: int = 3_000
    endpoint_timeout_ms: int = 5_000
    budget_ms: int = 10_000


@dataclass(frozen=True)
class Credential:
    """A passwordless DSN plus where its password comes from."""

    dsn: str
    dbpm_database: str | None = None
    dbpm_user: str | None = None
    # Local stdio development only, when DBPM is disabled.
    password_env: str | None = None

    @property
    def user(self) -> str:
        return unquote(urlparse(self.dsn).username or "")

    @property
    def schema(self) -> str:
        return unquote((urlparse(self.dsn).path or "").lstrip("/"))

    def dbpm_entry(self) -> tuple[str, str]:
        return (self.dbpm_database or self.schema, self.dbpm_user or self.user)


@dataclass(frozen=True)
class DatabaseConfig:
    id: str
    read: Credential
    import_credential: Credential | None = None
    allow_reads: bool = True
    allow_writes: bool = False
    allow_imports: bool = False
    ledger_table: str = "bizdb_import_ledger"
    # Per-database overrides (None = inherit the global limit).
    query_max_rows: int | None = None
    query_timeout_seconds: int | None = None
    export_max_rows: int | None = None
    export_timeout_seconds: int | None = None
    write_max_affected_rows: int | None = None
    write_require_where: bool | None = None

    @property
    def dsn(self) -> str:
        return self.read.dsn

    @property
    def schema(self) -> str:
        return self.read.schema


@dataclass(frozen=True)
class InstitutionScope:
    """Read scope and import permission for one normalized institution code."""

    scope: str  # "self" | "institutions" | "all"
    ids: tuple[str, ...] = ()
    allow_imports: bool = False


@dataclass(frozen=True)
class TablePolicy:
    """Explicit isolation for one table; None column means a public table."""

    column: str | None
    value_format: str = "code"  # "code" | "raw"

    @property
    def public(self) -> bool:
        return self.column is None


@dataclass(frozen=True)
class IsolationSettings:
    column_candidates: tuple[str, ...] = ("ins_id_cd",)
    # Keys are canonical "database:schema.table" (lower case).
    tables: dict[str, TablePolicy] = field(default_factory=dict)


@dataclass(frozen=True)
class ImportTable:
    database: str
    schema: str
    table: str
    modes: tuple[str, ...]
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    institution_column: str
    value_format: str = "code"
    max_rows: int | None = None

    @property
    def key(self) -> str:
        return table_key(self.database, self.schema, self.table)


@dataclass(frozen=True)
class ImportSettings:
    enabled: bool = False
    tables: dict[str, ImportTable] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteSettings:
    """Generic model-authored DML: local stdio development only."""

    enabled: bool = False
    require_where: bool = True
    require_where_except_tables: tuple[str, ...] = ()
    max_affected_rows: int = 1_000
    timeout_seconds: int = 30


@dataclass(frozen=True)
class Settings:
    databases: tuple[DatabaseConfig, ...]
    default_database: str
    server: ServerSettings = field(default_factory=ServerSettings)
    limits: Limits = field(default_factory=Limits)
    dbpm: DbpmSettings = field(default_factory=DbpmSettings)
    reads_enabled: bool = True
    writes: WriteSettings = field(default_factory=WriteSettings)
    imports: ImportSettings = field(default_factory=ImportSettings)
    institutions: dict[str, InstitutionScope] = field(default_factory=dict)
    isolation: IsolationSettings = field(default_factory=IsolationSettings)

    def database(self, database_id: str | None = None) -> DatabaseConfig:
        selected = (database_id or self.default_database).strip()
        for database in self.databases:
            if database.id == selected:
                return database
        available = ", ".join(database.id for database in self.databases)
        raise ConfigError(f"unknown database {selected!r}; available: {available}")

    # ---- per-database effective values ----
    def effective_query_max_rows(self, database_id: str | None = None) -> int:
        db = self.database(database_id)
        return db.query_max_rows if db.query_max_rows is not None else self.limits.query.max_rows

    def effective_query_timeout(self, database_id: str | None = None) -> int:
        db = self.database(database_id)
        if db.query_timeout_seconds is not None:
            return db.query_timeout_seconds
        return self.limits.query.timeout_seconds

    def effective_export_max_rows(self, database_id: str | None = None) -> int:
        db = self.database(database_id)
        return db.export_max_rows if db.export_max_rows is not None else self.limits.export.max_rows

    def effective_export_timeout(self, database_id: str | None = None) -> int:
        db = self.database(database_id)
        if db.export_timeout_seconds is not None:
            return db.export_timeout_seconds
        return self.limits.export.timeout_seconds

    def effective_write_max_affected_rows(self, database_id: str | None = None) -> int:
        db = self.database(database_id)
        if db.write_max_affected_rows is not None:
            return db.write_max_affected_rows
        return self.writes.max_affected_rows

    def effective_write_require_where(self, database_id: str | None = None) -> bool:
        db = self.database(database_id)
        if db.write_require_where is not None:
            return db.write_require_where
        return self.writes.require_where

    def is_write_where_exempt(self, table: str) -> bool:
        if not table:
            return False
        name = table.strip().lower()
        return any(
            fnmatch.fnmatch(name, pattern.lower())
            for pattern in self.writes.require_where_except_tables
        )

    @property
    def write_enabled(self) -> bool:
        return self.writes.enabled

    @property
    def write_timeout_seconds(self) -> int:
        return self.writes.timeout_seconds

    @property
    def export_row_batch_size(self) -> int:
        return self.limits.export.row_batch_size


def table_key(database: str, schema: str, table: str) -> str:
    """Canonical policy key for one table: ``database:schema.table``."""
    return f"{database}:{schema.lower()}.{table.lower()}"


# ---------------------------------------------------------------------------
# Typed field readers
# ---------------------------------------------------------------------------


def _mapping(raw: Any, where: str, allowed: set[str]) -> dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping")
    unknown = sorted(str(key) for key in raw if key not in allowed)
    if unknown:
        raise ConfigError(f"{where} has unknown keys: {', '.join(unknown)}")
    return raw


def _bool(raw: dict[str, Any], key: str, where: str, default: bool) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{where}.{key} must be true or false")
    return value


def _int(
    raw: dict[str, Any],
    key: str,
    where: str,
    default: int | None,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int | None:
    value = raw.get(key, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{where}.{key} must be an integer")
    if value < minimum:
        raise ConfigError(f"{where}.{key} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"{where}.{key} must be <= {maximum}, got {value}")
    return value


def _req_int(raw: dict[str, Any], key: str, where: str, default: int, **kw: Any) -> int:
    value = _int(raw, key, where, default, **kw)
    assert value is not None
    return value


def _str(raw: dict[str, Any], key: str, where: str, default: str | None = None) -> str | None:
    value = raw.get(key, default)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{where}.{key} must be a non-empty string")
    return value.strip()


def _str_list(raw: dict[str, Any], key: str, where: str) -> tuple[str, ...]:
    value = raw.get(key, [])
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in value):
        raise ConfigError(f"{where}.{key} must be a list of non-empty strings")
    return tuple(v.strip() for v in value)


def _identifier(value: str, where: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ConfigError(f"{where} is not a valid identifier")
    return value


def _choice(raw: dict[str, Any], key: str, where: str, default: str, choices: set[str]) -> str:
    value = raw.get(key, default)
    if value not in choices:
        raise ConfigError(f"{where}.{key} must be one of {', '.join(sorted(choices))}")
    return value


# ---------------------------------------------------------------------------
# Section parsers
# ---------------------------------------------------------------------------


def _parse_server(raw: Any) -> ServerSettings:
    where = "server"
    raw = _mapping(
        raw,
        where,
        {
            "transport",
            "host",
            "port",
            "auth_token_env",
            "allowed_hosts",
            "max_request_bytes",
            "tool_deadline_seconds",
        },
    )
    token_env = _str(raw, "auth_token_env", where, "BIZDB_MCP_TOKEN")
    assert token_env is not None
    if not _ENV_NAME_RE.fullmatch(token_env):
        raise ConfigError(f"{where}.auth_token_env must be an environment variable name")
    return ServerSettings(
        transport=_choice(raw, "transport", where, "stdio", {"stdio", "streamable-http"}),
        host=_str(raw, "host", where, "127.0.0.1") or "127.0.0.1",
        port=_req_int(raw, "port", where, 8080, maximum=65535),
        auth_token_env=token_env,
        allowed_hosts=_str_list(raw, "allowed_hosts", where),
        max_request_bytes=_req_int(raw, "max_request_bytes", where, 16 * MIB),
        tool_deadline_seconds=_req_int(raw, "tool_deadline_seconds", where, 150),
    )


def _parse_limits(raw: Any) -> Limits:
    raw = _mapping(raw, "limits", {"query", "export", "import", "concurrency", "rate"})

    q = _mapping(raw.get("query"), "limits.query", {"max_rows", "max_bytes", "timeout_seconds"})
    query = QueryLimits(
        max_rows=_req_int(q, "max_rows", "limits.query", 200),
        max_bytes=_req_int(q, "max_bytes", "limits.query", 1 * MIB),
        timeout_seconds=_req_int(q, "timeout_seconds", "limits.query", 10),
    )

    e = _mapping(
        raw.get("export"),
        "limits.export",
        {"max_rows", "max_bytes", "timeout_seconds", "row_batch_size"},
    )
    export = ExportLimits(
        max_rows=_req_int(e, "max_rows", "limits.export", 100_000),
        max_bytes=_req_int(
            e, "max_bytes", "limits.export", 20 * MIB, maximum=PLATFORM_FILE_RESULT_MAX_BYTES
        ),
        timeout_seconds=_req_int(e, "timeout_seconds", "limits.export", 120),
        row_batch_size=_req_int(e, "row_batch_size", "limits.export", 20_000),
    )

    i = _mapping(
        raw.get("import"),
        "limits.import",
        {"max_bytes", "max_rows", "max_columns", "max_field_bytes", "timeout_seconds"},
    )
    import_ = ImportLimits(
        max_bytes=_req_int(
            i, "max_bytes", "limits.import", 10 * MIB, maximum=PLATFORM_ARTIFACT_MAX_BYTES
        ),
        max_rows=_req_int(i, "max_rows", "limits.import", 50_000),
        max_columns=_req_int(i, "max_columns", "limits.import", 256),
        max_field_bytes=_req_int(i, "max_field_bytes", "limits.import", 64 * 1024),
        timeout_seconds=_req_int(i, "timeout_seconds", "limits.import", 120),
    )

    c = _mapping(
        raw.get("concurrency"),
        "limits.concurrency",
        {"total", "per_institution", "export", "import"},
    )
    concurrency = ConcurrencyLimits(
        total=_req_int(c, "total", "limits.concurrency", 4),
        per_institution=_req_int(c, "per_institution", "limits.concurrency", 2),
        export=_req_int(c, "export", "limits.concurrency", 1),
        import_=_req_int(c, "import", "limits.concurrency", 1),
    )

    r = _mapping(raw.get("rate"), "limits.rate", {"per_minute", "burst"})
    rate = RateLimits(
        per_minute=_req_int(r, "per_minute", "limits.rate", 60),
        burst=_req_int(r, "burst", "limits.rate", 10),
    )
    return Limits(query=query, export=export, import_=import_, concurrency=concurrency, rate=rate)


def _parse_endpoint(value: Any, where: str) -> DbpmEndpoint:
    if not isinstance(value, str) or ":" not in value:
        raise ConfigError(f"{where} must be 'host:port'")
    host, _, port_text = value.strip().rpartition(":")
    if not host or any(ch.isspace() for ch in host) or not port_text.isdigit():
        raise ConfigError(f"{where} must be 'host:port'")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise ConfigError(f"{where} port is out of range")
    return DbpmEndpoint(host=host, port=port)


def _parse_dbpm(raw: Any) -> DbpmSettings:
    where = "dbpm"
    raw = _mapping(
        raw,
        where,
        {"enabled", "servers", "connect_timeout_ms", "endpoint_timeout_ms", "budget_ms"},
    )
    enabled = _bool(raw, "enabled", where, False)
    servers_raw = raw.get("servers", [])
    if not isinstance(servers_raw, list):
        raise ConfigError(f"{where}.servers must be a list of 'host:port'")
    servers = tuple(
        _parse_endpoint(value, f"{where}.servers[{idx}]") for idx, value in enumerate(servers_raw)
    )
    if enabled and not servers:
        raise ConfigError(f"{where}.servers is required when DBPM is enabled")
    return DbpmSettings(
        enabled=enabled,
        servers=servers,
        connect_timeout_ms=_req_int(raw, "connect_timeout_ms", where, 3_000),
        endpoint_timeout_ms=_req_int(raw, "endpoint_timeout_ms", where, 5_000),
        budget_ms=_req_int(raw, "budget_ms", where, 10_000),
    )


def _parse_credential(raw: Any, where: str) -> Credential:
    raw = _mapping(raw, where, {"dsn", "dbpm_database", "dbpm_user", "password_env"})
    dsn = _str(raw, "dsn", where)
    if dsn is None:
        raise ConfigError(f"{where}.dsn is required")
    try:
        parsed = urlparse(dsn)
        parsed.port  # noqa: B018 - validates the port component
    except ValueError as exc:
        raise ConfigError(f"{where}.dsn is not a valid URL") from exc
    if parsed.scheme not in ("mysql", "mysql+pymysql"):
        raise ConfigError(f"{where}.dsn must use the mysql:// scheme")
    if not parsed.hostname or not parsed.username:
        raise ConfigError(f"{where}.dsn must include user and host")
    if parsed.password is not None:
        raise ConfigError(f"{where}.dsn must not contain a password; use DBPM")
    if not (parsed.path or "").lstrip("/"):
        raise ConfigError(f"{where}.dsn must include a database name")
    password_env = _str(raw, "password_env", where)
    if password_env is not None and not _ENV_NAME_RE.fullmatch(password_env):
        raise ConfigError(f"{where}.password_env must be an environment variable name")
    return Credential(
        dsn=dsn,
        dbpm_database=_str(raw, "dbpm_database", where),
        dbpm_user=_str(raw, "dbpm_user", where),
        password_env=password_env,
    )


_DATABASE_KEYS = {
    "dsn",
    "dbpm_database",
    "dbpm_user",
    "password_env",
    "default",
    "allow_reads",
    "allow_writes",
    "allow_imports",
    "import_credential",
    "ledger_table",
    "query_max_rows",
    "query_timeout_seconds",
    "export_max_rows",
    "export_timeout_seconds",
    "write_max_affected_rows",
    "write_require_where",
}


def _parse_databases(raw: Any) -> tuple[tuple[DatabaseConfig, ...], str]:
    if not isinstance(raw, dict) or not raw:
        raise ConfigError("databases must be a non-empty mapping of id -> settings")
    databases: list[DatabaseConfig] = []
    defaults: list[str] = []
    for database_id, entry in raw.items():
        database_id = str(database_id)
        where = f"databases.{database_id}"
        if not _DATABASE_ID_RE.fullmatch(database_id):
            raise ConfigError(f"invalid database id {database_id!r}")
        entry = _mapping(entry, where, _DATABASE_KEYS)
        read = _parse_credential(
            {k: entry[k] for k in ("dsn", "dbpm_database", "dbpm_user", "password_env") if k in entry},
            where,
        )
        import_credential = (
            _parse_credential(entry["import_credential"], f"{where}.import_credential")
            if entry.get("import_credential") is not None
            else None
        )
        if import_credential is not None and import_credential.schema != read.schema:
            raise ConfigError(f"{where}.import_credential must target the same database name")
        ledger = _identifier(
            _str(entry, "ledger_table", where, "bizdb_import_ledger") or "", f"{where}.ledger_table"
        )
        if _bool(entry, "default", where, False):
            defaults.append(database_id)
        databases.append(
            DatabaseConfig(
                id=database_id,
                read=read,
                import_credential=import_credential,
                allow_reads=_bool(entry, "allow_reads", where, True),
                allow_writes=_bool(entry, "allow_writes", where, False),
                allow_imports=_bool(entry, "allow_imports", where, False),
                ledger_table=ledger,
                query_max_rows=_int(entry, "query_max_rows", where, None),
                query_timeout_seconds=_int(entry, "query_timeout_seconds", where, None),
                export_max_rows=_int(entry, "export_max_rows", where, None),
                export_timeout_seconds=_int(entry, "export_timeout_seconds", where, None),
                write_max_affected_rows=_int(entry, "write_max_affected_rows", where, None),
                write_require_where=(
                    _bool(entry, "write_require_where", where, True)
                    if "write_require_where" in entry
                    else None
                ),
            )
        )
    if len(defaults) > 1:
        raise ConfigError("only one database may set default: true")
    return tuple(databases), defaults[0] if defaults else databases[0].id


def _parse_writes(raw: Any) -> WriteSettings:
    where = "writes"
    raw = _mapping(
        raw,
        where,
        {
            "enabled",
            "require_where",
            "require_where_except_tables",
            "max_affected_rows",
            "timeout_seconds",
        },
    )
    return WriteSettings(
        enabled=_bool(raw, "enabled", where, False),
        require_where=_bool(raw, "require_where", where, True),
        require_where_except_tables=_str_list(raw, "require_where_except_tables", where),
        max_affected_rows=_req_int(raw, "max_affected_rows", where, 1_000),
        timeout_seconds=_req_int(raw, "timeout_seconds", where, 30),
    )


def _parse_institutions(raw: Any) -> dict[str, InstitutionScope]:
    from .institution import institute_code_of

    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("institutions must be a mapping of institution code -> scope")
    scopes: dict[str, InstitutionScope] = {}
    for key, entry in raw.items():
        if not isinstance(key, str) or not key.strip():
            # YAML turns unquoted 00001000 into an int and drops leading zeros.
            raise ConfigError("institution codes must be quoted strings, e.g. \"00001000\"")
        code = key.strip()
        where = f"institutions.{code}"
        if institute_code_of(code) != code:
            raise ConfigError(
                f"{where} looks like a raw id with a length prefix; use the normalized code"
            )
        if code in scopes:
            raise ConfigError(f"{where} is duplicated")
        entry = _mapping(entry, where, {"scope", "ids", "allow_imports"})
        scope = _choice(entry, "scope", where, "self", {"self", "institutions", "all"})
        ids = _str_list(entry, "ids", where)
        if scope == "institutions" and not ids:
            raise ConfigError(f"{where}.ids is required for scope: institutions")
        if scope != "institutions" and ids:
            raise ConfigError(f"{where}.ids is only valid for scope: institutions")
        for other in ids:
            if institute_code_of(other) != other:
                raise ConfigError(f"{where}.ids must contain normalized codes")
        scopes[code] = InstitutionScope(
            scope=scope,
            ids=tuple(dict.fromkeys(ids)),
            allow_imports=_bool(entry, "allow_imports", where, False),
        )
    return scopes


def _split_table_key(key: Any, where: str, database_ids: set[str]) -> tuple[str, str, str]:
    if not isinstance(key, str):
        raise ConfigError(f"{where} keys must be 'database:schema.table'")
    match = _TABLE_KEY_RE.fullmatch(key.strip())
    if not match:
        raise ConfigError(f"{where} key {key!r} must be 'database:schema.table'")
    database, schema, table = match.groups()
    if database not in database_ids:
        raise ConfigError(f"{where} key {key!r} names an unknown database")
    _identifier(schema, f"{where} key {key!r} schema")
    _identifier(table, f"{where} key {key!r} table")
    return database, schema, table


def _parse_isolation(raw: Any, database_ids: set[str]) -> IsolationSettings:
    where = "isolation"
    raw = _mapping(raw, where, {"column_candidates", "public_tables", "tables"})
    candidates = _str_list(raw, "column_candidates", where) or ("ins_id_cd",)
    for column in candidates:
        _identifier(column, f"{where}.column_candidates")
    tables: dict[str, TablePolicy] = {}
    for key in _str_list(raw, "public_tables", where):
        tables[table_key(*_split_table_key(key, f"{where}.public_tables", database_ids))] = (
            TablePolicy(column=None)
        )
    explicit = raw.get("tables") or {}
    if not isinstance(explicit, dict):
        raise ConfigError(f"{where}.tables must be a mapping")
    for key, entry in explicit.items():
        canonical = table_key(*_split_table_key(key, f"{where}.tables", database_ids))
        entry_where = f"{where}.tables.{key}"
        entry = _mapping(entry, entry_where, {"column", "value_format"})
        column = _str(entry, "column", entry_where)
        if column is None:
            raise ConfigError(f"{entry_where}.column is required (use public_tables for public)")
        if canonical in tables:
            raise ConfigError(f"{entry_where} is configured more than once")
        tables[canonical] = TablePolicy(
            column=_identifier(column, f"{entry_where}.column"),
            value_format=_choice(entry, "value_format", entry_where, "code", {"code", "raw"}),
        )
    return IsolationSettings(column_candidates=candidates, tables=tables)


def _parse_imports(raw: Any, database_ids: set[str]) -> ImportSettings:
    where = "imports"
    raw = _mapping(raw, where, {"enabled", "tables"})
    tables_raw = raw.get("tables") or {}
    if not isinstance(tables_raw, dict):
        raise ConfigError(f"{where}.tables must be a mapping")
    tables: dict[str, ImportTable] = {}
    for key, entry in tables_raw.items():
        database, schema, table = _split_table_key(key, f"{where}.tables", database_ids)
        entry_where = f"{where}.tables.{key}"
        entry = _mapping(
            entry,
            entry_where,
            {"modes", "columns", "key_columns", "institution_column", "value_format", "max_rows"},
        )
        modes = _str_list(entry, "modes", entry_where)
        if not modes or not set(modes) <= {"insert", "upsert"}:
            raise ConfigError(f"{entry_where}.modes must list insert and/or upsert")
        columns = _str_list(entry, "columns", entry_where)
        if not columns:
            raise ConfigError(f"{entry_where}.columns is required")
        for column in columns:
            _identifier(column, f"{entry_where}.columns")
        if len({c.lower() for c in columns}) != len(columns):
            raise ConfigError(f"{entry_where}.columns contains duplicates")
        key_columns = _str_list(entry, "key_columns", entry_where)
        if "upsert" in modes and not key_columns:
            raise ConfigError(f"{entry_where}.key_columns is required for upsert")
        lower_columns = {c.lower() for c in columns}
        if not {c.lower() for c in key_columns} <= lower_columns:
            raise ConfigError(f"{entry_where}.key_columns must be a subset of columns")
        institution_column = _str(entry, "institution_column", entry_where)
        if institution_column is None:
            raise ConfigError(f"{entry_where}.institution_column is required")
        _identifier(institution_column, f"{entry_where}.institution_column")
        if institution_column.lower() in lower_columns:
            raise ConfigError(
                f"{entry_where}.institution_column is server-assigned; do not list it in columns"
            )
        spec = ImportTable(
            database=database,
            schema=schema,
            table=table,
            modes=tuple(dict.fromkeys(modes)),
            columns=columns,
            key_columns=key_columns,
            institution_column=institution_column,
            value_format=_choice(entry, "value_format", entry_where, "code", {"code", "raw"}),
            max_rows=_int(entry, "max_rows", entry_where, None),
        )
        if spec.key in tables:
            raise ConfigError(f"{entry_where} is configured more than once")
        tables[spec.key] = spec
    return ImportSettings(enabled=_bool(raw, "enabled", where, False), tables=tables)


# ---------------------------------------------------------------------------
# Loading and cross-section checks
# ---------------------------------------------------------------------------


_TOP_LEVEL_KEYS = {
    "server",
    "limits",
    "dbpm",
    "databases",
    "reads",
    "writes",
    "imports",
    "institutions",
    "isolation",
}


def parse_settings(document: Any) -> Settings:
    """Build validated Settings from an already-parsed YAML document."""
    document = _mapping(document, "config", _TOP_LEVEL_KEYS)
    databases, default_database = _parse_databases(document.get("databases"))
    database_ids = {database.id for database in databases}
    reads = _mapping(document.get("reads"), "reads", {"enabled"})
    settings = Settings(
        databases=databases,
        default_database=default_database,
        server=_parse_server(document.get("server")),
        limits=_parse_limits(document.get("limits")),
        dbpm=_parse_dbpm(document.get("dbpm")),
        reads_enabled=_bool(reads, "enabled", "reads", True),
        writes=_parse_writes(document.get("writes")),
        imports=_parse_imports(document.get("imports"), database_ids),
        institutions=_parse_institutions(document.get("institutions")),
        isolation=_parse_isolation(document.get("isolation"), database_ids),
    )
    _check_consistency(settings)
    return settings


def _check_consistency(settings: Settings) -> None:
    http = settings.server.transport == "streamable-http"
    if http and settings.writes.enabled:
        raise ConfigError(
            "writes.enabled is a local stdio development switch and is refused over HTTP"
        )
    for database in settings.databases:
        for label, credential in (("", database.read), (".import_credential", database.import_credential)):
            if credential is None:
                continue
            where = f"databases.{database.id}{label}"
            if settings.dbpm.enabled and credential.password_env:
                raise ConfigError(f"{where}.password_env is not allowed when DBPM is enabled")
            if not settings.dbpm.enabled:
                if http:
                    raise ConfigError("dbpm.enabled must be true for streamable-http deployments")
                if not credential.password_env:
                    raise ConfigError(f"{where}.password_env is required when DBPM is disabled")
        if database.allow_imports and database.import_credential is None:
            raise ConfigError(
                f"databases.{database.id}.allow_imports requires a separate import_credential"
            )
    if settings.limits.query.timeout_seconds >= settings.server.tool_deadline_seconds:
        raise ConfigError("limits.query.timeout_seconds must be below server.tool_deadline_seconds")
    if settings.limits.export.timeout_seconds >= settings.server.tool_deadline_seconds:
        raise ConfigError("limits.export.timeout_seconds must be below server.tool_deadline_seconds")
    if settings.limits.import_.timeout_seconds >= settings.server.tool_deadline_seconds:
        raise ConfigError("limits.import.timeout_seconds must be below server.tool_deadline_seconds")
    for spec in settings.imports.tables.values():
        database = settings.database(spec.database)
        if settings.imports.enabled and not database.allow_imports:
            # A whitelisted table on a database that does not allow imports is
            # harmless but almost always a mistake; make it explicit.
            raise ConfigError(
                f"imports.tables.{spec.key} targets database {database.id!r} without allow_imports"
            )


def legacy_environment() -> list[str]:
    """Names (never values) of set environment variables that no longer apply."""
    return [name for name in _LEGACY_ENV if os.environ.get(name, "").strip()]


def load_settings(path: str | os.PathLike[str] | None = None) -> Settings:
    """Load ``config.yaml`` from *path*, ``$BIZDB_CONFIG``, or the working directory."""
    legacy = legacy_environment()
    if legacy:
        raise ConfigError(
            "configuration moved to config.yaml; unset legacy environment variables: "
            + ", ".join(legacy)
        )
    resolved = Path(path or os.environ.get(CONFIG_PATH_ENV, "").strip() or DEFAULT_CONFIG_PATH)
    try:
        text = resolved.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {resolved}") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read config file {resolved}: {exc.strerror}") from exc
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file {resolved} is not valid YAML") from exc
    return parse_settings(document)


def auth_token(settings: Settings) -> str:
    """The HTTP bearer token; refuses to return an empty value."""
    token = os.environ.get(settings.server.auth_token_env, "")
    if not token.strip():
        raise ConfigError(f"{settings.server.auth_token_env} must be set for streamable-http")
    return token
