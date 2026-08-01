"""Environment-driven configuration for the database MCP server.

The default configuration is one database and read-only operation. Multiple
databases are supplied as JSON, writes require two explicit switches, and
DBPM is an opt-in external provider because the private Java SDK is not a
Python dependency of this project.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing or invalid."""


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"{name} is required")
    return value


def _optional_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


def _coerce_bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ConfigError(f"{name} must be a boolean")


def _optional_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip()
    return default if not raw else _coerce_bool(raw, name)


_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_DATABASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def _header_name(value: str) -> str:
    if not _HEADER_NAME_RE.fullmatch(value):
        raise ConfigError("BIZDB_SANDBOX_API_TOKEN_HEADER is not a valid HTTP header name")
    return value


def _base_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ConfigError("BIZDB_SANDBOX_BASE_URL must be an http(s) URL")
    return value.rstrip("/")


def _csv(name: str) -> tuple[str, ...]:
    values = tuple(part.strip() for part in _require(name).split(",") if part.strip())
    if not values:
        raise ConfigError(f"{name} must contain at least one value")
    return values


@dataclass(frozen=True)
class DatabaseConfig:
    id: str
    dsn: str
    allow_writes: bool = False
    dbpm_database: str | None = None
    dbpm_user: str | None = None


@dataclass(frozen=True)
class DbpmSettings:
    enabled: bool
    command: str | None
    servers: tuple[str, ...]
    app_name: str | None
    app_key: str | None
    app_ip: str | None
    connect_timeout_ms: int
    read_timeout_ms: int
    command_timeout_seconds: int


@dataclass(frozen=True)
class ExportIdentity:
    """Identity injected into one server process by its trusted caller."""

    session_id: str
    org_id: str
    user_id: str
    conversation_id: str | None = None


@dataclass(frozen=True)
class Settings:
    databases: tuple[DatabaseConfig, ...]
    default_database: str

    write_enabled: bool
    write_require_where: bool
    write_max_affected_rows: int
    write_timeout_seconds: int

    sandbox_base_url: str
    sandbox_api_token: str
    sandbox_api_token_header: str
    sandbox_upload_timeout_seconds: int

    query_max_rows: int
    query_timeout_seconds: int
    export_timeout_seconds: int
    export_max_rows: int
    export_row_batch_size: int

    dbpm: DbpmSettings

    def database(self, database_id: str | None = None) -> DatabaseConfig:
        selected = (database_id or self.default_database).strip()
        for database in self.databases:
            if database.id == selected:
                return database
        available = ", ".join(database.id for database in self.databases)
        raise ConfigError(f"unknown database {selected!r}; available: {available}")


def _database_from_entry(database_id: str, raw: Any) -> DatabaseConfig:
    if not _DATABASE_ID_RE.fullmatch(database_id):
        raise ConfigError(f"invalid database id {database_id!r}")
    if isinstance(raw, str):
        raw = {"dsn": raw}
    if not isinstance(raw, dict):
        raise ConfigError(f"database {database_id!r} must be an object")

    dsn = raw.get("dsn")
    if not isinstance(dsn, str) or not dsn.strip():
        raise ConfigError(f"database {database_id!r} requires a non-empty dsn")

    if "allow_writes" in raw:
        allow_writes = _coerce_bool(raw["allow_writes"], f"{database_id}.allow_writes")
    elif "read_only" in raw:
        allow_writes = not _coerce_bool(raw["read_only"], f"{database_id}.read_only")
    else:
        allow_writes = False

    dbpm_database = raw.get("dbpm_database", raw.get("db_name"))
    dbpm_user = raw.get("dbpm_user", raw.get("db_user"))
    for field_name, field_value in (
        ("dbpm_database", dbpm_database),
        ("dbpm_user", dbpm_user),
    ):
        if field_value is not None and (
            not isinstance(field_value, str) or not field_value.strip()
        ):
            raise ConfigError(f"{database_id}.{field_name} must be a non-empty string")

    return DatabaseConfig(
        id=database_id,
        dsn=dsn.strip(),
        allow_writes=allow_writes,
        dbpm_database=dbpm_database.strip() if isinstance(dbpm_database, str) else None,
        dbpm_user=dbpm_user.strip() if isinstance(dbpm_user, str) else None,
    )


def _load_databases() -> tuple[DatabaseConfig, ...]:
    raw = os.environ.get("BIZDB_DATABASES_JSON", "").strip()
    if not raw:
        raw = os.environ.get("BIZDB_DATABASES", "").strip()

    if not raw:
        return (DatabaseConfig(id="primary", dsn=_require("BIZDB_MYSQL_DSN")),)

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError("BIZDB_DATABASES_JSON must be valid JSON") from exc

    if isinstance(payload, dict) and "connections" in payload:
        payload = payload["connections"]

    entries: list[tuple[str, Any]] = []
    if isinstance(payload, dict):
        entries = [(str(database_id), value) for database_id, value in payload.items()]
    elif isinstance(payload, list):
        for value in payload:
            if not isinstance(value, dict) or not value.get("id"):
                raise ConfigError("each database entry requires an id")
            entries.append((str(value["id"]), value))
    else:
        raise ConfigError("BIZDB_DATABASES_JSON must be an object, list, or connections object")

    databases = tuple(_database_from_entry(database_id, value) for database_id, value in entries)
    if not databases:
        raise ConfigError("at least one database must be configured")
    if len({database.id for database in databases}) != len(databases):
        raise ConfigError("database ids must be unique")
    return databases


def _load_dbpm() -> DbpmSettings:
    enabled = _optional_bool("BIZDB_DBPM_ENABLED", False)
    if not enabled:
        return DbpmSettings(
            enabled=False,
            command=None,
            servers=(),
            app_name=None,
            app_key=None,
            app_ip=None,
            connect_timeout_ms=5_000,
            read_timeout_ms=5_000,
            command_timeout_seconds=10,
        )

    return DbpmSettings(
        enabled=True,
        command=_require("BIZDB_DBPM_COMMAND"),
        servers=_csv("BIZDB_DBPM_SERVERS"),
        app_name=_require("BIZDB_DBPM_APP_NAME"),
        app_key=_require("BIZDB_DBPM_APP_KEY"),
        app_ip=_require("BIZDB_DBPM_APP_IP"),
        connect_timeout_ms=_optional_int("BIZDB_DBPM_CONNECT_TIMEOUT_MS", 5_000),
        read_timeout_ms=_optional_int("BIZDB_DBPM_READ_TIMEOUT_MS", 5_000),
        command_timeout_seconds=_optional_int("BIZDB_DBPM_TIMEOUT_SECONDS", 10),
    )


def load_settings() -> Settings:
    databases = _load_databases()
    default_database = os.environ.get("BIZDB_DEFAULT_DATABASE", databases[0].id).strip()
    if default_database not in {database.id for database in databases}:
        raise ConfigError(f"BIZDB_DEFAULT_DATABASE {default_database!r} is not configured")

    return Settings(
        databases=databases,
        default_database=default_database,
        write_enabled=_optional_bool("BIZDB_WRITE_ENABLED", False),
        write_require_where=_optional_bool("BIZDB_WRITE_REQUIRE_WHERE", True),
        write_max_affected_rows=_optional_int("BIZDB_WRITE_MAX_AFFECTED_ROWS", 1_000),
        write_timeout_seconds=_optional_int("BIZDB_WRITE_TIMEOUT_SECONDS", 30),
        sandbox_base_url=_base_url(_require("BIZDB_SANDBOX_BASE_URL")),
        sandbox_api_token=_require("BIZDB_SANDBOX_API_TOKEN"),
        sandbox_api_token_header=_header_name(
            os.environ.get("BIZDB_SANDBOX_API_TOKEN_HEADER", "X-API-Key").strip()
            or "X-API-Key"
        ),
        sandbox_upload_timeout_seconds=_optional_int(
            "BIZDB_SANDBOX_UPLOAD_TIMEOUT_SECONDS", 120
        ),
        query_max_rows=_optional_int("BIZDB_QUERY_MAX_ROWS", 200),
        query_timeout_seconds=_optional_int("BIZDB_QUERY_TIMEOUT_SECONDS", 10),
        export_timeout_seconds=_optional_int("BIZDB_EXPORT_TIMEOUT_SECONDS", 600),
        export_max_rows=_optional_int("BIZDB_EXPORT_MAX_ROWS", 2_000_000),
        export_row_batch_size=_optional_int("BIZDB_EXPORT_ROW_BATCH_SIZE", 20_000),
        dbpm=_load_dbpm(),
    )


def load_export_identity() -> ExportIdentity:
    """Load the identity that the process is authorized to act for."""
    return ExportIdentity(
        session_id=_require("BIZDB_SESSION_ID"),
        org_id=_require("BIZDB_ORG_ID"),
        user_id=_require("BIZDB_USER_ID"),
        conversation_id=(os.environ.get("BIZDB_CONVERSATION_ID", "").strip() or None),
    )
