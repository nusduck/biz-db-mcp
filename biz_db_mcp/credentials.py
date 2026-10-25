"""Startup credential resolution for the configured databases.

Every credential the server may need is resolved once at startup so a missing
password fails fast instead of surfacing mid-call. Passwords are held in
memory only: they are never written to the environment, disk, or logs, and
the store's ``repr`` shows database ids and purposes only.

Resolution failures name the database id and purpose plus a stable DBPM code,
never a secret.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any, Literal
from urllib.parse import unquote, urlparse

from .config import Credential, DatabaseConfig, DbpmSettings, Settings
from .dbpm import DbpmError, fetch_password

Purpose = Literal["read", "import"]
DEFAULT_MYSQL_PORT = 3306


class CredentialError(RuntimeError):
    """Raised when a configured credential cannot be resolved."""


class CredentialStore:
    """Passwords resolved once, keyed by database id and purpose."""

    def __init__(self, credentials: Mapping[tuple[str, Purpose], dict[str, Any]]) -> None:
        self._credentials = {key: dict(value) for key, value in credentials.items()}

    @classmethod
    def resolve(
        cls,
        settings: Settings,
        *,
        fetch: Callable[[DbpmSettings, str, str], str] = fetch_password,
    ) -> "CredentialStore":
        """Resolve every credential needed for this configuration."""
        resolved: dict[tuple[str, Purpose], dict[str, Any]] = {}
        for database in settings.databases:
            for purpose, credential in _needed_credentials(settings, database):
                password = _resolve_password(settings, database, purpose, credential, fetch)
                resolved[(database.id, purpose)] = _connect_kwargs(
                    database.id, purpose, credential, password
                )
        return cls(resolved)

    def connect_kwargs(self, database_id: str, purpose: Purpose) -> dict[str, Any]:
        """pymysql connection kwargs for a resolved database and purpose."""
        try:
            stored = self._credentials[(database_id, purpose)]
        except KeyError:
            raise CredentialError(
                f"no {purpose} credential resolved for database {database_id!r}"
            ) from None
        return dict(stored)

    def __repr__(self) -> str:
        entries = ", ".join(
            f"{database_id!r}:{purpose}"
            for database_id, purpose in sorted(self._credentials)
        )
        return f"CredentialStore({entries})"


def _needed_credentials(
    settings: Settings,
    database: DatabaseConfig,
) -> tuple[tuple[Purpose, Credential], ...]:
    needed: list[tuple[Purpose, Credential]] = [("read", database.read)]
    needs_import = (
        settings.imports.enabled
        and database.allow_imports
        and database.import_credential is not None
    )
    if needs_import:
        # ``needs_import`` guarantees the credential is present.
        credential = database.import_credential
        assert credential is not None
        needed.append(("import", credential))
    return tuple(needed)


def _resolve_password(
    settings: Settings,
    database: DatabaseConfig,
    purpose: Purpose,
    credential: Credential,
    fetch: Callable[[DbpmSettings, str, str], str],
) -> str:
    if settings.dbpm.enabled:
        db_name, user_name = credential.dbpm_entry()
        try:
            return fetch(settings.dbpm, db_name, user_name)
        except DbpmError as exc:
            raise CredentialError(
                f"cannot resolve {purpose} credential for database {database.id!r}: "
                f"DBPM {exc.code}"
            ) from None
    return _password_from_environment(database.id, purpose, credential)


def _password_from_environment(
    database_id: str,
    purpose: Purpose,
    credential: Credential,
) -> str:
    env_name = credential.password_env
    if not env_name:
        raise CredentialError(
            f"cannot resolve {purpose} credential for database {database_id!r}: "
            "no password source configured"
        )
    password = os.environ.get(env_name, "")
    if not password:
        raise CredentialError(
            f"cannot resolve {purpose} credential for database {database_id!r}: "
            f"environment variable {env_name} is not set"
        )
    return password


def _connect_kwargs(
    database_id: str,
    purpose: Purpose,
    credential: Credential,
    password: str,
) -> dict[str, Any]:
    """Parse the passwordless DSN into pymysql kwargs."""
    try:
        parsed = urlparse(credential.dsn)
        port = parsed.port or DEFAULT_MYSQL_PORT
        host = parsed.hostname
    except ValueError:
        raise CredentialError(
            f"cannot resolve {purpose} credential for database {database_id!r}: invalid DSN"
        ) from None
    user = unquote(parsed.username or "")
    database = unquote((parsed.path or "").lstrip("/"))
    if not host or not user or not database:
        raise CredentialError(
            f"cannot resolve {purpose} credential for database {database_id!r}: invalid DSN"
        )
    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
