"""Tests for startup credential resolution."""

from __future__ import annotations

import pytest

from biz_db_mcp.credentials import CredentialError, CredentialStore
from biz_db_mcp.dbpm import DbpmError
from conftest import make_settings

_DBPM_OVERRIDES = {
    "dbpm": {"enabled": True, "servers": ["127.0.0.1:7000"]},
    "databases": {
        "primary": {
            "dsn": "mysql://reader@db:3306/app",
            "password_env": None,
            "allow_imports": True,
            "import_credential": {
                "dsn": "mysql://loader@db:3306/app",
                "password_env": None,
            },
        }
    },
    "imports": {"enabled": True},
}


class FakeFetch:
    """Stand-in for dbpm.fetch_password that records calls."""

    def __init__(
        self,
        passwords: dict[tuple[str, str], str] | None = None,
        error: DbpmError | None = None,
    ) -> None:
        self.calls: list[tuple[str, str]] = []
        self.passwords = passwords or {}
        self.error = error

    def __call__(self, dbpm_settings, db_name: str, user_name: str) -> str:
        self.calls.append((db_name, user_name))
        if self.error is not None:
            raise self.error
        return self.passwords[(db_name, user_name)]


def test_dbpm_resolves_read_and_import_credentials() -> None:
    fetch = FakeFetch({("app", "reader"): "read-secret", ("app", "loader"): "import-secret"})

    store = CredentialStore.resolve(make_settings(_DBPM_OVERRIDES), fetch=fetch)

    assert fetch.calls == [("app", "reader"), ("app", "loader")]
    assert store.connect_kwargs("primary", "read") == {
        "host": "db",
        "port": 3306,
        "user": "reader",
        "password": "read-secret",
        "database": "app",
    }
    assert store.connect_kwargs("primary", "import") == {
        "host": "db",
        "port": 3306,
        "user": "loader",
        "password": "import-secret",
        "database": "app",
    }


def test_dbpm_entry_overrides_are_used_for_the_lookup() -> None:
    overrides = {
        "dbpm": {"enabled": True, "servers": ["127.0.0.1:7000"]},
        "databases": {
            "primary": {
                "dsn": "mysql://reader@db:3306/app",
                "password_env": None,
                "dbpm_database": "appdb",
                "dbpm_user": "reader-svc",
            }
        },
    }
    fetch = FakeFetch({("appdb", "reader-svc"): "overridden-secret"})

    store = CredentialStore.resolve(make_settings(overrides), fetch=fetch)

    assert fetch.calls == [("appdb", "reader-svc")]
    assert store.connect_kwargs("primary", "read")["password"] == "overridden-secret"


def test_import_credential_is_skipped_when_imports_are_disabled() -> None:
    overrides = {
        **_DBPM_OVERRIDES,
        "imports": {"enabled": False},
    }
    fetch = FakeFetch({("app", "reader"): "read-secret"})

    store = CredentialStore.resolve(make_settings(overrides), fetch=fetch)

    assert fetch.calls == [("app", "reader")]
    with pytest.raises(CredentialError, match="import"):
        store.connect_kwargs("primary", "import")


def test_import_credential_is_skipped_when_database_disallows_imports() -> None:
    overrides = {
        **_DBPM_OVERRIDES,
        "databases": {
            "primary": {
                "dsn": "mysql://reader@db:3306/app",
                "password_env": None,
                "allow_imports": False,
                "import_credential": None,
            }
        },
    }
    fetch = FakeFetch({("app", "reader"): "read-secret"})

    store = CredentialStore.resolve(make_settings(overrides), fetch=fetch)

    assert fetch.calls == [("app", "reader")]
    with pytest.raises(CredentialError, match="import"):
        store.connect_kwargs("primary", "import")


def test_dbpm_failure_names_database_purpose_and_code_without_secret() -> None:
    fetch = FakeFetch(error=DbpmError("ALL_ENDPOINTS_FAILED", ((0, "CONNECT_FAILED"),)))

    with pytest.raises(CredentialError) as info:
        CredentialStore.resolve(make_settings(_DBPM_OVERRIDES), fetch=fetch)

    message = str(info.value)
    assert "primary" in message
    assert "read" in message
    assert "ALL_ENDPOINTS_FAILED" in message
    assert "secret" not in message
    # ``from None`` keeps the original DBPM traceback out of the chain.
    assert info.value.__cause__ is None


def test_env_mode_reads_the_password_from_the_environment() -> None:
    store = CredentialStore.resolve(make_settings())

    kwargs = store.connect_kwargs("primary", "read")

    assert kwargs == {
        "host": "localhost",
        "port": 3306,
        "user": "user",
        "password": "password",
        "database": "app",
    }


def test_env_mode_missing_password_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TEST_DB_PASSWORD")

    with pytest.raises(CredentialError) as info:
        CredentialStore.resolve(make_settings())

    message = str(info.value)
    assert "primary" in message
    assert "read" in message
    assert "TEST_DB_PASSWORD" in message
    assert "password" not in message.replace("TEST_DB_PASSWORD", "")


def test_connect_kwargs_unquotes_user_and_database_and_keeps_port() -> None:
    overrides = {
        "databases": {
            "primary": {
                "dsn": "mysql://re%40der@db:3307/my%20app",
                "password_env": "TEST_DB_PASSWORD",
            }
        }
    }

    store = CredentialStore.resolve(make_settings(overrides))

    assert store.connect_kwargs("primary", "read") == {
        "host": "db",
        "port": 3307,
        "user": "re@der",
        "password": "password",
        "database": "my app",
    }


def test_connect_kwargs_rejects_unknown_database_and_purpose() -> None:
    store = CredentialStore.resolve(make_settings())

    with pytest.raises(CredentialError, match="other"):
        store.connect_kwargs("other", "read")
    with pytest.raises(CredentialError, match="import"):
        store.connect_kwargs("primary", "import")


def test_connect_kwargs_returns_a_copy() -> None:
    store = CredentialStore.resolve(make_settings())

    kwargs = store.connect_kwargs("primary", "read")
    kwargs["password"] = "tampered"

    assert store.connect_kwargs("primary", "read")["password"] == "password"


def test_repr_shows_database_ids_and_purposes_only() -> None:
    fetch = FakeFetch({("app", "reader"): "read-secret", ("app", "loader"): "import-secret"})

    store = CredentialStore.resolve(make_settings(_DBPM_OVERRIDES), fetch=fetch)
    text = repr(store)

    assert "primary" in text
    assert "read" in text
    assert "import" in text
    assert "read-secret" not in text
    assert "import-secret" not in text
