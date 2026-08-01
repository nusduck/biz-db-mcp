import json

import pytest

from biz_db_mcp.config import ConfigError, load_export_identity, load_settings


def _base_env(monkeypatch):
    monkeypatch.setenv("BIZDB_MYSQL_DSN", "mysql://user:password@localhost:3306/app")
    monkeypatch.setenv("BIZDB_SANDBOX_BASE_URL", "http://sandbox:8000/")
    monkeypatch.setenv("BIZDB_SANDBOX_API_TOKEN", "token")


def test_legacy_single_database_is_read_only_by_default(monkeypatch):
    _base_env(monkeypatch)

    settings = load_settings()

    assert settings.default_database == "primary"
    assert settings.database().dsn.endswith("/app")
    assert settings.write_enabled is False
    assert settings.database().allow_writes is False


def test_connections_json_and_explicit_write_switch(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("BIZDB_MYSQL_DSN", raising=False)
    monkeypatch.setenv(
        "BIZDB_DATABASES_JSON",
        json.dumps(
            {
                "connections": [
                    {"id": "primary", "dsn": "mysql://u:p@db/app"},
                    {"id": "writer", "dsn": "mysql://u:p@db/app", "allow_writes": True},
                ]
            }
        ),
    )
    monkeypatch.setenv("BIZDB_DEFAULT_DATABASE", "writer")
    monkeypatch.setenv("BIZDB_WRITE_ENABLED", "true")

    settings = load_settings()

    assert settings.database().id == "writer"
    assert settings.database("writer").allow_writes is True


def test_invalid_numeric_guardrail_is_rejected(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("BIZDB_QUERY_MAX_ROWS", "0")

    with pytest.raises(ConfigError, match="BIZDB_QUERY_MAX_ROWS"):
        load_settings()


def test_dbpm_requires_a_complete_provider_configuration(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("BIZDB_DBPM_ENABLED", "true")

    with pytest.raises(ConfigError, match="BIZDB_DBPM_COMMAND"):
        load_settings()


def test_export_identity_is_process_scoped(monkeypatch):
    monkeypatch.setenv("BIZDB_SESSION_ID", "session-1")
    monkeypatch.setenv("BIZDB_ORG_ID", "org-1")
    monkeypatch.setenv("BIZDB_USER_ID", "user-1")

    identity = load_export_identity()

    assert identity.session_id == "session-1"
    assert identity.org_id == "org-1"
