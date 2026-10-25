import pytest

from biz_db_mcp.config import ConfigError, auth_token, load_settings, parse_settings
from conftest import BASE_CONFIG, make_settings


def test_minimal_config_is_read_only_stdio():
    settings = make_settings()

    assert settings.default_database == "primary"
    assert settings.server.transport == "stdio"
    assert settings.writes.enabled is False
    assert settings.imports.enabled is False
    assert settings.database().allow_writes is False


def test_unknown_keys_fail_closed():
    with pytest.raises(ConfigError, match="unknown keys: max_row"):
        make_settings({"limits": {"query": {"max_row": 5}}})


def test_dsn_with_password_is_rejected():
    with pytest.raises(ConfigError, match="must not contain a password"):
        make_settings({"databases": {"primary": {"dsn": "mysql://u:secret@db/app"}}})


def test_http_requires_dbpm_and_refuses_generic_writes():
    with pytest.raises(ConfigError, match="dbpm.enabled must be true"):
        make_settings({"server": {"transport": "streamable-http"}})
    with pytest.raises(ConfigError, match="writes.enabled"):
        make_settings({"server": {"transport": "streamable-http"}, "writes": {"enabled": True}})


def test_dbpm_mode_forbids_password_env():
    with pytest.raises(ConfigError, match="password_env is not allowed"):
        make_settings({"dbpm": {"enabled": True, "servers": ["127.0.0.1:7000"]}})


def test_institution_codes_must_be_quoted_and_normalized():
    doc = dict(BASE_CONFIG, institutions={1000: {"scope": "self"}})
    with pytest.raises(ConfigError, match="quoted strings"):
        parse_settings(doc)
    with pytest.raises(ConfigError, match="length prefix"):
        make_settings({"institutions": {"0800001000": {"scope": "self"}}})


def test_export_limit_cannot_exceed_platform_file_result_cap():
    with pytest.raises(ConfigError, match="max_bytes must be <="):
        make_settings({"limits": {"export": {"max_bytes": 20 * 1024 * 1024 + 1}}})


def test_import_table_requires_server_assigned_institution_column():
    with pytest.raises(ConfigError, match="server-assigned"):
        make_settings({
            "imports": {"tables": {"primary:app.t": {
                "modes": ["insert"], "columns": ["a", "ins_id_cd"], "institution_column": "ins_id_cd",
            }}},
        })


def test_load_settings_reads_yaml_and_rejects_legacy_env(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text(
        "server: {transport: stdio}\n"
        "databases:\n  primary: {dsn: 'mysql://u@db/app', password_env: TEST_DB_PASSWORD}\n"
        "institutions:\n  '00002000': {scope: self}\n",
        encoding="utf-8",
    )
    assert load_settings(path).institutions["00002000"].scope == "self"

    monkeypatch.setenv("BIZDB_MYSQL_DSN", "mysql://u:leaked@db/app")
    with pytest.raises(ConfigError) as excinfo:
        load_settings(path)
    assert "BIZDB_MYSQL_DSN" in str(excinfo.value)
    assert "leaked" not in str(excinfo.value)


def test_auth_token_must_be_present(monkeypatch):
    settings = make_settings()
    monkeypatch.delenv("BIZDB_MCP_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="BIZDB_MCP_TOKEN"):
        auth_token(settings)
