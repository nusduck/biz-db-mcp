import copy
from typing import Any

import pytest

from biz_db_mcp.config import Settings, parse_settings

BASE_CONFIG: dict[str, Any] = {
    "server": {"transport": "stdio"},
    "dbpm": {"enabled": False},
    "databases": {
        "primary": {"dsn": "mysql://user@localhost:3306/app", "password_env": "TEST_DB_PASSWORD"},
    },
    "institutions": {
        "00001000": {"scope": "all"},
        "00002000": {"scope": "self", "allow_imports": True},
        "00003000": {"scope": "institutions", "ids": ["00003000", "00003001"]},
    },
}


def _merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def make_settings(overrides: dict[str, Any] | None = None) -> Settings:
    """Settings built from BASE_CONFIG deep-merged with *overrides*."""
    return parse_settings(_merge(BASE_CONFIG, overrides or {}))


@pytest.fixture
def settings_factory():
    return make_settings


@pytest.fixture(autouse=True)
def _test_db_password(monkeypatch):
    monkeypatch.setenv("TEST_DB_PASSWORD", "password")
