from contextlib import contextmanager

import pytest

from biz_db_mcp import db
from conftest import make_settings
from biz_db_mcp.db import QueryError, _parse_dsn, execute_write


def test_dbpm_can_supply_password_for_a_passwordless_dsn():
    parsed = _parse_dsn("mysql://user@localhost:3306/app", password="resolved-password")

    assert parsed["user"] == "user"
    assert parsed["password"] == "resolved-password"


def test_write_is_disabled_by_default(monkeypatch):
    settings = make_settings()

    with pytest.raises(QueryError, match="disabled"):
        execute_write(settings, "UPDATE users SET active = 0 WHERE id = 1", [])


class _Cursor:
    rowcount = 1
    lastrowid = 7

    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))


class _Connection:
    def __init__(self, rowcount=1):
        self.cursor_obj = _Cursor()
        self.cursor_obj.rowcount = rowcount
        self.begun = False
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.cursor_obj

    def begin(self):
        self.begun = True

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _writer_settings(monkeypatch, max_rows="1000"):
    return make_settings({
        "databases": {"primary": {"allow_writes": True}},
        "writes": {"enabled": True, "max_affected_rows": int(max_rows)},
    })


def test_write_commits_one_guarded_statement(monkeypatch):
    settings = _writer_settings(monkeypatch)
    connection = _Connection()

    @contextmanager
    def fake_connection(*args, **kwargs):
        yield connection

    monkeypatch.setattr(db, "_connection", fake_connection)

    result = execute_write(
        settings, "UPDATE users SET active = %s WHERE id = %s", [1, 4]
    )

    assert result["affected_rows"] == 1
    assert connection.begun is True
    assert connection.committed is True
    assert connection.rolled_back is False


def test_write_rolls_back_when_affected_rows_exceed_limit(monkeypatch):
    settings = _writer_settings(monkeypatch, max_rows="1")
    connection = _Connection(rowcount=2)

    @contextmanager
    def fake_connection(*args, **kwargs):
        yield connection

    monkeypatch.setattr(db, "_connection", fake_connection)

    with pytest.raises(QueryError, match="exceeding"):
        execute_write(settings, "DELETE FROM users WHERE active = 0", [])

    assert connection.committed is False
    assert connection.rolled_back is True
