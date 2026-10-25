"""Regression tests for flexibility hardening: atomic limit, multi-table WHERE, dry_run safety, offset validation."""

from contextlib import contextmanager

import pytest

from biz_db_mcp import db
from conftest import make_settings
from biz_db_mcp.db import QueryError


def _writer_settings(monkeypatch, max_rows="1000", except_tables=""):
    return make_settings({
        "databases": {"primary": {"allow_writes": True}},
        "writes": {
            "enabled": True,
            "max_affected_rows": int(max_rows),
            "require_where_except_tables": [t for t in except_tables.split(",") if t],
        },
    })


class _Cursor:
    rowcount = 1
    lastrowid = 1
    def __init__(self):
        self.executed = []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self.executed.append(sql)

class _Connection:
    def __init__(self, rowcount=1):
        self.cursor_obj = _Cursor()
        self.cursor_obj.rowcount = rowcount
        self.committed = False
        self.rolled_back = False
        self.begun = False
    def cursor(self): return self.cursor_obj
    def begin(self): self.begun = True
    def commit(self): self.committed = True
    def rollback(self): self.rolled_back = True
    def close(self): pass


# ---- P1: batch limit must always rollback, even with atomic=False ----

def test_execute_many_rolls_back_on_limit_exceed_even_when_not_atomic(monkeypatch):
    settings = _writer_settings(monkeypatch, max_rows="1")
    conn = _Connection(rowcount=1)

    @contextmanager
    def fake(*a, **kw):
        yield conn
    monkeypatch.setattr(db, "_connection", fake)

    # two inserts each affects 1 row -> total 2 exceeds limit 1
    with pytest.raises(QueryError, match="exceeding"):
        db.execute_many(settings, [
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["a"]},
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["b"]},
        ], atomic=False)

    assert conn.rolled_back is True
    assert conn.committed is False


def test_execute_many_rolls_back_on_limit_exceed_when_atomic(monkeypatch):
    settings = _writer_settings(monkeypatch, max_rows="1")
    conn = _Connection(rowcount=1)

    @contextmanager
    def fake(*a, **kw):
        yield conn
    monkeypatch.setattr(db, "_connection", fake)

    with pytest.raises(QueryError, match="exceeding"):
        db.execute_many(settings, [
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["a"]},
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["b"]},
        ], atomic=True)

    assert conn.rolled_back is True
    assert conn.committed is False


# ---- P1: non-atomic vs atomic on non-limit runtime failure ----

def test_execute_many_non_atomic_commits_on_runtime_failure(monkeypatch):
    settings = _writer_settings(monkeypatch, max_rows="100")

    class FailingCursor(_Cursor):
        cnt = 0
        def execute(self, sql, params=None):
            self.cnt += 1
            self.executed.append(sql)
            if self.cnt == 2:
                raise RuntimeError("second fails")

    class FailingConn(_Connection):
        def __init__(self):
            self.cursor_obj = FailingCursor()
            self.committed = False
            self.rolled_back = False
            self.begun = False

    conn = FailingConn()
    @contextmanager
    def fake(*a, **kw):
        yield conn
    monkeypatch.setattr(db, "_connection", fake)

    with pytest.raises(RuntimeError, match="second fails"):
        db.execute_many(settings, [
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["a"]},
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["b"]},
        ], atomic=False)

    assert conn.committed is True
    assert conn.rolled_back is False


def test_execute_many_atomic_rolls_back_on_runtime_failure(monkeypatch):
    settings = _writer_settings(monkeypatch, max_rows="100")

    class FailingCursor(_Cursor):
        cnt = 0
        def execute(self, sql, params=None):
            self.cnt += 1
            self.executed.append(sql)
            if self.cnt == 2:
                raise RuntimeError("second fails")

    class FailingConn(_Connection):
        def __init__(self):
            self.cursor_obj = FailingCursor()
            self.committed = False
            self.rolled_back = False
            self.begun = False

    conn = FailingConn()
    @contextmanager
    def fake(*a, **kw):
        yield conn
    monkeypatch.setattr(db, "_connection", fake)

    with pytest.raises(RuntimeError, match="second fails"):
        db.execute_many(settings, [
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["a"]},
            {"sql": "INSERT INTO users(name) VALUES (%s)", "params": ["b"]},
        ], atomic=True)

    assert conn.rolled_back is True
    assert conn.committed is False


# ---- P1: WHERE allowlist must not apply to multi-table DML ----

def test_where_allowlist_not_applied_to_multi_table_update(monkeypatch):
    settings = _writer_settings(monkeypatch, except_tables="temp_*")
    conn = _Connection()

    @contextmanager
    def fake(*a, **kw):
        yield conn
    monkeypatch.setattr(db, "_connection", fake)

    # single-table exempt table without WHERE should succeed
    db.execute_write(settings, "UPDATE temp_cache SET x=1", [])
    assert conn.committed is True

    # multi-table with exempt first table but without WHERE must be rejected
    conn2 = _Connection()
    @contextmanager
    def fake2(*a, **kw):
        yield conn2
    monkeypatch.setattr(db, "_connection", fake2)

    with pytest.raises(Exception, match="WHERE"):
        db.execute_write(settings, "UPDATE temp_cache JOIN users ON temp_cache.id=users.id SET users.active=0", [])

    conn3 = _Connection()
    @contextmanager
    def fake3(*a, **kw):
        yield conn3
    monkeypatch.setattr(db, "_connection", fake3)

    with pytest.raises(Exception, match="WHERE"):
        db.execute_write(settings, "DELETE FROM temp_cache JOIN users ON temp_cache.id=users.id", [])

    # DELETE FROM exempt table with JOIN should also require WHERE
    # Actually the above already tests JOIN; test comma multi-table
    conn4 = _Connection()
    @contextmanager
    def fake4(*a, **kw):
        yield conn4
    monkeypatch.setattr(db, "_connection", fake4)
    with pytest.raises(Exception, match="WHERE"):
        db.execute_write(settings, "UPDATE temp_cache, users SET users.active=0", [])


# ---- P1: dry_run must not fallback to executing DML ----

def test_execute_dry_run_does_not_execute_dml_when_explain_fails(monkeypatch):
    settings = _writer_settings(monkeypatch)

    executed = []

    @contextmanager
    def fake(*a, **kw):
        class Cur:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, sql, params=None):
                executed.append(sql)
                raise RuntimeError("EXPLAIN not supported")
            def fetchall(self): return []
        class Conn:
            def cursor(self): return Cur()
            def close(self): pass
        yield Conn()
    monkeypatch.setattr(db, "_connection", fake)

    with pytest.raises(QueryError, match="dry_run not supported"):
        db.execute_write(settings, "UPDATE users SET x=1 WHERE id=%s", [1], dry_run=True)

    # only EXPLAIN should have been attempted, no raw DML
    assert len(executed) == 1
    assert executed[0].startswith("EXPLAIN")


# ---- P2: offset validation before dry_run ----

def test_query_offset_validation_before_dry_run(monkeypatch):
    settings = make_settings()

    with pytest.raises(QueryError, match="offset"):
        db.run_query(settings, "SELECT 1", None, limit=10, offset=-7, dry_run=True)

    with pytest.raises(QueryError, match="offset"):
        db.run_query(settings, "SELECT 1", None, limit=10, offset=-1, dry_run=False)
