from contextlib import contextmanager

import pytest

from biz_db_mcp import db
from biz_db_mcp.errors import RESOURCE_NOT_ACCESSIBLE, BizDbError
from biz_db_mcp.institution import parse_institution
from biz_db_mcp.policy import AccessPolicy
from conftest import make_settings

TABLES = {
    ("app", "orders"): ("BASE TABLE", ["id", "amount", "ins_id_cd"]),
    ("app", "dict_region"): ("BASE TABLE", ["code", "name"]),
    ("app", "audit_log"): ("BASE TABLE", ["id", "msg"]),
    ("other", "orders"): ("BASE TABLE", ["id", "ins_id_cd"]),
}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, tuple(params or ())))
        if "INFORMATION_SCHEMA.TABLES" in sql and "TABLE_NAME = %s" in sql:
            entry = TABLES.get((params[0], params[1]))
            self._rows = [{"kind": entry[0]}] if entry else []
        elif "INFORMATION_SCHEMA.TABLES" in sql:
            self._rows = [
                {"name": t, "type": kind, "comment": ""}
                for (s, t), (kind, _) in sorted(TABLES.items())
                if s == params[0]
            ]
        elif "INFORMATION_SCHEMA.COLUMNS" in sql and "ORDINAL_POSITION" in sql:
            self._rows = [{"name": c} for c in TABLES[(params[0], params[1])][1]]
        elif "INFORMATION_SCHEMA.COLUMNS" in sql:
            entry = TABLES.get((params[0], params[1]))
            self._rows = [{"name": c} for c in entry[1]] if entry else []
        else:
            self._rows = list(self.conn.result_rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class FakeConn:
    def __init__(self, result_rows=()):
        self.executed = []
        self.result_rows = list(result_rows)

    def cursor(self):
        return FakeCursor(self)


@pytest.fixture
def fake(monkeypatch):
    conn = FakeConn()

    @contextmanager
    def connection(*args, **kwargs):
        yield conn

    monkeypatch.setattr(db, "_connection", connection)
    return conn


def _policy(settings, code="00002000"):
    return AccessPolicy(settings, parse_institution(code))


def _settings():
    return make_settings({"isolation": {"public_tables": ["primary:app.dict_region"]}})


def test_query_executes_only_the_filtered_bounded_sql(fake):
    settings = _settings()
    fake.result_rows = [{"id": 1, "amount": 3}]

    result = db.run_query(
        settings, _policy(settings, "0800002000"),
        "SELECT id, amount FROM orders WHERE amount > %s ORDER BY id", [1], limit=5,
    )

    sql, params = fake.executed[-1]
    assert "(SELECT * FROM `app`.`orders` WHERE `ins_id_cd` IN (%s)) AS `orders`" in sql
    assert sql.endswith("ORDER BY `id` LIMIT 6")
    assert params == ("00002000", 1)
    assert result["rows"] == [{"id": 1, "amount": 3}]
    assert result["truncated"] is False


def test_query_reports_row_and_byte_truncation(fake):
    settings = make_settings({"limits": {"query": {"max_bytes": 60}}})
    fake.result_rows = [{"id": i, "amount": i} for i in range(3)]
    rows = db.run_query(settings, _policy(settings), "SELECT id, amount FROM orders", None, limit=2)
    assert rows["truncated_reason"] == "row_limit" and rows["row_count"] == 2

    fake.result_rows = [{"id": 1, "amount": "x" * 50}]
    sized = db.run_query(settings, _policy(settings), "SELECT id, amount FROM orders", None, limit=2)
    assert sized["truncated_reason"] == "byte_limit" and sized["rows"] == []


def test_all_scope_reads_without_row_filter(fake):
    settings = _settings()
    db.run_query(settings, _policy(settings, "00001000"), "SELECT id FROM orders", None, limit=1)
    sql, params = fake.executed[-1]
    assert "ins_id_cd" not in sql and "`app`.`orders`" in sql
    assert params == ()


@pytest.mark.parametrize(
    "sql",
    ["SELECT * FROM audit_log", "SELECT * FROM other.orders", "SELECT * FROM missing"],
)
def test_unisolated_other_schema_and_missing_tables_look_the_same(fake, sql):
    settings = _settings()
    with pytest.raises(BizDbError) as excinfo:
        db.run_query(settings, _policy(settings), sql, None, limit=1)
    assert excinfo.value.code == RESOURCE_NOT_ACCESSIBLE
    assert str(excinfo.value) == "RESOURCE_NOT_ACCESSIBLE: table does not exist or is not accessible"


def test_list_tables_hides_tables_without_isolation(fake):
    settings = _settings()
    names = [row["name"] for row in db.list_tables(settings, _policy(settings))]
    assert names == ["dict_region", "orders"]


def test_describe_table_denies_unisolated_table(fake):
    settings = _settings()
    assert [c["name"] for c in db.describe_table(settings, _policy(settings), "orders")] == [
        "id", "amount", "ins_id_cd",
    ]
    with pytest.raises(BizDbError):
        db.describe_table(settings, _policy(settings), "audit_log")


def test_driver_errors_are_redacted():
    import pymysql

    err = db._translate_db_error(pymysql.err.OperationalError(1054, "Unknown column 'secret_value'"))
    assert "secret_value" not in str(err) and "1054" in str(err)
    assert db._translate_db_error(pymysql.err.ProgrammingError(1146, "x")).code == RESOURCE_NOT_ACCESSIBLE
