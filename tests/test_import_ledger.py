"""Unit tests for the persistent import idempotency ledger."""

from __future__ import annotations

import hashlib

import pymysql.err
import pytest

from biz_db_mcp import import_ledger
from biz_db_mcp.errors import IMPORTS_DISABLED, BizDbError


class _Cursor:
    def __init__(
        self,
        *,
        table_row: dict | None = None,
        index_rows: list[dict] | None = None,
        existing_row: dict | None = None,
        execute_error: Exception | None = None,
    ) -> None:
        self.table_row = table_row
        self.index_rows = index_rows or []
        self.existing_row = existing_row
        self.execute_error = execute_error
        self.executed: list[tuple[str, tuple | None]] = []
        self.rowcount = 1
        self._last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))
        self._last_sql = sql
        if self.execute_error is not None:
            raise self.execute_error

    def fetchone(self):
        if "INFORMATION_SCHEMA.TABLES" in self._last_sql:
            return self.table_row
        return self.existing_row

    def fetchall(self):
        return self.index_rows


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _unique_index_rows(columns: list[str]) -> list[dict]:
    return [
        {
            "INDEX_NAME": "uk_idempotency",
            "COLUMN_NAME": column,
            "SEQ_IN_INDEX": index,
            "NON_UNIQUE": 0,
        }
        for index, column in enumerate(columns, start=1)
    ]


def _ledger_table() -> dict:
    return {"TABLE_TYPE": "BASE TABLE", "ENGINE": "InnoDB"}


# ---------------------------------------------------------------------------
# idempotency_key
# ---------------------------------------------------------------------------


def test_idempotency_key_is_sha256_of_separator_joined_parts():
    expected = hashlib.sha256(
        "\x1f".join(
            [
                "00002000",
                "primary",
                "app.report_results",
                "a" * 64,
                "insert",
            ]
        ).encode("utf-8")
    ).hexdigest()

    assert (
        import_ledger.idempotency_key(
            "00002000", "primary", "app.report_results", "a" * 64, "insert"
        )
        == expected
    )


def test_idempotency_key_lowercases_table_and_changes_with_each_part():
    base = import_ledger.idempotency_key(
        "00002000", "primary", "app.Report_Results", "a" * 64, "insert"
    )

    assert base == import_ledger.idempotency_key(
        "00002000", "primary", "app.report_results", "a" * 64, "insert"
    )
    assert base != import_ledger.idempotency_key(
        "00002001", "primary", "app.report_results", "a" * 64, "insert"
    )
    assert base != import_ledger.idempotency_key(
        "00002000", "other", "app.report_results", "a" * 64, "insert"
    )
    assert base != import_ledger.idempotency_key(
        "00002000", "primary", "app.report_results", "b" * 64, "insert"
    )
    assert base != import_ledger.idempotency_key(
        "00002000", "primary", "app.report_results", "a" * 64, "upsert"
    )


# ---------------------------------------------------------------------------
# insert_entry / finalize_entry / find_entry
# ---------------------------------------------------------------------------


def test_insert_entry_writes_one_parameterized_row():
    cursor = _Cursor()

    inserted = import_ledger.insert_entry(
        cursor,
        ledger_table="bizdb_import_ledger",
        key="k" * 64,
        batch_id="b" * 32,
        institution_code="00002000",
        database_id="primary",
        table="APP.Report_Results",
        content_sha256="a" * 64,
        mode="insert",
        row_count=3,
    )

    assert inserted is True
    sql, params = cursor.executed[0]
    assert sql.startswith("INSERT INTO `bizdb_import_ledger`")
    assert "`idempotency_key`" in sql
    assert "(%s, %s, %s, %s, %s, %s, %s, %s)" in sql
    assert params == (
        "k" * 64,
        "b" * 32,
        "00002000",
        "primary",
        "app.report_results",
        "a" * 64,
        "insert",
        3,
    )


def test_insert_entry_returns_false_on_duplicate_key():
    error = pymysql.err.IntegrityError(
        1062, "Duplicate entry 'k' for key 'uk_idempotency'"
    )
    cursor = _Cursor(execute_error=error)

    assert (
        import_ledger.insert_entry(
            cursor,
            ledger_table="bizdb_import_ledger",
            key="k" * 64,
            batch_id="b" * 32,
            institution_code="00002000",
            database_id="primary",
            table="app.report_results",
            content_sha256="a" * 64,
            mode="insert",
            row_count=1,
        )
        is False
    )


def test_insert_entry_reraises_other_database_errors():
    cursor = _Cursor(execute_error=pymysql.err.OperationalError(2006, "gone away"))

    with pytest.raises(pymysql.err.OperationalError):
        import_ledger.insert_entry(
            cursor,
            ledger_table="bizdb_import_ledger",
            key="k" * 64,
            batch_id="b" * 32,
            institution_code="00002000",
            database_id="primary",
            table="app.report_results",
            content_sha256="a" * 64,
            mode="insert",
            row_count=1,
        )


def test_finalize_entry_updates_counts_by_batch_id():
    cursor = _Cursor()

    import_ledger.finalize_entry(
        cursor,
        ledger_table="bizdb_import_ledger",
        batch_id="b" * 32,
        inserted=3,
        updated=0,
    )

    sql, params = cursor.executed[0]
    assert sql.startswith("UPDATE `bizdb_import_ledger`")
    assert "`inserted_rows` = %s" in sql
    assert "`updated_rows` = %s" in sql
    assert "`batch_id` = %s" in sql
    assert params == (3, 0, "b" * 32)


def test_find_entry_scopes_by_institution_and_normalizes_keys():
    cursor = _Cursor(
        existing_row={
            "BATCH_ID": "b" * 32,
            "ROW_COUNT": 4,
            "INSERTED_ROWS": 4,
            "UPDATED_ROWS": 0,
        }
    )

    row = import_ledger.find_entry(
        cursor,
        ledger_table="bizdb_import_ledger",
        key="k" * 64,
        institution_code="00002000",
    )

    assert row == {
        "batch_id": "b" * 32,
        "row_count": 4,
        "inserted_rows": 4,
        "updated_rows": 0,
    }
    sql, params = cursor.executed[0]
    assert sql.startswith("SELECT")
    assert "`idempotency_key` = %s" in sql
    assert "`institution_code` = %s" in sql
    assert params == ("k" * 64, "00002000")


def test_find_entry_returns_none_when_missing():
    cursor = _Cursor(existing_row=None)

    assert (
        import_ledger.find_entry(
            cursor,
            ledger_table="bizdb_import_ledger",
            key="k" * 64,
            institution_code="00002000",
        )
        is None
    )


# ---------------------------------------------------------------------------
# check_ledger
# ---------------------------------------------------------------------------


def test_check_ledger_accepts_innodb_with_idempotency_unique_index():
    cursor = _Cursor(
        table_row=_ledger_table(),
        index_rows=_unique_index_rows(["idempotency_key"]),
    )

    import_ledger.check_ledger(
        _Connection(cursor), schema="app", ledger_table="bizdb_import_ledger"
    )

    assert len(cursor.executed) == 2
    assert cursor.executed[0][1] == ("app", "bizdb_import_ledger")
    assert "INFORMATION_SCHEMA.STATISTICS" in cursor.executed[1][0]


def test_check_ledger_rejects_missing_table():
    cursor = _Cursor(table_row=None, index_rows=[])

    with pytest.raises(BizDbError) as excinfo:
        import_ledger.check_ledger(
            _Connection(cursor), schema="app", ledger_table="bizdb_import_ledger"
        )

    assert excinfo.value.code == IMPORTS_DISABLED
    assert excinfo.value.message == import_ledger.LEDGER_MISSING_MESSAGE
    assert "migrations/001_import_ledger.sql" in excinfo.value.message


def test_check_ledger_rejects_non_innodb_table():
    cursor = _Cursor(
        table_row={"TABLE_TYPE": "BASE TABLE", "ENGINE": "MyISAM"},
        index_rows=_unique_index_rows(["idempotency_key"]),
    )

    with pytest.raises(BizDbError, match="apply migrations/001_import_ledger.sql"):
        import_ledger.check_ledger(
            _Connection(cursor), schema="app", ledger_table="bizdb_import_ledger"
        )


def test_check_ledger_rejects_non_unique_or_wrong_index():
    for rows in (
        [
            {
                "INDEX_NAME": "idx_key",
                "COLUMN_NAME": "idempotency_key",
                "SEQ_IN_INDEX": 1,
                "NON_UNIQUE": 1,
            }
        ],
        _unique_index_rows(["idempotency_key", "batch_id"]),
        _unique_index_rows(["batch_id"]),
        [],
    ):
        cursor = _Cursor(table_row=_ledger_table(), index_rows=rows)
        with pytest.raises(BizDbError) as excinfo:
            import_ledger.check_ledger(
                _Connection(cursor),
                schema="app",
                ledger_table="bizdb_import_ledger",
            )
        assert excinfo.value.code == IMPORTS_DISABLED


def test_check_ledger_fails_closed_on_database_errors():
    cursor = _Cursor(execute_error=RuntimeError("SECRET ledger failure"))

    with pytest.raises(BizDbError) as excinfo:
        import_ledger.check_ledger(
            _Connection(cursor), schema="app", ledger_table="bizdb_import_ledger"
        )

    assert excinfo.value.code == IMPORTS_DISABLED
    assert excinfo.value.message == import_ledger.LEDGER_MISSING_MESSAGE
    assert "SECRET" not in str(excinfo.value)
