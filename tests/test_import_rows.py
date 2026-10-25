"""Behavior tests for the guarded, whitelisted ``import_rows`` pipeline.

All database access is faked: the cursor records SQL/parameters and answers
INFORMATION_SCHEMA queries from dictionaries. No test connects to MySQL.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import time
from contextlib import contextmanager
from dataclasses import replace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pymysql.err
import pytest

from biz_db_mcp import import_rows
from biz_db_mcp.config import Settings
from biz_db_mcp.errors import (
    CONTENT_DIGEST_MISMATCH,
    DATABASE_ERROR,
    DEADLINE_EXCEEDED,
    FORBIDDEN_INSTITUTION,
    IMPORT_OUTCOME_UNKNOWN,
    IMPORTS_DISABLED,
    INVALID_ARGUMENT,
    INVALID_FILE,
    TABLE_NOT_IMPORTABLE,
    TOO_LARGE,
    VALIDATION_FAILED,
    BizDbError,
)
from biz_db_mcp.import_rows import ImportRequest, ImportResult
from biz_db_mcp.institution import Institution, parse_institution
from conftest import make_settings

INSTITUTION = parse_institution("0800002000")  # raw 0800002000, code 00002000
RAW_INSTITUTION = parse_institution("0800002000")


# ---------------------------------------------------------------------------
# Settings and payload helpers
# ---------------------------------------------------------------------------

DEFAULT_SPEC: dict[str, Any] = {
    "modes": ["insert", "upsert"],
    "columns": [
        "report_date",
        "branch_code",
        "metric",
        "value",
        "note",
        "quantity",
    ],
    "key_columns": ["report_date", "branch_code"],
    "institution_column": "ins_id_cd",
    "value_format": "code",
    "max_rows": 100,
}

DEFAULT_HEADER = ["report_date", "branch_code", "metric", "value", "note"]
DEFAULT_ROWS = (
    ("2026-01-01", "0001", "sales", "1.25", ""),
    ("2026-01-02", "0002", "cost", "2.50", "memo"),
)


def import_settings(
    *,
    table_spec: dict[str, Any] | None = None,
    value_format: str = "code",
    limits: dict[str, Any] | None = None,
) -> Settings:
    spec = dict(DEFAULT_SPEC)
    spec["value_format"] = value_format
    if table_spec:
        spec.update(table_spec)
    overrides: dict[str, Any] = {
        "databases": {
            "primary": {
                "allow_imports": True,
                "import_credential": {
                    "dsn": "mysql://importer@localhost:3306/app",
                    "password_env": "TEST_DB_PASSWORD",
                },
            }
        },
        "imports": {
            "enabled": True,
            "tables": {"primary:app.report_results": spec},
        },
    }
    if limits is not None:
        overrides["limits"] = {"import": limits}
    return make_settings(overrides)


def csv_payload(
    rows: tuple[tuple[Any, ...], ...] = DEFAULT_ROWS,
    header: list[str] | None = None,
) -> bytes:
    columns = list(DEFAULT_HEADER if header is None else header)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def jsonl_rows() -> list[dict[str, Any]]:
    return [
        {
            "report_date": "2026-01-01",
            "branch_code": "0001",
            "metric": "sales",
            "value": "1.25",
            "note": None,
        },
        {
            "report_date": "2026-01-02",
            "branch_code": "0002",
            "metric": "cost",
            "value": "2.50",
            "note": "memo",
        },
    ]


def jsonl_payload(rows: list[dict[str, Any]] | None = None) -> bytes:
    selected = jsonl_rows() if rows is None else rows
    return ("\n".join(json.dumps(row) for row in selected) + "\n").encode("utf-8")


def parquet_payload(
    rows: tuple[tuple[Any, ...], ...] = DEFAULT_ROWS,
    *,
    extra: dict[str, Any] | None = None,
) -> bytes:
    count = len(rows)
    arrays: dict[str, Any] = {
        "report_date": [row[0] for row in rows],
        "branch_code": [row[1] for row in rows],
        "metric": [row[2] for row in rows],
        "value": [row[3] for row in rows],
        "note": [row[4] if row[4] != "" else None for row in rows],
        "quantity": pa.array([None] * count, type=pa.int32()),
    }
    if extra:
        arrays.update(extra)
    sink = io.BytesIO()
    pq.write_table(pa.table(arrays), sink)
    return sink.getvalue()


def make_request(
    data: bytes | str,
    *,
    filename: str = "report.csv",
    fmt: str | None = None,
    mode: str = "insert",
    dry_run: bool = False,
    table: str = "app.report_results",
    database: str = "primary",
    institution: Institution | None = None,
    sha256: str | None = None,
    file_base64: str | None = None,
) -> ImportRequest:
    payload = data.encode("utf-8") if isinstance(data, str) else data
    encoded = (
        base64.b64encode(payload).decode("ascii")
        if file_base64 is None
        else file_base64
    )
    digest = hashlib.sha256(payload).hexdigest() if sha256 is None else sha256
    return ImportRequest(
        institution=institution or INSTITUTION,
        database=database,
        table=table,
        file_base64=encoded,
        filename=filename,
        content_sha256=digest,
        format=fmt,
        mode=mode,
        dry_run=dry_run,
    )


def run(
    env: "Environment",
    request: ImportRequest,
    settings: Settings | None = None,
    *,
    deadline: float | None = None,
) -> ImportResult:
    return import_rows.run_import(
        settings or import_settings(),
        request,
        connect=env.connect,
        deadline=time.monotonic() + 60 if deadline is None else deadline,
    )


# ---------------------------------------------------------------------------
# Fake connection and cursor
# ---------------------------------------------------------------------------


def _column(
    name: str,
    data_type: str,
    column_type: str,
    *,
    nullable: bool = False,
    default: Any = None,
    extra: str = "",
    char_length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    collation: str | None = None,
) -> dict[str, Any]:
    return {
        "COLUMN_NAME": name,
        "DATA_TYPE": data_type,
        "COLUMN_TYPE": column_type,
        "IS_NULLABLE": "YES" if nullable else "NO",
        "COLUMN_DEFAULT": default,
        "EXTRA": extra,
        "CHARACTER_MAXIMUM_LENGTH": char_length,
        "NUMERIC_PRECISION": precision,
        "NUMERIC_SCALE": scale,
        "COLLATION_NAME": collation,
    }


DEFAULT_COLUMNS = (
    _column("report_date", "date", "date"),
    _column("branch_code", "varchar", "varchar(10)", char_length=10),
    _column("metric", "varchar", "varchar(32)", char_length=32),
    _column("value", "decimal", "decimal(10,2)", precision=10, scale=2),
    _column("note", "varchar", "varchar(64)", nullable=True, char_length=64),
    _column("quantity", "int", "int unsigned", nullable=True),
    _column("ins_id_cd", "varchar", "varchar(64)", char_length=64),
)
DEFAULT_UNIQUE = (("PRIMARY", ("report_date", "branch_code", "ins_id_cd")),)
_DEFAULT = object()


def replace_column(name: str, **fields: Any) -> list[dict[str, Any]]:
    """Return a copy of :data:`DEFAULT_COLUMNS` with *name* overridden."""
    return [
        {**column, **fields} if column["COLUMN_NAME"] == name else dict(column)
        for column in DEFAULT_COLUMNS
    ]


class Environment:
    """One fake connection/cursor plus the answers for INFORMATION_SCHEMA."""

    def __init__(
        self,
        *,
        columns: list[dict[str, Any]] | None = None,
        unique_indexes: tuple[tuple[str, tuple[str, ...]], ...] | None = None,
        table_row: Any = _DEFAULT,
        trigger_row: Any = None,
        existing_keys: int = 0,
        existing_ledger: dict[str, Any] | None = None,
        duplicate_ledger: bool = False,
        fail_commit: bool = False,
        fail_business: Exception | None = None,
        fail_metadata: Exception | None = None,
        force_rowcount: int | None = None,
        expire_on_begin: bool = False,
    ) -> None:
        self.columns = list(DEFAULT_COLUMNS if columns is None else columns)
        self.unique_indexes = (
            DEFAULT_UNIQUE if unique_indexes is None else unique_indexes
        )
        self.table_row = (
            {"TABLE_TYPE": "BASE TABLE", "ENGINE": "InnoDB"}
            if table_row is _DEFAULT
            else table_row
        )
        self.trigger_row = trigger_row
        self.existing_keys = existing_keys
        self.existing_ledger = existing_ledger
        self.duplicate_ledger = duplicate_ledger
        self.fail_commit = fail_commit
        self.fail_business = fail_business
        self.fail_metadata = fail_metadata
        self.force_rowcount = force_rowcount
        self.expire_on_begin = expire_on_begin
        self.clock = 0.0

        self.connect_calls = 0
        self.executed: list[tuple[str, Any]] = []
        self.business_inserts: list[tuple[str, list[Any]]] = []
        self.business_selects: list[tuple[str, list[Any]]] = []
        self.ledger_inserts: list[tuple[Any, ...]] = []
        self.ledger_updates: list[tuple[Any, ...]] = []
        self.cursor = _FakeCursor(self)
        self.connection = _FakeConnection(self)

    def connect(self):
        self.connect_calls += 1

        @contextmanager
        def _context():
            yield self.connection

        return _context()

    def statements(self, needle: str) -> list[tuple[str, Any]]:
        upper = needle.upper()
        return [
            (sql, params)
            for sql, params in self.executed
            if upper in " ".join(sql.upper().split())
        ]


class _FakeCursor:
    def __init__(self, env: Environment) -> None:
        self.env = env
        self.rowcount = 0
        self.lastrowid = 1
        self._pending = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        env = self.env
        env.executed.append((sql, params))
        upper = " ".join(sql.upper().split())
        self.rowcount = 0
        self._pending = ""

        if "INFORMATION_SCHEMA." in upper:
            if env.fail_metadata is not None:
                raise env.fail_metadata
            if "INFORMATION_SCHEMA.TABLES" in upper:
                self._pending = "tables"
                return
            if "INFORMATION_SCHEMA.COLUMNS" in upper:
                self._pending = "columns"
                return
            if "INFORMATION_SCHEMA.STATISTICS" in upper:
                self._pending = "statistics"
                return
            if "INFORMATION_SCHEMA.TRIGGERS" in upper:
                self._pending = "triggers"
                return

        if "`BIZDB_IMPORT_LEDGER`" in upper:
            if upper.startswith("INSERT"):
                if env.duplicate_ledger:
                    raise pymysql.err.IntegrityError(
                        1062,
                        "Duplicate entry 'secret-key' for key 'uk_idempotency'",
                    )
                env.ledger_inserts.append(tuple(params))
                self.rowcount = 1
                return
            if upper.startswith("UPDATE"):
                env.ledger_updates.append(tuple(params))
                self.rowcount = 1
                return
            if upper.startswith("SELECT"):
                self._pending = "ledger_select"
                return

        if upper.startswith("SELECT") and "FOR UPDATE" in upper:
            env.business_selects.append((sql, list(params or [])))
            self._pending = "existing"
            return

        if upper.startswith("INSERT INTO `APP`."):
            if env.fail_business is not None:
                raise env.fail_business
            env.business_inserts.append((sql, list(params or [])))
            values = sql.split("VALUES", 1)[1]
            first_tuple = values.split(")", 1)[0]
            column_count = first_tuple.count("%s")
            self.rowcount = (
                env.force_rowcount
                if env.force_rowcount is not None
                else len(params or []) // column_count
            )
            return

        raise AssertionError(f"unexpected SQL in test: {sql}")

    def fetchone(self) -> Any:
        if self._pending == "tables":
            return self.env.table_row
        if self._pending == "triggers":
            return self.env.trigger_row
        if self._pending == "ledger_select":
            return self.env.existing_ledger
        return None

    def fetchall(self) -> Any:
        if self._pending == "columns":
            return list(self.env.columns)
        if self._pending == "statistics":
            rows = []
            for name, columns in self.env.unique_indexes:
                for sequence, column in enumerate(columns, start=1):
                    rows.append(
                        {
                            "INDEX_NAME": name,
                            "COLUMN_NAME": column,
                            "SEQ_IN_INDEX": sequence,
                            "NON_UNIQUE": 0,
                        }
                    )
            return rows
        if self._pending == "existing":
            return [{} for _ in range(self.env.existing_keys)]
        return []


class _FakeConnection:
    def __init__(self, env: Environment) -> None:
        self.env = env
        self.begun = False
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return self.env.cursor

    def begin(self) -> None:
        self.begun = True
        if self.env.expire_on_begin:
            self.env.clock = 10_000.0

    def commit(self) -> None:
        if self.env.fail_commit:
            raise RuntimeError("connection lost while committing SECRETMETA")
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class _Clock:
    def __init__(self, env: Environment) -> None:
        self.env = env

    def monotonic(self) -> float:
        return self.env.clock


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


def test_imports_disabled_is_checked_before_the_payload():
    settings = make_settings()  # imports.enabled defaults to false
    env = Environment()
    request = make_request(
        b"ignored", filename="report.csv", file_base64="!!!not-base64!!!"
    )

    with pytest.raises(BizDbError) as excinfo:
        run(env, request, settings)

    assert excinfo.value.code == IMPORTS_DISABLED
    assert env.connect_calls == 0


def test_non_importable_tables_share_one_message():
    settings = import_settings()
    env = Environment()
    requests = [
        make_request(csv_payload(), table="report_results"),  # malformed
        make_request(csv_payload(), table="app.other"),  # not whitelisted
        make_request(csv_payload(), database="missing"),  # unknown database
        make_request(csv_payload(), database=""),  # empty database id
    ]

    messages = set()
    for request in requests:
        with pytest.raises(BizDbError) as excinfo:
            run(env, request, settings)
        assert excinfo.value.code == TABLE_NOT_IMPORTABLE
        messages.add(excinfo.value.message)

    assert messages == {"table is not available for import"}
    assert env.connect_calls == 0


def test_database_without_allow_imports_uses_the_same_message():
    settings = import_settings()
    disabled = replace(
        settings,
        databases=tuple(
            replace(database, allow_imports=False)
            for database in settings.databases
        ),
    )
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()), disabled)

    assert excinfo.value.code == TABLE_NOT_IMPORTABLE
    assert excinfo.value.message == "table is not available for import"
    assert env.connect_calls == 0


def test_read_scope_never_grants_import():
    settings = import_settings()
    env = Environment()

    for raw in ("00001000", "00003000", "00009000"):
        with pytest.raises(BizDbError) as excinfo:
            run(
                env,
                make_request(csv_payload(), institution=parse_institution(raw)),
                settings,
            )
        assert excinfo.value.code == FORBIDDEN_INSTITUTION

    assert env.connect_calls == 0


def test_mode_must_be_configured_for_the_table():
    settings = import_settings()
    env = Environment()
    for mode in ("delete", "replace", ""):
        with pytest.raises(BizDbError) as excinfo:
            run(env, make_request(csv_payload(), mode=mode), settings)
        assert excinfo.value.code == INVALID_ARGUMENT

    insert_only = import_settings(table_spec={"modes": ["insert"]})
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), mode="upsert"), insert_only)
    assert excinfo.value.code == INVALID_ARGUMENT
    assert env.connect_calls == 0


# ---------------------------------------------------------------------------
# Base64, digest, filename
# ---------------------------------------------------------------------------


def test_base64_rejects_whitespace_and_data_uri_prefixes():
    settings = import_settings()
    env = Environment()

    for encoded in ("Y W J j", "data:text/csv;base64,YWJj"):
        with pytest.raises(BizDbError) as excinfo:
            run(env, make_request(csv_payload(), file_base64=encoded), settings)
        assert excinfo.value.code == INVALID_FILE

    assert env.connect_calls == 0


def test_base64_must_be_canonical():
    # "YR==" decodes to the same byte as "YQ==" but is not what b64encode emits.
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), file_base64="YR=="))

    assert excinfo.value.code == INVALID_FILE
    assert env.connect_calls == 0


def test_oversized_base64_is_rejected_before_decoding():
    settings = import_settings(limits={"max_bytes": 1})
    env = Environment()

    # max_bytes=1 permits 4 encoded characters; this string is longer.
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(b"", file_base64="!!!!!"), settings)

    assert excinfo.value.code == TOO_LARGE
    assert env.connect_calls == 0


def test_decoded_size_is_bounded_too():
    settings = import_settings(limits={"max_bytes": 1})
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(b"ab", filename="report.csv"), settings)

    assert excinfo.value.code == TOO_LARGE
    assert env.connect_calls == 0


def test_digest_must_match_the_decoded_bytes():
    env = Environment()
    request = make_request(csv_payload(), sha256="0" * 64)

    with pytest.raises(BizDbError) as excinfo:
        run(env, request)

    assert excinfo.value.code == CONTENT_DIGEST_MISMATCH
    assert env.connect_calls == 0


def test_digest_must_be_lowercase_hex_sha256():
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), sha256="ABC"))

    assert excinfo.value.code == INVALID_ARGUMENT
    assert env.connect_calls == 0


def test_filename_must_be_a_plain_bounded_name():
    env = Environment()
    for filename in ("", "..", "../x.csv", "dir/x.csv", "dir\\x.csv", "\x01.csv", "a" * 256):
        with pytest.raises(BizDbError) as excinfo:
            run(env, make_request(csv_payload(), filename=filename))
        assert excinfo.value.code == INVALID_ARGUMENT

    assert env.connect_calls == 0


# ---------------------------------------------------------------------------
# Format resolution and sniffing
# ---------------------------------------------------------------------------


def test_format_is_inferred_from_csv_and_ndjson_extensions():
    env = Environment()

    csv_result = run(
        env, make_request(csv_payload(), filename="report.csv", dry_run=True)
    )
    assert csv_result.status == "dry_run"
    assert csv_result.row_count == 2

    jsonl_result = run(
        env,
        make_request(jsonl_payload(), filename="report.ndjson", dry_run=True),
        import_settings(),
    )
    assert jsonl_result.status == "dry_run"
    assert jsonl_result.row_count == 2


def test_explicit_format_must_agree_with_the_extension():
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), fmt="jsonl", filename="report.csv"))

    assert excinfo.value.code == INVALID_ARGUMENT


def test_format_is_required_when_the_extension_is_unknown():
    env = Environment()
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), filename="report.txt"))
    assert excinfo.value.code == INVALID_ARGUMENT

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), filename="report.txt", fmt="xlsx"))
    assert excinfo.value.code == INVALID_ARGUMENT


def test_parquet_magic_is_checked_before_parsing():
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(b"not a parquet file", filename="report.parquet"))

    assert excinfo.value.code == INVALID_FILE
    assert env.connect_calls == 0


def test_csv_accepts_and_strips_a_leading_bom():
    env = Environment()
    payload = b"\xef\xbb\xbf" + csv_payload()

    result = run(env, make_request(payload, filename="report.csv", dry_run=True))

    assert result.status == "dry_run"
    assert result.problems == ()
    assert result.row_count == 2


def test_jsonl_rejects_a_byte_order_mark():
    env = Environment()
    payload = b"\xef\xbb\xbf" + jsonl_payload()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(payload, filename="report.jsonl"))

    assert excinfo.value.code == INVALID_FILE
    assert env.connect_calls == 0


# ---------------------------------------------------------------------------
# Parsing bounds and malformed files
# ---------------------------------------------------------------------------


def test_csv_rejects_duplicate_header_names_case_insensitively():
    env = Environment()
    header = ["report_date", "Report_Date", "metric", "value", "note"]
    rows = (("2026-01-01", "0001", "sales", "1.25", ""),)

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(rows, header=header)))

    assert excinfo.value.code == INVALID_FILE


def test_csv_blank_lines_are_ignored():
    env = Environment()
    payload = csv_payload().replace(b"\n", b"\n\n", 1)

    result = run(env, make_request(payload, dry_run=True))

    assert result.status == "dry_run"
    assert result.row_count == 2
    assert result.problems == ()


def test_csv_rejects_ragged_rows():
    env = Environment()
    payload = csv_payload() + b"2026-01-03,0003,sales\n"

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(payload))

    assert excinfo.value.code == INVALID_FILE


def test_row_limit_rejects_the_whole_batch_before_database_access():
    settings = import_settings(table_spec={"max_rows": 1})
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()), settings)

    assert excinfo.value.code == TOO_LARGE
    assert env.connect_calls == 0


def test_column_limit_is_enforced():
    settings = import_settings(limits={"max_columns": 2})
    env = Environment()

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()), settings)

    assert excinfo.value.code == TOO_LARGE
    assert env.connect_calls == 0


def test_oversized_field_is_a_problem_not_truncation():
    settings = import_settings(limits={"max_field_bytes": 3})
    env = Environment()
    rows = (("2026-01-01", "0001", "sales", "1.25", "too long"),)

    result = run(env, make_request(csv_payload(rows), dry_run=True), settings)

    assert result.status == "dry_run"
    assert any(
        problem.row == 1
        and problem.column == "note"
        and "maximum field size" in problem.reason
        for problem in result.problems
    )
    assert env.business_inserts == []
    assert env.ledger_inserts == []


def test_csv_field_larger_than_pythons_parser_limit_is_a_problem():
    env = Environment()
    rows = (("2026-01-01", "0001", "sales", "1.25", "x" * 200_000),)

    result = run(env, make_request(csv_payload(rows), dry_run=True))

    assert result.status == "dry_run"
    assert any(
        problem.column == "note" and "maximum field size" in problem.reason
        for problem in result.problems
    )
    assert env.business_inserts == []


def test_jsonl_rejects_nested_values():
    env = Environment()
    rows = jsonl_rows()
    rows[0]["meta"] = {"nested": True}

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(jsonl_payload(rows), filename="report.jsonl"))

    assert excinfo.value.code == INVALID_FILE
    assert env.connect_calls == 0


def test_jsonl_lone_surrogate_is_a_validation_problem():
    env = Environment()
    rows = jsonl_rows()
    rows[0]["note"] = "\ud800"

    result = run(
        env, make_request(jsonl_payload(rows), filename="report.jsonl", dry_run=True)
    )

    assert result.status == "dry_run"
    assert any(
        problem.column == "note" and "UTF-8" in problem.reason
        for problem in result.problems
    )
    assert env.business_inserts == []


def test_jsonl_rejects_nan_and_infinity():
    env = Environment()
    rows = jsonl_rows()
    rows[0]["value"] = float("nan")

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(jsonl_payload(rows), filename="report.jsonl"))

    assert excinfo.value.code == INVALID_FILE


def test_jsonl_rows_must_have_the_first_rows_keys():
    env = Environment()
    rows = jsonl_rows()
    rows[1].pop("note")

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(jsonl_payload(rows), filename="report.jsonl"))

    assert excinfo.value.code == INVALID_FILE


def test_parquet_row_limit_uses_the_footer_before_reading_data():
    settings = import_settings(table_spec={"max_rows": 2})
    env = Environment()
    payload = parquet_payload(DEFAULT_ROWS + (("2026-01-03", "0003", "sales", "1.00", ""),))

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(payload, filename="report.parquet"), settings)

    assert excinfo.value.code == TOO_LARGE
    assert env.connect_calls == 0


def test_parquet_nested_columns_are_rejected():
    env = Environment()
    payload = parquet_payload(
        extra={"meta": pa.array([[1, 2], [3, 4]], type=pa.list_(pa.int64()))}
    )

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(payload, filename="report.parquet"))

    assert excinfo.value.code == INVALID_FILE
    assert env.connect_calls == 0


def test_parquet_dry_run_reads_all_batches():
    env = Environment()

    result = run(
        env,
        make_request(parquet_payload(), filename="report.parquet", dry_run=True),
    )

    assert result.status == "dry_run"
    assert result.row_count == 2
    assert result.problems == ()


# ---------------------------------------------------------------------------
# Metadata checks
# ---------------------------------------------------------------------------


def test_metadata_must_be_an_innodb_base_table():
    env = Environment(table_row={"TABLE_TYPE": "VIEW", "ENGINE": None})
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))
    assert excinfo.value.code == TABLE_NOT_IMPORTABLE

    env = Environment(table_row={"TABLE_TYPE": "BASE TABLE", "ENGINE": "MyISAM"})
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))
    assert excinfo.value.code == TABLE_NOT_IMPORTABLE

    env = Environment(table_row=None)
    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))
    assert excinfo.value.code == TABLE_NOT_IMPORTABLE


def test_any_trigger_refuses_the_table():
    env = Environment(trigger_row={"TRIGGER_NAME": "audit_insert"})

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == TABLE_NOT_IMPORTABLE
    assert env.business_inserts == []


def test_every_whitelisted_column_must_exist_in_the_table():
    columns = [column for column in DEFAULT_COLUMNS if column["COLUMN_NAME"] != "note"]
    env = Environment(columns=columns)

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == TABLE_NOT_IMPORTABLE
    assert env.business_inserts == []


def test_metadata_uses_config_identifiers_as_parameters():
    env = Environment()
    run(env, make_request(csv_payload()))

    column_queries = [
        entry
        for entry in env.statements("INFORMATION_SCHEMA.COLUMNS")
    ]
    assert len(column_queries) == 1
    sql, params = column_queries[0]
    assert params == ("app", "report_results")
    assert "report_results" not in sql
    assert "COLLATION_NAME" in sql


def test_metadata_database_errors_are_redacted():
    env = Environment(fail_metadata=RuntimeError("SECRETMETADATA"))

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == DATABASE_ERROR
    assert "SECRETMETADATA" not in str(excinfo.value)


# ---------------------------------------------------------------------------
# Column rules, institution binding, dry run
# ---------------------------------------------------------------------------


def test_unknown_file_column_becomes_a_dry_run_problem():
    env = Environment()
    header = DEFAULT_HEADER + ["extra"]
    rows = tuple(row + ("surprise",) for row in DEFAULT_ROWS)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True)
    )

    assert result.status == "dry_run"
    assert any(
        problem.row == 0
        and problem.column == "extra"
        and "not whitelisted" in problem.reason
        for problem in result.problems
    )
    assert env.business_inserts == []
    assert env.ledger_inserts == []


def test_generated_columns_cannot_appear_in_the_file():
    columns = list(DEFAULT_COLUMNS) + [
        _column(
            "computed",
            "varchar",
            "varchar(16)",
            nullable=True,
            extra="VIRTUAL GENERATED",
            char_length=16,
        )
    ]
    settings = import_settings(
        table_spec={"columns": DEFAULT_SPEC["columns"] + ["computed"]}
    )
    env = Environment(columns=columns)
    header = DEFAULT_HEADER + ["computed"]
    rows = tuple(row + ("x",) for row in DEFAULT_ROWS)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True), settings
    )

    assert result.status == "dry_run"
    assert any(
        problem.column == "computed" and "generated" in problem.reason
        for problem in result.problems
    )


def test_required_columns_must_be_present():
    env = Environment()
    header = ["branch_code", "metric", "value", "note"]
    rows = (("0001", "sales", "1.25", ""),)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True)
    )

    assert result.status == "dry_run"
    assert any(
        problem.row == 0
        and problem.column == "report_date"
        and "required column is missing" in problem.reason
        for problem in result.problems
    )


def test_none_into_a_not_null_column_is_a_problem():
    env = Environment()
    rows = (("", "0001", "sales", "1.25", ""),)

    result = run(env, make_request(csv_payload(rows), dry_run=True))

    assert result.status == "dry_run"
    assert any(
        problem.row == 1
        and problem.column == "report_date"
        and "NULL" in problem.reason
        for problem in result.problems
    )


def test_decimal_scale_overflow_is_a_problem():
    env = Environment()
    rows = (("2026-01-01", "0001", "sales", "1.234", ""),)

    result = run(env, make_request(csv_payload(rows), dry_run=True))

    assert result.status == "dry_run"
    assert result.inserted is None
    assert result.updated is None
    assert result.batch_id is None
    assert any(
        problem.row == 1
        and problem.column == "value"
        and "decimal" in problem.reason
        for problem in result.problems
    )


def test_varchar_length_is_checked_in_characters():
    env = Environment()
    rows = (("2026-01-01", "0001", "sales", "1.25", "x" * 65),)

    result = run(env, make_request(csv_payload(rows), dry_run=True))

    assert any(
        problem.column == "note" and "longer" in problem.reason
        for problem in result.problems
    )


def test_integer_range_is_checked_exactly():
    env = Environment()
    header = DEFAULT_HEADER + ["quantity"]
    rows = (("2026-01-01", "0001", "sales", "1.25", "", "999999999999999999999999"),)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True)
    )

    assert any(
        problem.column == "quantity" and "out of range" in problem.reason
        for problem in result.problems
    )


def test_binary_blob_and_geometry_columns_are_unsupported():
    columns = list(DEFAULT_COLUMNS) + [
        _column("attachment", "blob", "blob", nullable=True)
    ]
    settings = import_settings(
        table_spec={"columns": DEFAULT_SPEC["columns"] + ["attachment"]}
    )
    env = Environment(columns=columns)
    header = DEFAULT_HEADER + ["attachment"]
    rows = tuple(row + ("payload",) for row in DEFAULT_ROWS)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True), settings
    )

    assert result.status == "dry_run"
    assert any(
        problem.column == "attachment" and "unsupported" in problem.reason
        for problem in result.problems
    )
    assert env.business_inserts == []


def test_absurdly_long_integer_text_is_a_problem_not_a_crash():
    env = Environment()
    header = DEFAULT_HEADER + ["quantity"]
    rows = (("2026-01-01", "0001", "sales", "1.25", "", "9" * 5000),)

    result = run(env, make_request(csv_payload(rows, header=header), dry_run=True))

    assert result.status == "dry_run"
    assert any(
        problem.column == "quantity" and "out of range" in problem.reason
        for problem in result.problems
    )


def test_integer_strings_are_normalized_and_booleans_fit_tinyint_one():
    columns = list(DEFAULT_COLUMNS)
    columns[5] = _column("quantity", "tinyint", "tinyint(1)", nullable=True)
    settings = import_settings()
    env = Environment(columns=columns)
    header = DEFAULT_HEADER + ["quantity"]
    rows = (("2026-01-01", "0001", "sales", "1.25", "", "true"),)

    result = run(
        env, make_request(csv_payload(rows, header=header), dry_run=True), settings
    )

    assert result.status == "dry_run"
    assert result.problems == ()


def test_upsert_duplicate_key_tuples_in_the_file_are_problems():
    env = Environment()
    rows = (
        ("2026-01-01", "0001", "sales", "1.25", ""),
        ("2026-01-01", "0001", "cost", "2.50", ""),
    )

    result = run(
        env, make_request(csv_payload(rows), mode="upsert", dry_run=True)
    )

    assert result.status == "dry_run"
    assert any(
        problem.row == 2 and "duplicate key tuple" in problem.reason
        for problem in result.problems
    )


def test_another_institutions_value_fails_the_whole_batch_even_in_dry_run():
    env = Environment()
    header = DEFAULT_HEADER + ["ins_id_cd"]
    rows = tuple(row + ("00009999",) for row in DEFAULT_ROWS)

    with pytest.raises(BizDbError) as excinfo:
        run(
            env,
            make_request(csv_payload(rows, header=header), dry_run=True),
        )

    assert excinfo.value.code == FORBIDDEN_INSTITUTION
    assert env.business_inserts == []
    assert env.ledger_inserts == []


def test_institution_value_is_written_server_side():
    env = Environment()
    request = make_request(csv_payload(), mode="insert")

    result = run(env, request)

    assert result.status == "imported"
    sql, params = env.business_inserts[0]
    assert "`ins_id_cd`" in sql
    assert params.count("00002000") == 2
    for row in DEFAULT_ROWS:
        assert row[1] not in ("00002000",)

    # The file never carried ins_id_cd; the server supplied one value per row.
    columns = import_settings().imports.tables["primary:app.report_results"].columns
    assert "ins_id_cd" not in columns


def test_raw_value_format_binds_the_platform_raw_value():
    settings = import_settings(value_format="raw")
    env = Environment()
    request = make_request(csv_payload(), institution=RAW_INSTITUTION)

    result = run(env, request, settings)

    assert result.status == "imported"
    _sql, params = env.business_inserts[0]
    assert "0800002000" in params
    assert "00002000" not in params


def test_leading_zeros_are_preserved_for_string_columns():
    env = Environment()
    rows = (("2026-01-01", "00123", "sales", "1.25", ""),)

    result = run(env, make_request(csv_payload(rows)))

    assert result.status == "imported"
    _sql, params = env.business_inserts[0]
    assert "00123" in params


# ---------------------------------------------------------------------------
# Upsert safety and counts
# ---------------------------------------------------------------------------


def test_upsert_is_refused_when_a_unique_index_lacks_the_institution():
    env = Environment(
        unique_indexes=(("PRIMARY", ("report_date", "branch_code")),)
    )

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), mode="upsert"))

    assert excinfo.value.code == TABLE_NOT_IMPORTABLE
    assert env.business_inserts == []
    assert env.business_selects == []


def test_upsert_is_refused_when_a_global_unique_key_exists():
    env = Environment(
        unique_indexes=(
            ("uk_metric", ("metric",)),
            ("PRIMARY", ("report_date", "branch_code", "ins_id_cd")),
        )
    )

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(), mode="upsert"))

    assert excinfo.value.code == TABLE_NOT_IMPORTABLE


def test_upsert_counts_inserted_and_updated_from_the_locked_select():
    env = Environment(existing_keys=1)
    rows = DEFAULT_ROWS + (("2026-01-03", "0003", "sales", "3.00", ""),)

    result = run(env, make_request(csv_payload(rows), mode="upsert"))

    assert result.status == "imported"
    assert result.row_count == 3
    assert result.inserted == 2
    assert result.updated == 1
    assert len(env.business_selects) == 1
    select_sql, select_params = env.business_selects[0]
    assert "FOR UPDATE" in select_sql
    assert "`ins_id_cd` = %s" in select_sql
    assert select_params[0] == "00002000"
    insert_sql, _insert_params = env.business_inserts[0]
    assert "ON DUPLICATE KEY UPDATE" in insert_sql
    assert "`metric` = VALUES(`metric`)" in insert_sql
    assert "`ins_id_cd` = VALUES(`ins_id_cd`)" not in insert_sql
    assert env.ledger_updates == [(2, 1, result.batch_id)]


def test_upsert_duplicate_key_in_file_fails_a_real_write():
    rows = (
        ("2026-01-01", "0001", "sales", "1.25", ""),
        ("2026-01-01", "0001", "cost", "2.50", ""),
    )

    with pytest.raises(BizDbError) as excinfo:
        run(Environment(), make_request(csv_payload(rows), mode="upsert"))

    assert excinfo.value.code == VALIDATION_FAILED
    assert "duplicate key tuple" in excinfo.value.message


def _key_rows(first_branch: str, second_branch: str) -> tuple[tuple[Any, ...], ...]:
    """Two rows with the same date and differing key-column values."""
    return (
        ("2026-01-01", first_branch, "sales", "1.25", ""),
        ("2026-01-01", second_branch, "cost", "2.50", ""),
    )


def test_upsert_key_dedupe_follows_a_case_insensitive_collation():
    env = Environment(
        columns=replace_column("branch_code", COLLATION_NAME="utf8mb4_0900_ai_ci")
    )
    rows = _key_rows("abc", "ABC")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    duplicates = [
        problem
        for problem in result.problems
        if "duplicate key tuple" in problem.reason
    ]
    assert [problem.row for problem in duplicates] == [2]
    assert "collation" in duplicates[0].reason
    reasons = " ".join(problem.reason for problem in result.problems).lower()
    assert "abc" not in reasons
    assert env.business_inserts == []
    assert env.ledger_inserts == []


def test_case_insensitive_duplicate_key_fails_a_real_write():
    env = Environment(
        columns=replace_column("branch_code", COLLATION_NAME="utf8mb4_0900_ai_ci")
    )
    rows = _key_rows("abc", "ABC")

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(rows), mode="upsert"))

    assert excinfo.value.code == VALIDATION_FAILED
    assert "duplicate key tuple" in excinfo.value.message
    assert "abc" not in str(excinfo.value).lower()
    assert env.business_inserts == []
    assert env.ledger_inserts == []


def test_upsert_key_dedupe_strips_trailing_spaces():
    env = Environment(
        columns=replace_column("branch_code", COLLATION_NAME="utf8mb4_0900_ai_ci")
    )
    rows = _key_rows("abc", "abc ")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    duplicates = [
        problem
        for problem in result.problems
        if "duplicate key tuple" in problem.reason
    ]
    assert [problem.row for problem in duplicates] == [2]
    assert "abc" not in " ".join(
        problem.reason for problem in result.problems
    ).lower()


def test_upsert_key_dedupe_ignores_accents_under_an_ai_collation():
    env = Environment(
        columns=replace_column("branch_code", COLLATION_NAME="utf8mb4_0900_ai_ci")
    )
    rows = _key_rows("é", "e")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    duplicates = [
        problem
        for problem in result.problems
        if "duplicate key tuple" in problem.reason
    ]
    assert [problem.row for problem in duplicates] == [2]
    assert "é" not in " ".join(problem.reason for problem in result.problems)


def test_binary_collation_keeps_case_distinct_keys():
    columns = replace_column("branch_code", COLLATION_NAME="utf8mb4_bin")
    rows = _key_rows("abc", "ABC")

    dry_run = run(
        Environment(columns=columns),
        make_request(csv_payload(rows), mode="upsert", dry_run=True),
    )

    assert dry_run.status == "dry_run"
    assert dry_run.problems == ()
    assert dry_run.row_count == 2

    env = Environment(columns=columns)
    result = run(env, make_request(csv_payload(rows), mode="upsert"))

    assert result.status == "imported"
    assert result.inserted == 2
    assert result.updated == 0
    assert len(env.business_inserts) == 1


def test_case_sensitive_collation_keeps_case_distinct_keys():
    env = Environment(
        columns=replace_column("branch_code", COLLATION_NAME="utf8mb4_0900_as_cs")
    )
    rows = _key_rows("abc", "ABC")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    assert result.problems == ()
    assert result.row_count == 2


def test_integer_key_columns_are_not_compared_through_the_collation():
    columns = replace_column(
        "branch_code",
        DATA_TYPE="int",
        COLUMN_TYPE="int",
        COLLATION_NAME="utf8mb4_0900_ai_ci",
    )
    env = Environment(columns=columns)
    rows = _key_rows("1", "2")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    assert result.problems == ()
    assert result.row_count == 2


def test_integer_key_columns_compare_the_validated_number():
    columns = replace_column(
        "branch_code",
        DATA_TYPE="int",
        COLUMN_TYPE="int",
        COLLATION_NAME="utf8mb4_0900_ai_ci",
    )
    env = Environment(columns=columns)
    rows = _key_rows("1", "01")

    result = run(env, make_request(csv_payload(rows), mode="upsert", dry_run=True))

    assert result.status == "dry_run"
    duplicates = [
        problem
        for problem in result.problems
        if "duplicate key tuple" in problem.reason
    ]
    assert [problem.row for problem in duplicates] == [2]


# ---------------------------------------------------------------------------
# Ledger duplicate and transaction outcomes
# ---------------------------------------------------------------------------


def test_duplicate_ledger_key_returns_the_committed_result():
    env = Environment(
        duplicate_ledger=True,
        existing_ledger={
            "batch_id": "b" * 32,
            "row_count": 2,
            "inserted_rows": 2,
            "updated_rows": 0,
        },
    )

    result = run(env, make_request(csv_payload(), mode="insert"))

    assert result.status == "duplicate"
    assert result.batch_id == "b" * 32
    assert result.row_count == 2
    assert result.inserted == 2
    assert result.updated == 0
    assert result.problems == ()
    assert env.business_inserts == []
    assert env.connection.rolled_back is True
    assert env.connection.committed is False


def test_duplicate_ledger_key_without_a_visible_row_is_a_database_error():
    env = Environment(duplicate_ledger=True, existing_ledger=None)

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == DATABASE_ERROR
    assert env.connection.rolled_back is True


def test_commit_failure_reports_an_unknown_outcome():
    env = Environment(fail_commit=True)
    request = make_request(csv_payload(), mode="insert")

    with pytest.raises(BizDbError) as excinfo:
        run(env, request)

    assert excinfo.value.code == IMPORT_OUTCOME_UNKNOWN
    assert "batch" in excinfo.value.message
    assert "retrying the same file is safe" in excinfo.value.message
    assert env.connection.committed is False
    # We never claim a rollback we cannot prove.
    assert env.connection.rolled_back is False


def test_business_duplicate_key_maps_to_validation_failed():
    env = Environment(
        fail_business=pymysql.err.IntegrityError(
            1062, "Duplicate entry 'SECRETCELL' for key 'uk_something'"
        )
    )

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == VALIDATION_FAILED
    assert "conflicts with existing data" in excinfo.value.message
    assert "SECRETCELL" not in str(excinfo.value)
    assert env.connection.rolled_back is True
    assert env.connection.committed is False


def test_other_business_errors_are_redacted_database_errors():
    env = Environment(fail_business=RuntimeError("SECRETCELL"))

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == DATABASE_ERROR
    assert "SECRETCELL" not in str(excinfo.value)
    assert env.connection.rolled_back is True


def test_insert_rowcount_mismatch_rolls_back():
    env = Environment(force_rowcount=99)

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()))

    assert excinfo.value.code == DATABASE_ERROR
    assert "row count did not match" in excinfo.value.message
    assert env.connection.rolled_back is True
    assert env.connection.committed is False


def test_deadline_expiry_after_begin_rolls_back(monkeypatch):
    env = Environment(expire_on_begin=True)
    monkeypatch.setattr(import_rows, "time", _Clock(env))

    with pytest.raises(BizDbError) as excinfo:
        run(
            env,
            make_request(csv_payload()),
            deadline=100.0,
        )

    assert excinfo.value.code == DEADLINE_EXCEEDED
    assert env.connection.begun is True
    assert env.connection.rolled_back is True
    assert env.connection.committed is False
    assert env.business_inserts == []


def test_deadline_already_passed_fails_before_connecting(monkeypatch):
    env = Environment()
    monkeypatch.setattr(import_rows, "time", _Clock(env))

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload()), deadline=0.0)

    assert excinfo.value.code == DEADLINE_EXCEEDED
    assert env.connect_calls == 0


def test_validation_errors_never_include_cell_values():
    env = Environment()
    rows = (("2026-01-01", "0001", "sales", "1.234", "SECRETCELLVALUE"),)

    with pytest.raises(BizDbError) as excinfo:
        run(env, make_request(csv_payload(rows), dry_run=False))

    assert excinfo.value.code == VALIDATION_FAILED
    assert "SECRETCELLVALUE" not in str(excinfo.value)
    assert "1.234" not in str(excinfo.value)
    assert "row 1 column value" in excinfo.value.message
    assert env.connection.rolled_back is False  # failed before the transaction


# ---------------------------------------------------------------------------
# Successful inserts and chunking
# ---------------------------------------------------------------------------


def test_insert_commits_ledger_and_business_rows_together():
    env = Environment()
    request = make_request(csv_payload(), mode="insert")

    result = run(env, request)

    assert result.status == "imported"
    assert result.database == "primary"
    assert result.table == "app.report_results"
    assert result.row_count == 2
    assert result.inserted == 2
    assert result.updated == 0
    assert result.batch_id is not None and len(result.batch_id) == 32
    assert result.columns == (
        "report_date",
        "branch_code",
        "metric",
        "value",
        "note",
        "ins_id_cd",
    )
    assert result.problems == ()
    assert env.connection.begun is True
    assert env.connection.committed is True
    assert env.connection.rolled_back is False
    assert env.ledger_inserts == [
        (
            import_rows_hint_key(request),
            result.batch_id,
            "00002000",
            "primary",
            "app.report_results",
            request.content_sha256,
            "insert",
            2,
        )
    ]
    assert env.ledger_updates == [(2, 0, result.batch_id)]
    sql, params = env.business_inserts[0]
    assert sql.startswith("INSERT INTO `app`.`report_results`")
    assert sql.count("(%s") == 2


def test_ledger_row_is_written_before_business_rows():
    env = Environment()

    run(env, make_request(csv_payload()))

    statements = [sql for sql, _params in env.executed]
    ledger_index = next(
        index
        for index, sql in enumerate(statements)
        if "INSERT INTO `bizdb_import_ledger`" in sql
    )
    business_index = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("INSERT INTO `app`.")
    )
    assert ledger_index < business_index


def test_insert_uses_chunks_of_at_most_500_rows():
    env = Environment()
    rows = tuple(
        ("2026-01-01", f"{index:04d}", "sales", "1.25", "")
        for index in range(501)
    )
    settings = import_settings(table_spec={"max_rows": 1000})

    result = run(env, make_request(csv_payload(rows)), settings)

    assert result.status == "imported"
    assert result.inserted == 501
    assert len(env.business_inserts) == 2
    assert env.business_inserts[0][0].count("(%s") == 500
    assert env.business_inserts[1][0].count("(%s") == 1


def import_rows_hint_key(request: ImportRequest) -> str:
    from biz_db_mcp.import_ledger import idempotency_key

    return idempotency_key(
        "00002000",
        "primary",
        "app.report_results",
        request.content_sha256,
        "insert",
    )
