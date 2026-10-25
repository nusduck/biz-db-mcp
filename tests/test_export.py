import base64
import csv
import hashlib
import json
import tempfile
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pymysql.constants import FIELD_TYPE, FLAG

from biz_db_mcp import errors
from biz_db_mcp.export import (
    MIME_TYPES,
    ColumnMeta,
    column_meta_from_cursor,
    export_filename,
    write_export,
)

MIB = 1024 * 1024


@pytest.fixture
def temp_dir(tmp_path, monkeypatch):
    """Point tempfile.mkstemp() at a per-test directory we can inspect."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    return tmp_path


def _column(
    name: str,
    type_code: int,
    *,
    precision: int | None = None,
    scale: int | None = None,
    unsigned: bool = False,
    binary: bool = False,
) -> ColumnMeta:
    return ColumnMeta(
        name=name,
        type_code=type_code,
        precision=precision,
        scale=scale,
        unsigned=unsigned,
        binary=binary,
    )


def _export(
    rows,
    columns,
    *,
    fmt="parquet",
    filename=None,
    max_rows=100_000,
    max_bytes=20 * MIB,
    batch_size=1000,
    deadline=None,
):
    return write_export(
        rows,
        columns,
        fmt=fmt,
        filename=filename or f"out.{fmt}",
        max_rows=max_rows,
        max_bytes=max_bytes,
        batch_size=batch_size,
        deadline=time.monotonic() + 60 if deadline is None else deadline,
    )


# ---------------------------------------------------------------------------
# Parquet schema and value mapping
# ---------------------------------------------------------------------------


def test_parquet_schema_is_fixed_before_the_first_batch(temp_dir):
    columns = [_column("id", FIELD_TYPE.LONG), _column("note", FIELD_TYPE.VAR_STRING)]

    def rows():
        for index in range(20_000):
            yield {"id": index, "note": None}
        yield {"id": 20_000, "note": "hello"}

    export = _export(rows(), columns, batch_size=20_000)
    table = pq.read_table(export.path)

    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("note").type == pa.string()
    assert table.num_rows == 20_001
    assert table.column("note")[19_999].as_py() is None
    assert table.column("note")[20_000].as_py() == "hello"
    assert export.row_count == 20_001
    assert export.filename == "out.parquet"
    assert export.mime_type == MIME_TYPES["parquet"]
    assert export.size == export.path.stat().st_size
    assert export.sha256 == hashlib.sha256(export.path.read_bytes()).hexdigest()


def test_parquet_preserves_decimal_precision(temp_dir):
    columns = [_column("amount", FIELD_TYPE.NEWDECIMAL, precision=38, scale=6)]
    value = Decimal("12345678901234567890.123456")

    export = _export([{"amount": value}], columns)
    table = pq.read_table(export.path)

    assert table.schema.field("amount").type == pa.decimal128(38, 6)
    assert table.column("amount")[0].as_py() == value


def test_parquet_uses_decimal256_above_38_digits(temp_dir):
    columns = [_column("amount", FIELD_TYPE.NEWDECIMAL, precision=46, scale=6)]
    value = Decimal("1" * 40 + "." + "2" * 6)

    export = _export([{"amount": value}], columns)
    table = pq.read_table(export.path)

    assert table.schema.field("amount").type == pa.decimal256(46, 6)
    assert table.column("amount")[0].as_py() == value


def test_parquet_falls_back_to_string_for_unknown_decimal_precision(temp_dir):
    columns = [_column("amount", FIELD_TYPE.NEWDECIMAL)]

    export = _export([{"amount": Decimal("1.25")}], columns)
    table = pq.read_table(export.path)

    assert table.schema.field("amount").type == pa.string()
    assert table.column("amount")[0].as_py() == "1.25"


def test_parquet_keeps_unsigned_bigint_range(temp_dir):
    columns = [_column("big", FIELD_TYPE.LONGLONG, unsigned=True)]

    export = _export([{"big": 2**64 - 1}], columns)
    table = pq.read_table(export.path)

    assert table.schema.field("big").type == pa.uint64()
    assert table.column("big")[0].as_py() == 2**64 - 1


def test_parquet_handles_temporal_and_binary_columns(temp_dir):
    columns = [
        _column("d", FIELD_TYPE.DATE),
        _column("dt", FIELD_TYPE.DATETIME),
        _column("t", FIELD_TYPE.TIME),
        _column("payload", FIELD_TYPE.BLOB, binary=True),
    ]
    row = {
        "d": date(2024, 1, 2),
        "dt": datetime(2024, 1, 2, 3, 4, 5, 123456),
        "t": timedelta(hours=27, minutes=2, seconds=3, microseconds=4),
        "payload": b"\x00\xff",
    }

    export = _export([row], columns)
    table = pq.read_table(export.path)

    assert table.schema.field("d").type == pa.date32()
    assert table.schema.field("dt").type == pa.timestamp("us")
    assert table.schema.field("t").type == pa.duration("us")
    assert table.schema.field("payload").type == pa.binary()
    assert table.column("d")[0].as_py() == date(2024, 1, 2)
    assert table.column("dt")[0].as_py() == datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert table.column("t")[0].as_py() == timedelta(
        hours=27, minutes=2, seconds=3, microseconds=4
    )
    assert table.column("payload")[0].as_py() == b"\x00\xff"


def test_parquet_zero_rows_still_has_the_schema(temp_dir):
    columns = [_column("id", FIELD_TYPE.LONG), _column("name", FIELD_TYPE.VAR_STRING)]

    export = _export([], columns)
    table = pq.read_table(export.path)

    assert export.row_count == 0
    assert table.num_rows == 0
    assert table.schema.field("id").type == pa.int64()
    assert table.schema.field("name").type == pa.string()


def test_duplicate_column_names_are_rejected(temp_dir):
    columns = [_column("id", FIELD_TYPE.LONG), _column("id", FIELD_TYPE.LONG)]

    with pytest.raises(errors.BizDbError) as excinfo:
        _export([{"id": 1}], columns)

    assert excinfo.value.code == errors.INVALID_ARGUMENT
    assert list(temp_dir.iterdir()) == []


# ---------------------------------------------------------------------------
# CSV and JSONL
# ---------------------------------------------------------------------------


def test_csv_writes_the_header_for_zero_rows(temp_dir):
    columns = [_column("id", FIELD_TYPE.LONG), _column("name", FIELD_TYPE.VAR_STRING)]

    export = _export([], columns, fmt="csv")

    assert export.row_count == 0
    assert export.path.read_bytes() == b"id,name\r\n"
    assert export.mime_type == MIME_TYPES["csv"]


def test_csv_formats_values_without_a_bom(temp_dir):
    columns = [
        _column("empty", FIELD_TYPE.VAR_STRING),
        _column("amount", FIELD_TYPE.NEWDECIMAL, precision=10, scale=4),
        _column("payload", FIELD_TYPE.BLOB, binary=True),
        _column("dt", FIELD_TYPE.DATETIME),
        _column("d", FIELD_TYPE.DATE),
        _column("t", FIELD_TYPE.TIME),
        _column("flag", FIELD_TYPE.TINY),
    ]
    row = {
        "empty": None,
        "amount": Decimal("12.3400"),
        "payload": b"\x00\x01",
        "dt": datetime(2024, 1, 2, 3, 4, 5),
        "d": date(2024, 1, 2),
        "t": timedelta(seconds=-3661, microseconds=5),
        "flag": True,
    }

    export = _export([row], columns, fmt="csv")
    raw = export.path.read_bytes()

    assert not raw.startswith(b"\xef\xbb\xbf")
    records = list(csv.reader(raw.decode("utf-8").splitlines()))
    assert records[0] == ["empty", "amount", "payload", "dt", "d", "t", "flag"]
    assert records[1] == [
        "",
        "12.3400",
        base64.b64encode(b"\x00\x01").decode("ascii"),
        "2024-01-02T03:04:05",
        "2024-01-02",
        # -3661s + 5us == -3660.999995s, formatted with hours > 24 allowed.
        "-01:01:00.999995",
        "1",
    ]


def test_jsonl_formats_decimal_bytes_and_temporal_values(temp_dir):
    columns = [
        _column("amount", FIELD_TYPE.NEWDECIMAL, precision=10, scale=2),
        _column("payload", FIELD_TYPE.BLOB, binary=True),
        _column("d", FIELD_TYPE.DATE),
        _column("dt", FIELD_TYPE.DATETIME),
        _column("t", FIELD_TYPE.TIME),
    ]
    row = {
        "amount": Decimal("12.34"),
        "payload": b"\x00\xff",
        "d": date(2024, 1, 2),
        "dt": datetime(2024, 1, 2, 3, 4, 5),
        "t": timedelta(hours=27, minutes=2, seconds=3, microseconds=4),
    }

    export = _export([row], columns, fmt="jsonl")
    lines = export.path.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert list(payload) == ["amount", "payload", "d", "dt", "t"]
    assert payload["amount"] == "12.34"
    assert payload["payload"] == base64.b64encode(b"\x00\xff").decode("ascii")
    assert payload["d"] == "2024-01-02"
    assert payload["dt"] == "2024-01-02T03:04:05"
    assert payload["t"] == "27:02:03.000004"
    assert export.mime_type == MIME_TYPES["jsonl"]


def test_jsonl_zero_rows_is_an_empty_file(temp_dir):
    export = _export([], [_column("id", FIELD_TYPE.LONG)], fmt="jsonl")

    assert export.row_count == 0
    assert export.path.read_bytes() == b""


# ---------------------------------------------------------------------------
# Limits and cleanup
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", ["parquet", "csv", "jsonl"])
def test_reiterable_rows_are_consumed_exactly_once(temp_dir, fmt):
    rows = [{"id": index} for index in range(4)]

    export = _export(rows, [_column("id", FIELD_TYPE.LONG)], fmt=fmt, batch_size=2)

    assert export.row_count == 4
    if fmt == "parquet":
        values = pq.read_table(export.path).column("id").to_pylist()
    elif fmt == "csv":
        records = list(csv.reader(export.path.read_text(encoding="utf-8").splitlines()))
        values = [int(record[0]) for record in records[1:]]
    else:
        lines = export.path.read_text(encoding="utf-8").splitlines()
        values = [json.loads(line)["id"] for line in lines]
    assert values == [0, 1, 2, 3]


def test_exactly_max_rows_succeeds(temp_dir):
    rows = [{"id": index} for index in range(3)]

    export = _export(rows, [_column("id", FIELD_TYPE.LONG)], fmt="csv", max_rows=3, batch_size=2)

    assert export.row_count == 3


def test_max_rows_exceeded_stops_the_iterator_and_removes_the_file(temp_dir):
    consumed = 0

    def rows():
        nonlocal consumed
        for index in range(100):
            consumed += 1
            yield {"id": index}

    with pytest.raises(errors.BizDbError) as excinfo:
        _export(rows(), [_column("id", FIELD_TYPE.LONG)], max_rows=5, batch_size=4)

    assert excinfo.value.code == errors.TOO_LARGE
    # Only the row that proved the overflow is read beyond the limit.
    assert consumed == 6
    assert list(temp_dir.iterdir()) == []


def test_max_bytes_exceeded_removes_the_file(temp_dir):
    columns = [_column("payload", FIELD_TYPE.VAR_STRING)]
    rows = [{"payload": "x" * 4096} for _ in range(10)]

    with pytest.raises(errors.BizDbError) as excinfo:
        _export(rows, columns, fmt="csv", max_bytes=1024, batch_size=1)

    assert excinfo.value.code == errors.TOO_LARGE
    assert list(temp_dir.iterdir()) == []


def test_max_bytes_exceeded_for_parquet_removes_the_file(temp_dir):
    columns = [_column("payload", FIELD_TYPE.VAR_STRING)]

    with pytest.raises(errors.BizDbError) as excinfo:
        _export([{"payload": "x" * 4096}], columns, max_bytes=64)

    assert excinfo.value.code == errors.TOO_LARGE
    assert list(temp_dir.iterdir()) == []


def test_iterator_failure_removes_the_file(temp_dir):
    def rows():
        yield {"id": 1}
        raise RuntimeError("stream failed")

    with pytest.raises(RuntimeError, match="stream failed"):
        _export(rows(), [_column("id", FIELD_TYPE.LONG)], fmt="csv")

    assert list(temp_dir.iterdir()) == []


def test_deadline_exceeded_removes_the_file(temp_dir):
    consumed = 0

    def rows():
        nonlocal consumed
        for index in range(100):
            consumed += 1
            yield {"id": index}

    with pytest.raises(errors.BizDbError) as excinfo:
        _export(
            rows(),
            [_column("id", FIELD_TYPE.LONG)],
            batch_size=2,
            deadline=time.monotonic() - 1,
        )

    assert excinfo.value.code == errors.DEADLINE_EXCEEDED
    assert consumed == 0
    assert list(temp_dir.iterdir()) == []


# ---------------------------------------------------------------------------
# Filenames
# ---------------------------------------------------------------------------


def test_export_filename_accepts_cjk_and_strips_one_known_extension():
    assert export_filename("订单_2024", "parquet") == "订单_2024.parquet"
    assert export_filename("orders.parquet", "parquet") == "orders.parquet"
    assert export_filename("orders.parquet", "csv") == "orders.csv"
    assert export_filename("  report.csv ", "jsonl") == "report.jsonl"
    assert export_filename("Q1 report-2024_final", "csv") == "Q1 report-2024_final.csv"
    assert export_filename("x" * 120, "csv") == "x" * 120 + ".csv"


@pytest.mark.parametrize(
    "dataset_name",
    [
        "../x",
        "a/b",
        ".hidden",
        "",
        "   ",
        ".",
        "..",
        "a\\b",
        "a:b",
        "a\x00b",
        "a\nb",
        "x" * 121,
    ],
)
def test_export_filename_rejects_unsafe_names(dataset_name):
    with pytest.raises(errors.BizDbError) as excinfo:
        export_filename(dataset_name, "csv")

    assert excinfo.value.code == errors.INVALID_ARGUMENT


# ---------------------------------------------------------------------------
# Cursor metadata
# ---------------------------------------------------------------------------


def _field(
    name,
    type_code,
    *,
    flags=0,
    charsetnr=33,
    length=0,
    scale=0,
):
    return SimpleNamespace(
        name=name,
        type_code=type_code,
        flags=flags,
        charsetnr=charsetnr,
        length=length,
        scale=scale,
    )


def test_column_meta_from_cursor_reads_pymysql_fields():
    cursor = SimpleNamespace(
        _result=SimpleNamespace(
            fields=[
                _field("id", FIELD_TYPE.LONG, charsetnr=63, length=11),
                _field("amount", FIELD_TYPE.NEWDECIMAL, charsetnr=63, length=12, scale=2),
                _field(
                    "total",
                    FIELD_TYPE.NEWDECIMAL,
                    flags=FLAG.UNSIGNED,
                    charsetnr=63,
                    length=11,
                    scale=2,
                ),
                _field("note", FIELD_TYPE.VAR_STRING, charsetnr=63, length=255),
                _field(
                    "name",
                    FIELD_TYPE.VAR_STRING,
                    flags=FLAG.BINARY,
                    charsetnr=33,
                    length=255,
                ),
                _field("raw", FIELD_TYPE.BLOB, charsetnr=63, length=255),
            ]
        ),
        description=None,
    )

    metas = column_meta_from_cursor(cursor)

    assert [meta.name for meta in metas] == ["id", "amount", "total", "note", "name", "raw"]
    # DECIMAL(10, 2) signed: wire length 10 + 1 sign + 1 point = 12.
    assert metas[1] == ColumnMeta("amount", FIELD_TYPE.NEWDECIMAL, 10, 2, False, False)
    # DECIMAL(10, 2) unsigned: wire length 10 + 0 sign + 1 point = 11.
    assert metas[2] == ColumnMeta("total", FIELD_TYPE.NEWDECIMAL, 10, 2, True, False)
    # Binary charset on a numeric column does not make its values bytes.
    assert metas[0].binary is False
    assert metas[3].binary is True
    assert metas[4].binary is True
    assert metas[5].binary is True


def test_column_meta_from_cursor_falls_back_to_description():
    cursor = SimpleNamespace(
        _result=None,
        description=(
            ("id", FIELD_TYPE.LONG, None, 11, 11, 0, False),
            ("amount", FIELD_TYPE.NEWDECIMAL, None, 12, 12, 2, True),
            ("payload", FIELD_TYPE.BLOB, None, 255, 255, 0, True),
        ),
    )

    metas = column_meta_from_cursor(cursor)

    assert metas[0] == ColumnMeta("id", FIELD_TYPE.LONG, scale=0)
    assert metas[1].precision == 10
    assert metas[1].scale == 2
    assert metas[1].unsigned is False
    assert metas[2].binary is True


def test_column_meta_from_cursor_without_metadata_is_empty():
    assert column_meta_from_cursor(SimpleNamespace(_result=None, description=None)) == []
