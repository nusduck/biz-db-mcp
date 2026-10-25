"""Stream one SELECT result into a bounded Parquet, CSV, or JSONL file.

The server no longer uploads exports anywhere: :func:`write_export` only
materialises a local temporary file, bounded by rows, bytes, and a monotonic
deadline.  ``mcp_files`` then embeds that file in the MCP tool result as a
standard-base64 resource blob; the platform saves it into the caller's
workspace and takes the filename from the last path segment of the resource
URI.

Parquet schemas are fixed from :class:`ColumnMeta` *before* the first row is
consumed, so a column that is NULL in the first batch cannot lock the file
into a `null` Arrow type.  CSV and JSONL have no schema, so they stream
row-by-row within each batch.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import tempfile
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq
from pymysql.constants import FIELD_TYPE, FLAG

from . import errors

ExportFormat = Literal["parquet", "csv", "jsonl"]

MIME_TYPES: dict[str, str] = {
    "parquet": "application/vnd.apache.parquet",
    "csv": "text/csv",
    "jsonl": "application/x-ndjson",
}

_FILE_EXTENSIONS: dict[str, str] = {
    "parquet": ".parquet",
    "csv": ".csv",
    "jsonl": ".jsonl",
}
_KNOWN_EXTENSIONS = (".parquet", ".csv", ".jsonl")
TEMP_PREFIX = "bizdb-export-"
_MAX_FILENAME_STEM = 120
_ALLOWED_PUNCTUATION = "._- "
_BINARY_CHARSET = 63

# MySQL type families used by the Arrow mapping and by column metadata.
_INT64_TYPES = frozenset({FIELD_TYPE.TINY, FIELD_TYPE.SHORT, FIELD_TYPE.INT24, FIELD_TYPE.LONG})
_DECIMAL_TYPES = frozenset({FIELD_TYPE.DECIMAL, FIELD_TYPE.NEWDECIMAL})
_ALWAYS_BINARY_TYPES = frozenset({FIELD_TYPE.BIT, FIELD_TYPE.GEOMETRY})
_BLOB_TYPES = frozenset(
    {FIELD_TYPE.TINY_BLOB, FIELD_TYPE.MEDIUM_BLOB, FIELD_TYPE.LONG_BLOB, FIELD_TYPE.BLOB}
)
_TEXT_TYPES = (
    frozenset({FIELD_TYPE.VARCHAR, FIELD_TYPE.VAR_STRING, FIELD_TYPE.STRING}) | _BLOB_TYPES
)


@dataclass(frozen=True)
class ColumnMeta:
    """One result column, as needed to fix an export schema up front."""

    name: str
    type_code: int
    precision: int | None = None
    scale: int | None = None
    unsigned: bool = False
    binary: bool = False


@dataclass(frozen=True)
class ExportFile:
    """A finished local export file plus the metadata the tool result reports."""

    path: Path
    filename: str
    format: ExportFormat
    mime_type: str
    row_count: int
    size: int
    sha256: str


# ---------------------------------------------------------------------------
# Column metadata
# ---------------------------------------------------------------------------


def column_meta_from_cursor(cursor: Any) -> list[ColumnMeta]:
    """Describe *cursor*'s result columns after ``execute()``.

    pymysql keeps the ``Field`` descriptors on ``cursor._result.fields`` for
    both buffered and unbuffered (``SSCursor``) cursors, so they are preferred:
    they carry the unsigned/binary flags and the binary charset number that
    ``cursor.description`` lacks.  ``cursor.description`` is the DB-API
    fallback for cursors that expose only the 7-tuple form.

    Decimal precision is derived from the wire column length: MySQL reports
    ``length`` as ``precision + 1`` (the sign byte, omitted for UNSIGNED)
    ``+ 1`` (the decimal point, only when scale > 0).  pymysql's own
    ``Field.description()`` copies that raw length into both ``internal_size``
    and ``precision`` (it carries a TODO there), so it overstates a DECIMAL's
    precision by one or two digits.  Inverting the wire layout recovers the
    declared precision:

        precision = length - (1 if scale > 0 else 0) - (0 if unsigned else 1)

    The ``description`` fallback has no unsigned flag, so it assumes the signed
    layout; it also cannot distinguish TEXT from BLOB, so BLOB-family type
    codes are treated as binary and VARCHAR/STRING as text.
    """
    result = getattr(cursor, "_result", None)
    fields = getattr(result, "fields", None)
    if fields:
        return [_column_meta_from_field(field) for field in fields]
    description = getattr(cursor, "description", None) or ()
    return [_column_meta_from_description(entry) for entry in description]


def _column_meta_from_field(field: Any) -> ColumnMeta:
    type_code = _optional_int(getattr(field, "type_code", None))
    if type_code is None:
        type_code = FIELD_TYPE.NULL
    flags = _optional_int(getattr(field, "flags", None)) or 0
    scale = _optional_int(getattr(field, "scale", None))
    length = _optional_int(getattr(field, "length", None))
    unsigned = bool(flags & FLAG.UNSIGNED)
    precision = None
    if type_code in _DECIMAL_TYPES and length is not None:
        precision = _decimal_precision(length, scale, unsigned)
    charsetnr = _optional_int(getattr(field, "charsetnr", None))
    return ColumnMeta(
        name=str(getattr(field, "name", "")),
        type_code=type_code,
        precision=precision,
        scale=scale,
        unsigned=unsigned,
        binary=_field_is_binary(type_code, flags, charsetnr),
    )


def _column_meta_from_description(entry: Sequence[Any]) -> ColumnMeta:
    type_code = int(entry[1]) if len(entry) > 1 and entry[1] is not None else FIELD_TYPE.NULL
    internal_size = _optional_int(entry[3]) if len(entry) > 3 else None
    scale = _optional_int(entry[5]) if len(entry) > 5 else None
    precision = None
    if type_code in _DECIMAL_TYPES and internal_size is not None:
        precision = _decimal_precision(internal_size, scale, False)
    return ColumnMeta(
        name=str(entry[0]) if entry else "",
        type_code=type_code,
        precision=precision,
        scale=scale,
        unsigned=False,
        binary=type_code in _ALWAYS_BINARY_TYPES or type_code in _BLOB_TYPES,
    )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_precision(length: int, scale: int | None, unsigned: bool) -> int | None:
    precision = length
    if scale is not None and scale > 0:
        precision -= 1
    if not unsigned:
        precision -= 1
    return precision if precision > 0 else None


def _field_is_binary(type_code: int, flags: int, charsetnr: int | None) -> bool:
    if type_code in _ALWAYS_BINARY_TYPES:
        return True
    if type_code not in _TEXT_TYPES:
        # Numeric and temporal columns also use the binary charset; that says
        # nothing about their values being bytes.
        return False
    if flags & FLAG.BINARY:
        return True
    return charsetnr == _BINARY_CHARSET


# ---------------------------------------------------------------------------
# Filenames
# ---------------------------------------------------------------------------


def export_filename(dataset_name: str, fmt: ExportFormat) -> str:
    """Return a safe ``<name><extension>`` filename for one export.

    One known extension (case-insensitive) is stripped from *dataset_name*
    first, so ``"orders.parquet"`` with ``fmt="csv"`` becomes ``"orders.csv"``
    rather than ``"orders.parquet.csv"``.  Names may contain letters, digits
    (including CJK, via ``str.isalnum()``), ``.``, ``_``, ``-``, and spaces;
    anything that could escape the platform's output directory or break the
    resource URI is refused.
    """
    if fmt not in _FILE_EXTENSIONS:
        raise errors.BizDbError(
            errors.INVALID_ARGUMENT,
            f"unsupported export format {fmt!r}; use parquet, csv, or jsonl",
        )
    if not isinstance(dataset_name, str):
        raise errors.BizDbError(errors.INVALID_ARGUMENT, "dataset_name must be a string")
    name = dataset_name.strip()
    lowered = name.lower()
    for extension in _KNOWN_EXTENSIONS:
        if lowered.endswith(extension):
            name = name[: -len(extension)]
            break
    _validate_export_name(name, dataset_name)
    return f"{name}{_FILE_EXTENSIONS[fmt]}"


def _validate_export_name(name: str, original: Any) -> None:
    if not name or name in (".", ".."):
        raise errors.BizDbError(
            errors.INVALID_ARGUMENT, f"dataset name {original!r} must contain a filename"
        )
    if name.startswith("."):
        raise errors.BizDbError(
            errors.INVALID_ARGUMENT, f"dataset name {original!r} must not start with '.'"
        )
    if len(name) > _MAX_FILENAME_STEM:
        raise errors.BizDbError(
            errors.INVALID_ARGUMENT,
            f"dataset name is longer than {_MAX_FILENAME_STEM} characters",
        )
    for character in name:
        allowed = character.isalnum() or character in _ALLOWED_PUNCTUATION
        if character in "/\\:" or not character.isprintable() or not allowed:
            raise errors.BizDbError(
                errors.INVALID_ARGUMENT,
                f"dataset name {original!r} contains unsupported characters; "
                "use letters, digits, '.', '_', '-', or spaces",
            )


# ---------------------------------------------------------------------------
# Staging directory
# ---------------------------------------------------------------------------


def prepare_temp_dir(temp_dir: str | None, *, stale_seconds: int) -> int:
    """Create the staging directory and delete stale export files; return count removed.

    Files are normally removed as soon as the tool result is built; leftovers
    only exist after a crash or kill. Only this service's own prefix is touched.
    """
    directory = Path(temp_dir) if temp_dir else Path(tempfile.gettempdir())
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    cutoff = time.time() - stale_seconds
    removed = 0
    for path in directory.glob(f"{TEMP_PREFIX}*"):
        try:
            if path.is_file() and not path.is_symlink() and path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        except OSError:
            continue
    return removed


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def write_export(
    rows: Iterable[dict[str, Any]],
    columns: Sequence[ColumnMeta],
    *,
    fmt: ExportFormat,
    filename: str,
    max_rows: int,
    max_bytes: int,
    batch_size: int,
    deadline: float,
    temp_dir: str | None = None,
) -> ExportFile:
    """Stream *rows* into a fresh temporary file and return its metadata.

    *rows* is consumed once, in batches of *batch_size*, and is abandoned as
    soon as the row limit, byte limit, or deadline is exceeded: the iterator
    is never drained and the partial file is deleted.  *deadline* is a
    ``time.monotonic()`` value.

    Parquet uses a schema fixed from *columns*; CSV writes a header even for
    zero rows; JSONL writes one object per row and leaves the file empty when
    there are no rows.  On any exception the temporary file is removed before
    the exception propagates.
    """
    _validate_write_arguments(
        fmt=fmt, max_rows=max_rows, max_bytes=max_bytes, batch_size=batch_size
    )
    _reject_duplicate_columns(columns)

    fd, raw_path = tempfile.mkstemp(
        prefix=TEMP_PREFIX, suffix=_FILE_EXTENSIONS[fmt], dir=temp_dir
    )
    os.close(fd)
    path = Path(raw_path)
    try:
        batches = _iter_batches(
            rows, batch_size=batch_size, max_rows=max_rows, deadline=deadline
        )
        if fmt == "parquet":
            row_count = _write_parquet(
                path, batches, columns, _parquet_schema(columns), max_bytes=max_bytes
            )
        elif fmt == "csv":
            row_count = _write_csv(path, batches, columns, max_bytes=max_bytes)
        else:
            row_count = _write_jsonl(path, batches, columns, max_bytes=max_bytes)

        size = path.stat().st_size
        _ensure_within_bytes(size, max_bytes)
        return ExportFile(
            path=path,
            filename=filename,
            format=fmt,
            mime_type=MIME_TYPES[fmt],
            row_count=row_count,
            size=size,
            sha256=_sha256(path),
        )
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _validate_write_arguments(
    *, fmt: str, max_rows: int, max_bytes: int, batch_size: int
) -> None:
    if fmt not in MIME_TYPES:
        raise errors.BizDbError(
            errors.INVALID_ARGUMENT,
            f"unsupported export format {fmt!r}; use parquet, csv, or jsonl",
        )
    if max_rows < 0:
        raise errors.BizDbError(errors.INVALID_ARGUMENT, "max_rows must not be negative")
    if max_bytes < 0:
        raise errors.BizDbError(errors.INVALID_ARGUMENT, "max_bytes must not be negative")
    if batch_size < 1:
        raise errors.BizDbError(errors.INVALID_ARGUMENT, "batch_size must be a positive integer")


def _reject_duplicate_columns(columns: Sequence[ColumnMeta]) -> None:
    seen: set[str] = set()
    for column in columns:
        if column.name in seen:
            raise errors.BizDbError(
                errors.INVALID_ARGUMENT,
                f"duplicate column name {column.name!r} in the export result; "
                "alias duplicated columns in the SELECT",
            )
        seen.add(column.name)


def _iter_batches(
    rows: Iterable[dict[str, Any]],
    *,
    batch_size: int,
    max_rows: int,
    deadline: float,
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield ``(rows_seen, batch)`` while enforcing the deadline and row cap.

    The deadline is checked at every batch boundary and the row cap just before
    a row would be buffered, so the caller can stop without draining *rows*.
    """
    row_count = 0
    iterator = iter(rows)
    while True:
        _ensure_within_deadline(deadline)
        batch: list[dict[str, Any]] = []
        exhausted = True
        for row in iterator:
            row_count += 1
            if row_count > max_rows:
                raise errors.BizDbError(
                    errors.TOO_LARGE,
                    f"export exceeds max_rows={max_rows}; narrow the query or raise the limit",
                )
            batch.append(row)
            if len(batch) >= batch_size:
                exhausted = False
                break
        if not batch:
            return
        yield row_count, batch
        if exhausted:
            return


def _ensure_within_deadline(deadline: float) -> None:
    if time.monotonic() > deadline:
        raise errors.BizDbError(
            errors.DEADLINE_EXCEEDED,
            "export exceeded its time limit before finishing; narrow the query and retry",
        )


def _ensure_within_bytes(size: int, max_bytes: int) -> None:
    if size > max_bytes:
        raise errors.BizDbError(
            errors.TOO_LARGE,
            f"export exceeds max_bytes={max_bytes}; narrow the selected columns or raise the limit",
        )


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------


def _parquet_schema(columns: Sequence[ColumnMeta]) -> pa.Schema:
    return pa.schema([(column.name, _arrow_type(column)) for column in columns])


def _arrow_type(column: ColumnMeta) -> pa.DataType:
    """Map one MySQL type code to the fixed Arrow type for the whole file."""
    code = column.type_code
    if code in _INT64_TYPES:
        # MySQL's 32-bit integers fit in int64; UNSIGNED at this width also
        # fits, so no separate unsigned mapping is needed.
        return pa.int64()
    if code == FIELD_TYPE.LONGLONG:
        return pa.uint64() if column.unsigned else pa.int64()
    if code == FIELD_TYPE.YEAR:
        return pa.int16()
    if code in _DECIMAL_TYPES:
        return _decimal_type(column)
    if code == FIELD_TYPE.FLOAT:
        return pa.float32()
    if code == FIELD_TYPE.DOUBLE:
        return pa.float64()
    if code == FIELD_TYPE.DATE:
        return pa.date32()
    if code in (FIELD_TYPE.DATETIME, FIELD_TYPE.TIMESTAMP):
        return pa.timestamp("us")
    if code == FIELD_TYPE.TIME:
        return pa.duration("us")
    if code == FIELD_TYPE.BIT:
        return pa.binary()
    if code in (FIELD_TYPE.JSON, FIELD_TYPE.ENUM, FIELD_TYPE.SET):
        return pa.string()
    if code == FIELD_TYPE.GEOMETRY:
        return pa.binary()
    if code in _TEXT_TYPES:
        return pa.binary() if column.binary else pa.string()
    # FIELD_TYPE.NULL and unknown/computed types keep their text form.
    return pa.string()


def _decimal_type(column: ColumnMeta) -> pa.DataType:
    precision = column.precision
    if not precision:
        # Precision unknown: keep the exact decimal text rather than guess.
        return pa.string()
    scale = column.scale if column.scale is not None and column.scale >= 0 else 0
    if precision > 38:
        return pa.decimal256(precision, scale)
    return pa.decimal128(precision, scale)


def _table_from_rows(
    batch: Sequence[dict[str, Any]],
    columns: Sequence[ColumnMeta],
    schema: pa.Schema,
) -> pa.Table:
    arrays: list[pa.Array] = []
    for index, column in enumerate(columns):
        arrow_type = schema.field(index).type
        values = [_coerce_value(row.get(column.name), arrow_type) for row in batch]
        try:
            arrays.append(pa.array(values, type=arrow_type))
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
            raise errors.BizDbError(
                errors.DATABASE_ERROR,
                f"cannot encode column {column.name!r} as {arrow_type} for export",
            ) from exc
    return pa.Table.from_arrays(arrays, schema=schema)


def _coerce_value(value: Any, arrow_type: pa.DataType) -> Any:
    """Adapt one driver value to the fixed Arrow type without re-inferring it."""
    if value is None:
        return None
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        # pyarrow UTF-8 encodes str values for binary columns; this only
        # happens for TEXT/BLOB columns described via cursor.description.
        return value
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode("utf-8", errors="replace")
        if isinstance(value, Decimal):
            return str(value)
        return value if isinstance(value, str) else str(value)
    if pa.types.is_boolean(arrow_type):
        return bool(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_decimal(arrow_type):
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime_time.min)
        return value
    if pa.types.is_date(arrow_type):
        return value.date() if isinstance(value, datetime) else value
    return value


def _write_parquet(
    path: Path,
    batches: Iterable[tuple[int, list[dict[str, Any]]]],
    columns: Sequence[ColumnMeta],
    schema: pa.Schema,
    *,
    max_bytes: int,
) -> int:
    row_count = 0
    sink = pa.OSFile(str(path), "wb")
    try:
        writer = pq.ParquetWriter(sink, schema)
        try:
            for row_count, batch in batches:
                writer.write_table(_table_from_rows(batch, columns, schema))
                # tell() reflects the row groups already flushed, so an
                # oversized file is caught before the next batch is buffered.
                _ensure_within_bytes(sink.tell(), max_bytes)
        except BaseException:
            try:
                writer.close()
            except Exception:
                pass
            raise
        writer.close()
    finally:
        sink.close()
    return row_count


# ---------------------------------------------------------------------------
# CSV and JSONL
# ---------------------------------------------------------------------------


def _write_csv(
    path: Path,
    batches: Iterable[tuple[int, list[dict[str, Any]]]],
    columns: Sequence[ColumnMeta],
    *,
    max_bytes: int,
) -> int:
    names = [column.name for column in columns]
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(names)
        handle.flush()
        _ensure_within_bytes(_handle_size(handle), max_bytes)
        for row_count, batch in batches:
            for row in batch:
                writer.writerow([_csv_value(row.get(name)) for name in names])
            handle.flush()
            _ensure_within_bytes(_handle_size(handle), max_bytes)
    return row_count


def _write_jsonl(
    path: Path,
    batches: Iterable[tuple[int, list[dict[str, Any]]]],
    columns: Sequence[ColumnMeta],
    *,
    max_bytes: int,
) -> int:
    names = [column.name for column in columns]
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        for row_count, batch in batches:
            for row in batch:
                handle.write(_json_line(row, names))
            handle.flush()
            _ensure_within_bytes(_handle_size(handle), max_bytes)
    return row_count


def _handle_size(handle: Any) -> int:
    return os.fstat(handle.fileno()).st_size


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _format_duration(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _json_line(row: dict[str, Any], names: Sequence[str]) -> str:
    payload = {name: _json_value(row.get(name)) for name in names}
    return json.dumps(payload, ensure_ascii=False, default=str) + "\n"


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return _format_duration(value)
    return value


def _format_duration(value: timedelta) -> str:
    """Format a timedelta as ``[-]HH:MM:SS[.ffffff]`` (hours may exceed 24)."""
    total_microseconds = (
        (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
    )
    sign = "-" if total_microseconds < 0 else ""
    hours, remainder = divmod(abs(total_microseconds), 3_600_000_000)
    minutes, remainder = divmod(remainder, 60_000_000)
    seconds, microseconds = divmod(remainder, 1_000_000)
    text = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    if microseconds:
        text += f".{microseconds:06d}"
    return text


# ---------------------------------------------------------------------------
# File metadata
# ---------------------------------------------------------------------------


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
