"""Guarded, whitelisted file-to-table import for ``import_rows``.

The model never writes SQL here. The platform hands over an artifact as
canonical base64 plus its SHA-256; the operator whitelists exactly which
table, columns, and modes may be written; and the caller's institution is
assigned server-side into the institution column. Generic DML stays disabled.

Security order implemented by :func:`run_import`:

1. authorization (config flags, whitelist, institution, mode) before parsing
   or any database access;
2. bounded base64 decode, digest, and filename validation;
3. format resolution and content sniffing;
4. bounded parsing (rows, columns, field sizes) into header plus rows;
5. read-only metadata checks through the restricted import account;
6. column rules and institution binding;
7. upsert unique-index safety;
8. strict per-cell type validation with no silent coercion;
9. dry-run result, or one transaction with the ledger row, business writes,
   and a commit whose failure is reported as ``IMPORT_OUTCOME_UNKNOWN``.

CSV empty fields are imported as SQL ``NULL``. String values for string
columns are kept verbatim, so leading zeros survive. Type conversion is
strict: an integer column accepts integers and integer-looking strings, a
decimal column accepts ``Decimal``/``int``/JSON numbers/plain decimal strings
and is checked against the column's precision and scale without rounding, and
so on. Problem reasons never contain a cell value.
"""

from __future__ import annotations

import base64
import binascii
import csv
import datetime as dt
import decimal
import hashlib
import hmac
import io
import json
import math
import re
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any, ContextManager

import pyarrow as pa
import pyarrow.parquet as pq

from . import import_ledger
from .config import (
    ConfigError,
    DatabaseConfig,
    ImportLimits,
    ImportTable,
    Settings,
    table_key,
)
from .errors import (
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
from .institution import Institution

# ---------------------------------------------------------------------------
# Public data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ImportRequest:
    institution: Institution
    database: str
    table: str
    file_base64: str
    filename: str
    content_sha256: str
    format: str | None
    mode: str
    dry_run: bool


@dataclass(frozen=True)
class ImportProblem:
    row: int
    column: str | None
    reason: str


@dataclass(frozen=True)
class ImportResult:
    status: str
    database: str
    table: str
    row_count: int
    inserted: int | None
    updated: int | None
    batch_id: str | None
    columns: tuple[str, ...]
    problems: tuple[ImportProblem, ...]


# ---------------------------------------------------------------------------
# Constants and internal data model
# ---------------------------------------------------------------------------

_FORMATS = frozenset({"csv", "jsonl", "parquet"})
_MODES = frozenset({"insert", "upsert"})
_TABLE_RE = re.compile(r"^([A-Za-z0-9_$]{1,64})\.([A-Za-z0-9_$]{1,64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_INTEGER_RE = re.compile(r"^[+-]?\d+$")
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_YEAR_RE = re.compile(r"^\d{4}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$"
)
_TIME_RE = re.compile(r"^-?\d{1,3}:[0-5]\d:[0-5]\d(?:\.\d{1,6})?$")
_ENUM_TYPE_RE = re.compile(r"^(enum|set)\((.*)\)$", re.IGNORECASE | re.DOTALL)

_EXTENSIONS = (
    (".csv", "csv"),
    (".jsonl", "jsonl"),
    (".ndjson", "jsonl"),
    (".parquet", "parquet"),
)
_CHUNK_ROWS = 500
# MySQL caps a prepared statement at 65535 placeholders; stay comfortably
# below it even for a very wide whitelisted table while never exceeding 500
# rows per statement.
_MAX_PLACEHOLDERS = 60_000
_SAMPLE_LIMIT = 20
_ERROR_SAMPLE_LIMIT = 5
_BOM = b"\xef\xbb\xbf"
_PARQUET_MAGIC = b"PAR1"
_DUPLICATE_KEY_CODE = 1062
_UNSIGNED = "unsigned"

_TABLE_NOT_IMPORTABLE_MESSAGE = "table is not available for import"
_FORBIDDEN_INSTITUTION_MESSAGE = "institution is not permitted to import into this table"

_INTEGER_RANGES: dict[str, tuple[tuple[int, int], tuple[int, int]]] = {
    "tinyint": ((-128, 127), (0, 255)),
    "smallint": ((-32768, 32767), (0, 65535)),
    "mediumint": ((-8388608, 8388607), (0, 16777215)),
    "int": ((-2147483648, 2147483647), (0, 4294967295)),
    "integer": ((-2147483648, 2147483647), (0, 4294967295)),
    "bigint": (
        (-9223372036854775808, 9223372036854775807),
        (0, 18446744073709551615),
    ),
}

_STRING_TYPES = frozenset({"char", "varchar", "tinytext", "text", "mediumtext", "longtext"})
# String types whose key equality follows the column collation. Enum/set are
# compared by their stored string, so they are included here.
_COLLATION_TYPES = _STRING_TYPES | {"enum", "set"}
_DATETIME_TYPES = frozenset({"datetime", "timestamp"})
_UNSUPPORTED_TYPES = frozenset(
    {
        "binary",
        "varbinary",
        "tinyblob",
        "blob",
        "mediumblob",
        "longblob",
        "bit",
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "geomcollection",
    }
)

_NESTED_ARROW_CHECKS = tuple(
    getattr(pa.types, name)
    for name in (
        "is_list",
        "is_large_list",
        "is_fixed_size_list",
        "is_struct",
        "is_map",
        "is_union",
        "is_run_end_encoded",
    )
    if hasattr(pa.types, name)
)


@dataclass(frozen=True)
class ColumnMetadata:
    """One target column, trimmed to what import validation needs."""

    name: str
    data_type: str
    column_type: str
    nullable: bool
    has_default: bool
    auto_increment: bool
    generated: bool
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    collation: str | None = None


@dataclass(frozen=True)
class TableMetadata:
    columns: tuple[ColumnMetadata, ...]
    # (index name, columns in index order); an expression column is encoded
    # as "" so it can never satisfy the upsert safety rules.
    unique_indexes: tuple[tuple[str, tuple[str, ...]], ...]


@dataclass(frozen=True)
class _Authorization:
    database: DatabaseConfig
    spec: ImportTable
    bound_institution: str


@dataclass(frozen=True)
class _ParsedFile:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class _MappedColumns:
    bindings: tuple[tuple[int, ColumnMetadata], ...]
    written: tuple[tuple[int, ColumnMetadata], ...]
    institution_file_index: int | None
    present_targets: frozenset[str]


def _safe_text(value: str | None) -> str | None:
    """Return *value* if it is UTF-8 encodable, else a replacement form."""
    if value is None:
        return None
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return value.encode("utf-8", "replace").decode("utf-8")
    return value


class _ProblemSink:
    """Count every problem but keep at most :data:`_SAMPLE_LIMIT` samples."""

    def __init__(self) -> None:
        self.total = 0
        self.samples: list[ImportProblem] = []

    def add(self, row: int, column: str | None, reason: str) -> None:
        self.total += 1
        if len(self.samples) < _SAMPLE_LIMIT:
            self.samples.append(
                ImportProblem(row=row, column=_safe_text(column), reason=reason)
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_import(
    settings: Settings,
    request: ImportRequest,
    *,
    connect: Callable[[], ContextManager[Any]],
    deadline: float,
) -> ImportResult:
    """Validate and (unless *request.dry_run*) import one whitelisted file."""
    authorization = _authorize(settings, request)
    _ensure_deadline(deadline)

    decoded = _decode_payload(settings, request)
    file_format, text = _resolve_format(request.format, request.filename, decoded)
    _ensure_deadline(deadline)

    max_rows = _max_rows(settings, authorization.spec)
    sink = _ProblemSink()
    parsed = _parse_file(
        decoded,
        file_format,
        text,
        settings.limits.import_,
        max_rows,
        sink,
    )
    _ensure_deadline(deadline)

    canonical_table = f"{authorization.spec.schema}.{authorization.spec.table}"
    with connect() as conn:
        with conn.cursor() as cursor:
            metadata = load_table_metadata(
                cursor,
                schema=authorization.spec.schema,
                table=authorization.spec.table,
            )
            _assert_config_columns(metadata, authorization.spec)
            _ensure_deadline(deadline)

            mapped = _map_columns(authorization.spec, metadata, parsed.columns, sink)
            _check_institution_values(mapped, parsed, authorization.bound_institution)
            _add_missing_columns(
                mapped, authorization.spec, metadata, request.mode, sink
            )
            if request.mode == "upsert":
                _check_upsert_indexes(metadata, authorization.spec)

            rows = _validate_rows(
                parsed,
                mapped,
                authorization.spec,
                authorization.bound_institution,
                request.mode,
                sink,
            )

            written_columns = tuple(column.name for _, column in mapped.written)
            if sink.total:
                if request.dry_run:
                    return _dry_run_result(
                        authorization, canonical_table, len(rows), written_columns, sink
                    )
                raise BizDbError(
                    VALIDATION_FAILED, _validation_message(sink)
                )

            if request.dry_run:
                return _dry_run_result(
                    authorization, canonical_table, len(rows), written_columns, sink
                )

            return _execute_write(
                conn,
                cursor,
                authorization,
                request,
                canonical_table,
                written_columns,
                rows,
                deadline,
            )


# ---------------------------------------------------------------------------
# Step 1: authorization
# ---------------------------------------------------------------------------


def _authorize(settings: Settings, request: ImportRequest) -> _Authorization:
    """Resolve the whitelist before any file or database access.

    Every "this table is not importable" path shares one message so a caller
    cannot probe for tables, databases, or whitelist entries.
    """
    if not settings.imports.enabled:
        raise BizDbError(IMPORTS_DISABLED, "artifact import is disabled")

    database_id = request.database
    if not isinstance(database_id, str) or not database_id:
        raise _table_not_importable()
    try:
        database = settings.database(database_id)
    except ConfigError:
        raise _table_not_importable() from None
    if not database.allow_imports:
        raise _table_not_importable()

    table = request.table
    match = _TABLE_RE.fullmatch(table) if isinstance(table, str) else None
    if match is None:
        raise _table_not_importable()
    schema, table_name = match.groups()
    spec = settings.imports.tables.get(table_key(database.id, schema, table_name))
    if spec is None:
        raise _table_not_importable()

    scope = settings.institutions.get(request.institution.code)
    if scope is None or not scope.allow_imports:
        raise BizDbError(FORBIDDEN_INSTITUTION, _FORBIDDEN_INSTITUTION_MESSAGE)

    if (
        not isinstance(request.mode, str)
        or request.mode not in _MODES
        or request.mode not in spec.modes
    ):
        raise BizDbError(INVALID_ARGUMENT, "mode is not allowed for this table")

    bound = (
        request.institution.raw
        if spec.value_format == "raw"
        else request.institution.code
    )
    return _Authorization(database=database, spec=spec, bound_institution=bound)


def _table_not_importable() -> BizDbError:
    return BizDbError(TABLE_NOT_IMPORTABLE, _TABLE_NOT_IMPORTABLE_MESSAGE)


# ---------------------------------------------------------------------------
# Step 2: bounded bytes, digest, filename
# ---------------------------------------------------------------------------


def _decode_payload(settings: Settings, request: ImportRequest) -> bytes:
    """Decode canonical base64, bounded before and after decoding."""
    limit = settings.limits.import_.max_bytes
    encoded = request.file_base64
    if not isinstance(encoded, str):
        raise BizDbError(INVALID_ARGUMENT, "file_base64 must be a string")

    # 4 * ceil(limit / 3) is the longest canonical encoding of *limit* bytes.
    max_encoded_length = 4 * ((limit + 2) // 3)
    if len(encoded) > max_encoded_length:
        raise BizDbError(
            TOO_LARGE, f"file exceeds the {limit}-byte import limit"
        )
    if encoded[:5].lower() == "data:":
        raise BizDbError(
            INVALID_FILE, "file_base64 must not include a data URI prefix"
        )
    if any(character.isspace() for character in encoded):
        raise BizDbError(INVALID_FILE, "file_base64 must not contain whitespace")

    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BizDbError(INVALID_FILE, "file_base64 is not valid base64") from exc
    if base64.b64encode(decoded).decode("ascii") != encoded:
        raise BizDbError(INVALID_FILE, "file_base64 is not canonical base64")
    if len(decoded) > limit:
        raise BizDbError(
            TOO_LARGE, f"file exceeds the {limit}-byte import limit"
        )

    digest = request.content_sha256
    if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
        raise BizDbError(
            INVALID_ARGUMENT,
            "content_sha256 must be 64 lowercase hexadecimal characters",
        )
    if not hmac.compare_digest(hashlib.sha256(decoded).hexdigest(), digest):
        raise BizDbError(
            CONTENT_DIGEST_MISMATCH, "file content does not match content_sha256"
        )

    _check_filename(request.filename)
    return decoded


def _check_filename(filename: object) -> None:
    if not isinstance(filename, str) or not 1 <= len(filename) <= 255:
        raise BizDbError(INVALID_ARGUMENT, "filename must be 1 to 255 characters")
    if (
        filename in (".", "..")
        or "/" in filename
        or "\\" in filename
        or _CONTROL_RE.search(filename)
    ):
        raise BizDbError(
            INVALID_ARGUMENT,
            "filename must be a plain file name without path separators",
        )


# ---------------------------------------------------------------------------
# Step 3: format resolution and sniffing
# ---------------------------------------------------------------------------


def _resolve_format(
    explicit: str | None, filename: str, decoded: bytes
) -> tuple[str, str | None]:
    """Return ``(format, decoded_text_or_None)`` after content sniffing."""
    inferred = _infer_format(filename)
    if explicit is not None:
        if not isinstance(explicit, str) or explicit not in _FORMATS:
            raise BizDbError(
                INVALID_ARGUMENT, "format must be csv, jsonl, or parquet"
            )
        if inferred is not None and inferred != explicit:
            raise BizDbError(
                INVALID_ARGUMENT, "format does not match the file extension"
            )
        chosen = explicit
    elif inferred is not None:
        chosen = inferred
    else:
        raise BizDbError(
            INVALID_ARGUMENT,
            "format is required when the file extension is not csv, jsonl, or parquet",
        )

    if chosen == "parquet":
        if not decoded.startswith(_PARQUET_MAGIC) or not decoded.endswith(
            _PARQUET_MAGIC
        ):
            raise BizDbError(
                INVALID_FILE, "parquet file is missing its magic bytes"
            )
        return chosen, None

    payload = decoded
    if payload.startswith(_BOM):
        if chosen != "csv":
            raise BizDbError(
                INVALID_FILE, "a byte-order mark is only allowed for csv files"
            )
        payload = payload[len(_BOM) :]
    try:
        return chosen, payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BizDbError(INVALID_FILE, "file is not valid UTF-8") from exc


def _infer_format(filename: str) -> str | None:
    lowered = filename.lower()
    for suffix, file_format in _EXTENSIONS:
        if lowered.endswith(suffix):
            return file_format
    return None


# ---------------------------------------------------------------------------
# Step 4: bounded parsing
# ---------------------------------------------------------------------------


def _parse_file(
    decoded: bytes,
    file_format: str,
    text: str | None,
    limits: ImportLimits,
    max_rows: int,
    sink: _ProblemSink,
) -> _ParsedFile:
    if file_format == "parquet":
        return _parse_parquet(decoded, limits, max_rows, sink)
    assert text is not None
    if file_format == "csv":
        return _parse_csv(text, limits, max_rows, sink)
    return _parse_jsonl(text, limits, max_rows, sink)


def _parse_csv(
    text: str,
    limits: ImportLimits,
    max_rows: int,
    sink: _ProblemSink,
) -> _ParsedFile:
    # The file size already bounds a field; keep Python's csv parser from
    # rejecting a field before our own max_field_bytes problem can report it.
    if csv.field_size_limit() < limits.max_bytes:
        csv.field_size_limit(limits.max_bytes)

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = next(reader)
    except StopIteration:
        raise BizDbError(INVALID_FILE, "csv file has no header row") from None
    except csv.Error as exc:
        raise BizDbError(INVALID_FILE, "csv file is malformed") from exc
    columns = tuple(header)
    _check_columns(columns, limits)
    for name in columns:
        _record_field_size(name, limits.max_field_bytes, 0, None, sink)

    rows: list[tuple[Any, ...]] = []
    data_row = 0
    try:
        for fields in reader:
            if not fields:
                # A completely blank line is not a data row, so it is ignored
                # rather than reported as ragged.
                continue
            data_row += 1
            if len(fields) != len(columns):
                raise BizDbError(
                    INVALID_FILE,
                    f"csv row {data_row} has {len(fields)} fields but the header has "
                    f"{len(columns)}",
                )
            if len(rows) >= max_rows:
                raise _too_many_rows(max_rows)
            row = tuple(None if field == "" else field for field in fields)
            for index, value in enumerate(row):
                _record_field_size(
                    value, limits.max_field_bytes, data_row, columns[index], sink
                )
            rows.append(row)
    except BizDbError:
        raise
    except csv.Error as exc:
        raise BizDbError(INVALID_FILE, "csv file is malformed") from exc
    return _ParsedFile(columns=columns, rows=tuple(rows))


def _parse_jsonl(
    text: str,
    limits: ImportLimits,
    max_rows: int,
    sink: _ProblemSink,
) -> _ParsedFile:
    columns: tuple[str, ...] | None = None
    items: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line, parse_constant=_reject_json_constant)
        except (json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise BizDbError(
                INVALID_FILE, f"jsonl line {line_number} is not valid JSON"
            ) from exc
        if not isinstance(item, dict):
            raise BizDbError(
                INVALID_FILE, f"jsonl line {line_number} is not a JSON object"
            )
        if columns is None:
            columns = tuple(item.keys())
            _check_columns(columns, limits)
            for name in columns:
                _record_field_size(name, limits.max_field_bytes, 0, None, sink)
        elif set(item.keys()) != set(columns):
            raise BizDbError(
                INVALID_FILE,
                f"jsonl line {line_number} does not have the same fields as the first row",
            )
        for value in item.values():
            if isinstance(value, (dict, list)):
                raise BizDbError(
                    INVALID_FILE,
                    f"jsonl line {line_number} contains a nested value",
                )
            if isinstance(value, float) and not math.isfinite(value):
                raise BizDbError(
                    INVALID_FILE,
                    f"jsonl line {line_number} contains a non-finite number",
                )
        if len(items) >= max_rows:
            raise _too_many_rows(max_rows)
        items.append(item)

    if columns is None:
        raise BizDbError(INVALID_FILE, "jsonl file has no rows")

    rows: list[tuple[Any, ...]] = []
    for row_number, item in enumerate(items, start=1):
        row = tuple(item[name] for name in columns)
        for index, value in enumerate(row):
            _record_field_size(
                value, limits.max_field_bytes, row_number, columns[index], sink
            )
        rows.append(row)
    return _ParsedFile(columns=columns, rows=tuple(rows))


def _parse_parquet(
    decoded: bytes,
    limits: ImportLimits,
    max_rows: int,
    sink: _ProblemSink,
) -> _ParsedFile:
    try:
        parquet = pq.ParquetFile(io.BytesIO(decoded))
        metadata = parquet.metadata
        schema = parquet.schema_arrow
        if metadata is None:
            raise BizDbError(INVALID_FILE, "parquet file has no metadata")

        # Check the row count and the decompressed size from the footer before
        # reading any column data.
        if metadata.num_rows > max_rows:
            raise _too_many_rows(max_rows)
        decompressed = sum(
            metadata.row_group(index).total_byte_size
            for index in range(metadata.num_row_groups)
        )
        if decompressed > 8 * limits.max_bytes:
            raise BizDbError(
                TOO_LARGE, "parquet file expands beyond the import memory budget"
            )

        columns = tuple(schema.names)
        _check_columns(columns, limits)
        for name in columns:
            _record_field_size(name, limits.max_field_bytes, 0, None, sink)
        for field in schema:
            if not _supported_arrow_type(field.type):
                raise BizDbError(
                    INVALID_FILE, "parquet file contains a nested column type"
                )
    except BizDbError:
        raise
    except Exception as exc:  # noqa: BLE001 - redacted file failure
        raise BizDbError(INVALID_FILE, "parquet file could not be read") from exc

    rows: list[tuple[Any, ...]] = []
    try:
        for batch in parquet.iter_batches(batch_size=1000):
            for record in batch.to_pylist():
                if len(rows) >= max_rows:
                    raise _too_many_rows(max_rows)
                row = tuple(record.get(name) for name in columns)
                for index, value in enumerate(row):
                    _record_field_size(
                        value,
                        limits.max_field_bytes,
                        len(rows) + 1,
                        columns[index],
                        sink,
                    )
                rows.append(row)
    except BizDbError:
        raise
    except Exception as exc:  # noqa: BLE001 - redacted file failure
        raise BizDbError(INVALID_FILE, "parquet file could not be read") from exc

    if len(rows) != metadata.num_rows:
        raise BizDbError(INVALID_FILE, "parquet row count did not match its metadata")
    return _ParsedFile(columns=columns, rows=tuple(rows))


def _supported_arrow_type(data_type: Any) -> bool:
    if pa.types.is_dictionary(data_type):
        return _supported_arrow_type(data_type.value_type)
    return not any(check(data_type) for check in _NESTED_ARROW_CHECKS)


def _check_columns(columns: tuple[str, ...], limits: ImportLimits) -> None:
    if not columns:
        raise BizDbError(INVALID_FILE, "file has no columns")
    if len(columns) > limits.max_columns:
        raise BizDbError(
            TOO_LARGE, f"file exceeds the {limits.max_columns}-column import limit"
        )
    lowered = [column.lower() for column in columns]
    if len(set(lowered)) != len(lowered):
        raise BizDbError(INVALID_FILE, "file has duplicate column names")


def _record_field_size(
    value: Any,
    limit: int,
    row: int,
    column: str | None,
    sink: _ProblemSink,
) -> None:
    if isinstance(value, str):
        try:
            size = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            # e.g. a JSONL \uD800 escape: valid JSON syntax, but not a
            # Unicode scalar value MySQL utf8mb4 can store.
            sink.add(row, column, "field is not valid UTF-8 text")
            return
    elif isinstance(value, (bytes, bytearray)):
        size = len(value)
    else:
        return
    if size > limit:
        sink.add(row, column, "field exceeds the maximum field size")


def _max_rows(settings: Settings, spec: ImportTable) -> int:
    cap = settings.limits.import_.max_rows
    if spec.max_rows is not None:
        cap = min(cap, spec.max_rows)
    return cap


def _too_many_rows(max_rows: int) -> BizDbError:
    return BizDbError(TOO_LARGE, f"file exceeds the {max_rows}-row import limit")


def _reject_json_constant(value: str) -> Any:
    # ``json.loads`` calls this for NaN, Infinity, and -Infinity, which are not
    # valid JSON numbers and would not be valid MySQL values either.
    raise ValueError(f"non-standard JSON constant {value}")


# ---------------------------------------------------------------------------
# Step 5: target metadata (faked by tests through a small typed result)
# ---------------------------------------------------------------------------


def load_table_metadata(
    cursor: Any, *, schema: str, table: str
) -> TableMetadata:
    """Load base-table/InnoDB metadata, unique indexes, and trigger presence."""
    try:
        return _load_table_metadata(cursor, schema=schema, table=table)
    except BizDbError:
        raise
    except Exception as exc:  # noqa: BLE001 - redacted database failure
        raise BizDbError(
            DATABASE_ERROR, "could not read target table metadata"
        ) from exc


def _load_table_metadata(cursor: Any, *, schema: str, table: str) -> TableMetadata:
    cursor.execute(
        "SELECT TABLE_TYPE, ENGINE FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table),
    )
    table_row = cursor.fetchone()
    if table_row is None:
        raise _table_not_importable()
    table_type = _field(table_row, "TABLE_TYPE")
    if table_type is None or str(table_type).upper() != "BASE TABLE":
        raise _table_not_importable()
    if str(_field(table_row, "ENGINE") or "").upper() != "INNODB":
        raise _table_not_importable()

    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, "
        "COLUMN_DEFAULT, EXTRA, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
        "NUMERIC_SCALE, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    column_rows = cursor.fetchall() or ()
    columns = tuple(_column_metadata(row) for row in column_rows)
    if not columns:
        raise _table_not_importable()

    cursor.execute(
        "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE "
        "FROM INFORMATION_SCHEMA.STATISTICS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND NON_UNIQUE = 0 "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        (schema, table),
    )
    unique_indexes = _group_unique_indexes(cursor.fetchall() or ())

    cursor.execute(
        "SELECT TRIGGER_NAME FROM INFORMATION_SCHEMA.TRIGGERS "
        "WHERE EVENT_OBJECT_SCHEMA = %s AND EVENT_OBJECT_TABLE = %s LIMIT 1",
        (schema, table),
    )
    if cursor.fetchone():
        # Triggers can write other tables with side effects we did not review.
        raise _table_not_importable()

    return TableMetadata(columns=columns, unique_indexes=unique_indexes)


def _column_metadata(row: Any) -> ColumnMetadata:
    extra = str(_field(row, "EXTRA") or "")
    lowered_extra = extra.lower()
    column_default = _field(row, "COLUMN_DEFAULT")
    # ``DEFAULT_GENERATED`` describes an expression default; it is not a
    # generated (computed) column and must not be mistaken for one.
    generated = (
        "generated" in lowered_extra
        and "default_generated" not in lowered_extra
    )
    return ColumnMetadata(
        name=str(_field(row, "COLUMN_NAME") or ""),
        data_type=str(_field(row, "DATA_TYPE") or "").lower(),
        column_type=str(_field(row, "COLUMN_TYPE") or ""),
        nullable=str(_field(row, "IS_NULLABLE") or "").upper() == "YES",
        has_default=column_default is not None or "default_generated" in lowered_extra,
        auto_increment="auto_increment" in lowered_extra,
        generated=generated,
        character_maximum_length=_as_int(_field(row, "CHARACTER_MAXIMUM_LENGTH")),
        numeric_precision=_as_int(_field(row, "NUMERIC_PRECISION")),
        numeric_scale=_as_int(_field(row, "NUMERIC_SCALE")),
        collation=_as_text(_field(row, "COLLATION_NAME")),
    )


def _group_unique_indexes(
    rows: Sequence[Any],
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        if _as_int(_field(row, "NON_UNIQUE")) not in (0, None):
            continue
        name = str(_field(row, "INDEX_NAME") or "__unknown_index")
        column = _field(row, "COLUMN_NAME")
        sequence = _as_int(_field(row, "SEQ_IN_INDEX"))
        if sequence is None:
            sequence = len(grouped.get(name, ())) + 1
        # A functional/expression index cannot be reviewed column by column;
        # encode it as "" so the upsert rules refuse the table.
        grouped.setdefault(name, []).append(
            (sequence, "" if column is None else str(column))
        )
    return tuple(
        (name, tuple(column for _, column in sorted(parts)))
        for name, parts in grouped.items()
    )


def _assert_config_columns(metadata: TableMetadata, spec: ImportTable) -> None:
    available = {column.name.lower() for column in metadata.columns}
    for name in (*spec.columns, spec.institution_column):
        if name.lower() not in available:
            raise _table_not_importable()
    institution = next(
        column
        for column in metadata.columns
        if column.name.lower() == spec.institution_column.lower()
    )
    if (
        institution.generated
        or institution.auto_increment
        or institution.data_type in _UNSUPPORTED_TYPES
    ):
        raise _table_not_importable()


# ---------------------------------------------------------------------------
# Step 6: column rules and institution binding
# ---------------------------------------------------------------------------


def _map_columns(
    spec: ImportTable,
    metadata: TableMetadata,
    file_columns: tuple[str, ...],
    sink: _ProblemSink,
) -> _MappedColumns:
    by_lower = {column.name.lower(): column for column in metadata.columns}
    allowed = {column.lower() for column in spec.columns}
    institution_lower = spec.institution_column.lower()

    bindings: list[tuple[int, ColumnMetadata]] = []
    for index, name in enumerate(file_columns):
        lowered = name.lower()
        target = by_lower.get(lowered)
        if target is None or (lowered not in allowed and lowered != institution_lower):
            sink.add(0, name, "column is not whitelisted for import")
            continue
        if target.generated:
            sink.add(0, name, "generated columns cannot be imported")
            continue
        bindings.append((index, target))

    present = {column.name.lower() for _, column in bindings}
    by_target = {column.name.lower(): (index, column) for index, column in bindings}
    written: list[tuple[int, ColumnMetadata]] = []
    written_targets: set[str] = set()
    for name in spec.columns:
        binding = by_target.get(name.lower())
        if binding is None or binding[1].name.lower() == institution_lower:
            continue
        target = binding[1].name.lower()
        if target in written_targets:
            # Defensive: the config parser rejects duplicate whitelist
            # columns, but a hand-built Settings must not duplicate SQL columns.
            continue
        written_targets.add(target)
        written.append(binding)
    institution_column = by_lower[institution_lower]
    # The institution value is always the server-assigned one, even when the
    # file happened to carry a matching column.
    written.append((-1, institution_column))

    institution_binding = by_target.get(institution_lower)
    return _MappedColumns(
        bindings=tuple(bindings),
        written=tuple(written),
        institution_file_index=(
            institution_binding[0] if institution_binding is not None else None
        ),
        present_targets=frozenset(present),
    )


def _check_institution_values(
    mapped: _MappedColumns, parsed: _ParsedFile, bound_institution: str
) -> None:
    if mapped.institution_file_index is None:
        return
    for row in parsed.rows:
        value = row[mapped.institution_file_index]
        if not isinstance(value, str) or value != bound_institution:
            # One foreign row fails the whole batch, including dry runs.
            raise BizDbError(FORBIDDEN_INSTITUTION, _FORBIDDEN_INSTITUTION_MESSAGE)


def _add_missing_columns(
    mapped: _MappedColumns,
    spec: ImportTable,
    metadata: TableMetadata,
    mode: str,
    sink: _ProblemSink,
) -> None:
    institution_lower = spec.institution_column.lower()
    for column in metadata.columns:
        if column.name.lower() == institution_lower:
            continue
        if (
            column.generated
            or column.auto_increment
            or column.nullable
            or column.has_default
        ):
            continue
        if column.name.lower() not in mapped.present_targets:
            sink.add(0, column.name, "required column is missing from the file")

    if mode == "upsert":
        for key in spec.key_columns:
            if key.lower() not in mapped.present_targets:
                sink.add(0, key, "key column is missing from the file")


# ---------------------------------------------------------------------------
# Step 7: upsert unique-index safety
# ---------------------------------------------------------------------------


def _check_upsert_indexes(metadata: TableMetadata, spec: ImportTable) -> None:
    """Refuse upsert when a unique key could update another institution.

    Every UNIQUE/PRIMARY index must include the institution column, and the
    configured business key plus the institution column must exactly match one
    of those indexes. A global unique key would otherwise let ON DUPLICATE KEY
    UPDATE touch a row owned by a different institution.
    """
    institution = spec.institution_column.lower()
    if not spec.key_columns:
        raise _table_not_importable()
    if any(key.lower() == institution for key in spec.key_columns):
        # The business key must never be the server-assigned institution
        # column; otherwise a file value could steer the row identity.
        raise _table_not_importable()
    wanted = {key.lower() for key in spec.key_columns} | {institution}
    matched = False
    for _name, columns in metadata.unique_indexes:
        lowered = {column.lower() for column in columns}
        if institution not in lowered:
            raise _table_not_importable()
        if lowered == wanted:
            matched = True
    if not matched:
        raise _table_not_importable()


# ---------------------------------------------------------------------------
# Step 8: strict per-cell validation
# ---------------------------------------------------------------------------


def _validate_rows(
    parsed: _ParsedFile,
    mapped: _MappedColumns,
    spec: ImportTable,
    bound_institution: str,
    mode: str,
    sink: _ProblemSink,
) -> list[list[Any]]:
    file_index_by_target = {
        column.name.lower(): index for index, column in mapped.bindings
    }
    column_by_file_index = {index: column for index, column in mapped.bindings}
    key_indexes: list[int] = []
    keys_complete = True
    for key in spec.key_columns:
        index = file_index_by_target.get(key.lower())
        if index is None:
            keys_complete = False
            break
        key_indexes.append(index)

    rows: list[list[Any]] = []
    seen_keys: set[tuple[Any, ...]] = set()
    for row_number, source in enumerate(parsed.rows, start=1):
        values = list(source)
        for file_index, column in mapped.bindings:
            value, reason = _validate_cell(values[file_index], column)
            values[file_index] = value
            if reason is not None:
                sink.add(row_number, column.name, reason)

        if mode == "upsert" and keys_complete:
            # MySQL compares string key columns through the column collation,
            # so the in-file dedupe must be at least as coarse as that equality;
            # otherwise a later row would silently update the earlier row and
            # the inserted/updated counts would be wrong.
            key = tuple(
                _comparison_key(values[index], column_by_file_index[index])
                for index in key_indexes
            )
            if key in seen_keys:
                sink.add(
                    row_number,
                    ",".join(spec.key_columns),
                    "duplicate key tuple in the file (by the column collation)",
                )
            else:
                seen_keys.add(key)

        output: list[Any] = []
        for file_index, _column in mapped.written:
            output.append(
                bound_institution if file_index < 0 else values[file_index]
            )
        rows.append(output)
    return rows


def _comparison_key(value: Any, column: ColumnMetadata) -> Any:
    """Return a dedupe key at least as coarse as MySQL collation equality.

    MySQL compares string key columns with the column collation, which is
    usually case-insensitive and often accent- and trailing-space-insensitive.
    The key only needs to be coarser than MySQL, never finer: a false duplicate
    is a validation problem, while a missed duplicate would silently overwrite
    an earlier row and make the inserted/updated counts wrong.
    """
    collation = column.collation
    if (
        not isinstance(value, str)
        or column.data_type not in _COLLATION_TYPES
        or collation is None
    ):
        return value
    name = collation.lower()
    if name == "binary" or name.endswith("_bin"):
        return value
    # PAD SPACE: trailing spaces never distinguish key values. Applying this to
    # NO PAD collations too only makes the key coarser, which is acceptable.
    text = value.rstrip(" ")
    if "_ci" in name:
        text = text.casefold()
    if "_ai" in name or ("_as" not in name and "_cs" not in name):
        # Legacy collations (utf8mb4_general_ci, utf8mb4_unicode_ci, ...) have
        # no explicit accent marker but are accent-insensitive in practice.
        text = "".join(
            character
            for character in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(character)
        )
    return unicodedata.normalize("NFKC", text).rstrip(" ")


def _validate_cell(value: Any, column: ColumnMetadata) -> tuple[Any, str | None]:
    """Return ``(value_to_bind, problem_reason_or_None)`` for one cell."""
    # Binary/geometry/bit columns cannot be reviewed cell by cell; refuse the
    # column even when this particular value happens to be NULL.
    if column.data_type in _UNSUPPORTED_TYPES:
        return value, "unsupported column type"
    if value is None:
        if column.auto_increment or column.nullable or column.has_default:
            return None, None
        return value, "NULL is not allowed"

    data_type = column.data_type
    if data_type in _INTEGER_RANGES:
        return _validate_integer(value, column, _INTEGER_RANGES[data_type])
    if data_type in ("decimal", "numeric"):
        return _validate_decimal(value, column)
    if data_type in ("float", "double", "real"):
        return _validate_float(value)
    if data_type in _STRING_TYPES:
        return _validate_text(value, column)
    if data_type == "date":
        return _validate_date(value)
    if data_type in _DATETIME_TYPES:
        return _validate_datetime(value)
    if data_type == "time":
        return _validate_time(value)
    if data_type == "year":
        return _validate_year(value)
    if data_type in ("enum", "set"):
        return _validate_enum_or_set(value, column)
    if data_type == "json":
        return _validate_json(value)
    return value, "unsupported column type"


def _validate_integer(
    value: Any,
    column: ColumnMetadata,
    ranges: tuple[tuple[int, int], tuple[int, int]],
) -> tuple[Any, str | None]:
    tinyint_one = (
        column.data_type == "tinyint"
        and column.column_type.lower().startswith("tinyint(1)")
    )
    if isinstance(value, bool):
        if not tinyint_one:
            return value, "booleans are only accepted for tinyint(1) columns"
        return (1 if value else 0), None
    if isinstance(value, int):
        number = value
    elif isinstance(value, str):
        if tinyint_one and value.lower() in ("true", "false"):
            return (1 if value.lower() == "true" else 0), None
        if not _INTEGER_RE.fullmatch(value):
            return value, "value is not a valid integer"
        try:
            number = int(value)
        except ValueError:
            return value, "integer value is out of range"
    else:
        return value, "value is not a valid integer"

    unsigned = _UNSIGNED in column.column_type.lower()
    low, high = ranges[1] if unsigned else ranges[0]
    if not low <= number <= high:
        return value, "integer value is out of range"
    return number, None


def _validate_decimal(
    value: Any, column: ColumnMetadata
) -> tuple[Any, str | None]:
    if isinstance(value, bool):
        return value, "value is not a valid decimal"
    if isinstance(value, decimal.Decimal):
        number = value
    elif isinstance(value, int):
        number = decimal.Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            return value, "decimal value must be finite"
        # Use the shortest decimal representation of the float; never round
        # here, the column's scale check below rejects excess digits.
        number = decimal.Decimal(str(value))
    elif isinstance(value, str):
        if not _DECIMAL_RE.fullmatch(value):
            return value, "value is not a valid decimal"
        try:
            number = decimal.Decimal(value)
        except (decimal.InvalidOperation, ValueError):
            return value, "value is not a valid decimal"
    else:
        return value, "value is not a valid decimal"

    if not number.is_finite():
        return value, "decimal value must be finite"
    precision = column.numeric_precision
    scale = column.numeric_scale if column.numeric_scale is not None else 0
    if precision is not None:
        integer_digits = max(number.adjusted() + 1, 0) if number != 0 else 0
        if integer_digits > precision - scale:
            return value, "decimal value overflows the column precision"
    fractional_digits = max(-number.as_tuple().exponent, 0)
    if fractional_digits > scale:
        return value, "decimal value has too many decimal places"
    return value, None


def _validate_float(value: Any) -> tuple[Any, str | None]:
    if isinstance(value, bool):
        return value, "value is not a finite number"
    if isinstance(value, str):
        if not _DECIMAL_RE.fullmatch(value):
            return value, "value is not a finite number"
        try:
            number = float(value)
        except (ValueError, OverflowError):
            return value, "value is not a finite number"
    elif isinstance(value, (int, float, decimal.Decimal)):
        try:
            number = float(value)
        except (ValueError, OverflowError):
            return value, "value is not a finite number"
    else:
        return value, "value is not a finite number"
    if not math.isfinite(number):
        return value, "value is not a finite number"
    return value, None


def _validate_text(value: Any, column: ColumnMetadata) -> tuple[Any, str | None]:
    if not isinstance(value, str):
        return value, "value must be text"
    limit = column.character_maximum_length
    if limit is not None and len(value) > limit:
        return value, "value is longer than the column allows"
    return value, None


def _validate_date(value: Any) -> tuple[Any, str | None]:
    if isinstance(value, dt.datetime):
        return value, "value is not a valid date (YYYY-MM-DD)"
    if isinstance(value, dt.date):
        return value, None
    if isinstance(value, str) and _DATE_RE.fullmatch(value):
        try:
            dt.date.fromisoformat(value)
        except ValueError:
            return value, "value is not a valid date (YYYY-MM-DD)"
        return value, None
    return value, "value is not a valid date (YYYY-MM-DD)"


def _validate_datetime(value: Any) -> tuple[Any, str | None]:
    if isinstance(value, dt.datetime):
        return value, None
    if isinstance(value, str) and _DATETIME_RE.fullmatch(value):
        try:
            dt.datetime.fromisoformat(value)
        except ValueError:
            return value, "value is not a valid datetime"
        return value, None
    return value, "value is not a valid datetime"


def _validate_time(value: Any) -> tuple[Any, str | None]:
    if isinstance(value, dt.time):
        return value, None
    if isinstance(value, str) and _TIME_RE.fullmatch(value):
        hours = int(value.lstrip("-").split(":", 1)[0])
        if hours <= 838:
            return value, None
    return value, "value is not a valid time"


def _validate_year(value: Any) -> tuple[Any, str | None]:
    if isinstance(value, bool):
        return value, "value is not a valid year"
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and _YEAR_RE.fullmatch(value):
        number = int(value)
    else:
        return value, "value is not a valid year"
    if number == 0 or 1901 <= number <= 2155:
        return value, None
    return value, "year value is out of range"


def _validate_enum_or_set(
    value: Any, column: ColumnMetadata
) -> tuple[Any, str | None]:
    parsed = _enum_members(column.column_type)
    if parsed is None:
        return value, "unsupported column type"
    kind, members = parsed
    if not isinstance(value, str):
        return value, "value is not text"
    if kind == "enum":
        if value in members:
            return value, None
        return value, "value is not a member of the enum"
    if value == "":
        return value, None
    parts = value.split(",")
    if any(part == "" or part not in members for part in parts):
        return value, "set value contains an unknown member"
    if len(set(parts)) != len(parts):
        return value, "set value contains a duplicate member"
    return value, None


def _validate_json(value: Any) -> tuple[Any, str | None]:
    if not isinstance(value, str):
        return value, "value must be JSON text"
    try:
        json.loads(value, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, ValueError, RecursionError):
        return value, "value is not valid JSON"
    return value, None


def _enum_members(column_type: str) -> tuple[str, tuple[str, ...]] | None:
    match = _ENUM_TYPE_RE.fullmatch(column_type.strip())
    if match is None:
        return None
    kind = match.group(1).lower()
    body = match.group(2)
    values: list[str] = []
    index = 0
    while index < len(body):
        if body[index] != "'":
            index += 1
            continue
        index += 1
        current: list[str] = []
        while index < len(body):
            character = body[index]
            if character == "\\" and index + 1 < len(body):
                current.append(body[index + 1])
                index += 2
            elif character == "'":
                if index + 1 < len(body) and body[index + 1] == "'":
                    current.append("'")
                    index += 2
                else:
                    index += 1
                    break
            else:
                current.append(character)
                index += 1
        values.append("".join(current))
    return kind, tuple(values)


# ---------------------------------------------------------------------------
# Steps 9-11: dry run, transaction, idempotent duplicate
# ---------------------------------------------------------------------------


def _dry_run_result(
    authorization: _Authorization,
    canonical_table: str,
    row_count: int,
    columns: tuple[str, ...],
    sink: _ProblemSink,
) -> ImportResult:
    return ImportResult(
        status="dry_run",
        database=authorization.database.id,
        table=canonical_table,
        row_count=row_count,
        inserted=None,
        updated=None,
        batch_id=None,
        columns=columns,
        problems=tuple(sink.samples),
    )


def _execute_write(
    conn: Any,
    cursor: Any,
    authorization: _Authorization,
    request: ImportRequest,
    canonical_table: str,
    written_columns: tuple[str, ...],
    rows: list[list[Any]],
    deadline: float,
) -> ImportResult:
    spec = authorization.spec
    ledger_table = authorization.database.ledger_table
    institution_code = request.institution.code
    key = import_ledger.idempotency_key(
        institution_code,
        authorization.database.id,
        canonical_table,
        request.content_sha256,
        request.mode,
    )
    batch_id = uuid.uuid4().hex
    row_count = len(rows)

    try:
        conn.begin()
        _ensure_deadline(deadline)

        inserted_ledger_row = import_ledger.insert_entry(
            cursor,
            ledger_table=ledger_table,
            key=key,
            batch_id=batch_id,
            institution_code=institution_code,
            database_id=authorization.database.id,
            table=canonical_table,
            content_sha256=request.content_sha256,
            mode=request.mode,
            row_count=row_count,
        )
        if not inserted_ledger_row:
            # Another transaction committed the same import. Roll back our
            # empty transaction and return its committed counts.
            conn.rollback()
            existing = import_ledger.find_entry(
                cursor,
                ledger_table=ledger_table,
                key=key,
                institution_code=institution_code,
            )
            if existing is None:
                raise BizDbError(
                    DATABASE_ERROR,
                    "the duplicate import is not visible in the import ledger",
                )
            return _duplicate_result(
                authorization,
                canonical_table,
                written_columns,
                existing,
                row_count,
            )

        if request.mode == "insert":
            inserted = _insert_rows(
                cursor,
                schema=spec.schema,
                table=spec.table,
                columns=written_columns,
                rows=rows,
                deadline=deadline,
            )
            updated = 0
        else:
            existing_count = _count_existing_keys(
                cursor,
                schema=spec.schema,
                table=spec.table,
                columns=written_columns,
                key_columns=spec.key_columns,
                institution_column=spec.institution_column,
                bound_institution=authorization.bound_institution,
                rows=rows,
                deadline=deadline,
            )
            if existing_count > row_count:
                raise BizDbError(
                    DATABASE_ERROR,
                    "the existing row count exceeded the file row count",
                )
            inserted = row_count - existing_count
            updated = existing_count
            _upsert_rows(
                cursor,
                schema=spec.schema,
                table=spec.table,
                columns=written_columns,
                key_columns=spec.key_columns,
                institution_column=spec.institution_column,
                rows=rows,
                deadline=deadline,
            )

        import_ledger.finalize_entry(
            cursor,
            ledger_table=ledger_table,
            batch_id=batch_id,
            inserted=inserted,
            updated=updated,
        )
    except BizDbError:
        _safe_rollback(conn)
        raise
    except Exception as exc:  # noqa: BLE001 - classified by MySQL error code
        _safe_rollback(conn)
        if _mysql_error_code(exc) == _DUPLICATE_KEY_CODE:
            raise BizDbError(
                VALIDATION_FAILED, "imported data conflicts with existing data"
            ) from exc
        raise BizDbError(
            DATABASE_ERROR, "import transaction failed and was rolled back"
        ) from exc

    # A failed COMMIT is ambiguous: the server may have committed just before
    # the connection dropped. Never report a rollback we cannot prove.
    try:
        conn.commit()
    except Exception as exc:  # noqa: BLE001 - outcome is explicitly unknown
        raise BizDbError(
            IMPORT_OUTCOME_UNKNOWN,
            f"commit outcome is unknown for batch {batch_id}; retrying the same "
            "file is safe because the import ledger prevents a second write",
        ) from exc

    return ImportResult(
        status="imported",
        database=authorization.database.id,
        table=canonical_table,
        row_count=row_count,
        inserted=inserted,
        updated=updated,
        batch_id=batch_id,
        columns=written_columns,
        problems=(),
    )


def _duplicate_result(
    authorization: _Authorization,
    canonical_table: str,
    columns: tuple[str, ...],
    existing: dict[str, Any],
    fallback_row_count: int,
) -> ImportResult:
    stored_row_count = _as_int(existing.get("row_count"))
    return ImportResult(
        status="duplicate",
        database=authorization.database.id,
        table=canonical_table,
        row_count=(
            fallback_row_count if stored_row_count is None else stored_row_count
        ),
        inserted=_as_int(existing.get("inserted_rows")),
        updated=_as_int(existing.get("updated_rows")),
        batch_id=_as_text(existing.get("batch_id")),
        columns=columns,
        problems=(),
    )


def _insert_rows(
    cursor: Any,
    *,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    rows: Sequence[Sequence[Any]],
    deadline: float,
) -> int:
    inserted = 0
    for chunk in _chunks(rows, _statement_rows(len(columns))):
        _ensure_deadline(deadline)
        sql = _insert_sql(schema, table, columns, len(chunk))
        cursor.execute(sql, _flat_params(chunk))
        affected = _as_int(cursor.rowcount)
        if affected is None or affected < 0:
            raise BizDbError(
                DATABASE_ERROR,
                "the database did not report the inserted row count",
            )
        inserted += affected
    if inserted != len(rows):
        raise BizDbError(
            DATABASE_ERROR,
            "inserted row count did not match the file; the transaction was rolled back",
        )
    return inserted


def _count_existing_keys(
    cursor: Any,
    *,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    institution_column: str,
    bound_institution: str,
    rows: Sequence[Sequence[Any]],
    deadline: float,
) -> int:
    positions = {column.lower(): index for index, column in enumerate(columns)}
    key_positions = [positions[key.lower()] for key in key_columns]
    existing = 0
    for chunk in _chunks(rows, _statement_rows(len(key_columns))):
        _ensure_deadline(deadline)
        key_tuples = [tuple(row[position] for position in key_positions) for row in chunk]
        sql, params = _select_keys_sql(
            schema,
            table,
            key_columns,
            institution_column,
            bound_institution,
            key_tuples,
        )
        cursor.execute(sql, params)
        existing += len(cursor.fetchall() or ())
    return existing


def _upsert_rows(
    cursor: Any,
    *,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    institution_column: str,
    rows: Sequence[Sequence[Any]],
    deadline: float,
) -> None:
    excluded = {column.lower() for column in key_columns}
    excluded.add(institution_column.lower())
    update_columns = [
        column for column in columns if column.lower() not in excluded
    ]
    if not update_columns:
        # Keep the statement valid when the file only carries key columns.
        update_columns = [institution_column]
    updates = ", ".join(
        f"{_quote_identifier(column)} = VALUES({_quote_identifier(column)})"
        for column in update_columns
    )
    for chunk in _chunks(rows, _statement_rows(len(columns))):
        _ensure_deadline(deadline)
        sql = (
            _insert_sql(schema, table, columns, len(chunk))
            + " ON DUPLICATE KEY UPDATE "
            + updates
        )
        cursor.execute(sql, _flat_params(chunk))


def _statement_rows(column_count: int) -> int:
    """Rows per statement: at most 500 and under the placeholder cap."""
    return max(1, min(_CHUNK_ROWS, _MAX_PLACEHOLDERS // max(1, column_count)))


def _insert_sql(
    schema: str, table: str, columns: Sequence[str], row_count: int
) -> str:
    column_list = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values = ", ".join([placeholders] * row_count)
    return (
        f"INSERT INTO {_qualified_table(schema, table)} ({column_list}) "
        f"VALUES {values}"
    )


def _select_keys_sql(
    schema: str,
    table: str,
    key_columns: Sequence[str],
    institution_column: str,
    bound_institution: str,
    key_tuples: Sequence[tuple[Any, ...]],
) -> tuple[str, list[Any]]:
    column_list = ", ".join(_quote_identifier(column) for column in key_columns)
    if len(key_columns) == 1:
        placeholders = ", ".join(["%s"] * len(key_tuples))
        predicate = (
            f"{_quote_identifier(institution_column)} = %s "
            f"AND {_quote_identifier(key_columns[0])} IN ({placeholders})"
        )
    else:
        row_placeholder = "(" + ", ".join(["%s"] * len(key_columns)) + ")"
        values = ", ".join([row_placeholder] * len(key_tuples))
        predicate = (
            f"{_quote_identifier(institution_column)} = %s "
            f"AND ({column_list}) IN ({values})"
        )
    sql = (
        f"SELECT {column_list} FROM {_qualified_table(schema, table)} "
        f"WHERE {predicate} FOR UPDATE"
    )
    params: list[Any] = [bound_institution]
    for key_tuple in key_tuples:
        params.extend(key_tuple)
    return sql, params


def _flat_params(rows: Sequence[Sequence[Any]]) -> list[Any]:
    return [value for row in rows for value in row]


def _chunks(
    rows: Sequence[Sequence[Any]], size: int
) -> Iterator[Sequence[Sequence[Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _qualified_table(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _validation_message(sink: _ProblemSink) -> str:
    details = []
    for problem in sink.samples[:_ERROR_SAMPLE_LIMIT]:
        if problem.column:
            details.append(
                f"row {problem.row} column {problem.column}: {problem.reason}"
            )
        else:
            details.append(f"row {problem.row}: {problem.reason}")
    return (
        f"file has {sink.total} validation problem(s): " + "; ".join(details)
    )


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _ensure_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise BizDbError(DEADLINE_EXCEEDED, "the import deadline was exceeded")


def _safe_rollback(conn: Any) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001 - the original failure is what matters
        pass


def _mysql_error_code(exc: BaseException) -> int | None:
    args = getattr(exc, "args", ())
    if args and isinstance(args[0], int):
        return args[0]
    return None


def _field(row: Any, name: str) -> Any:
    if not isinstance(row, dict):
        return None
    for key, value in row.items():
        if str(key).lower() == name.lower():
            return value
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
