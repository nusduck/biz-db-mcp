"""Bulk export: stream one database SELECT to Parquet/CSV/JSONL in bounded batches."""

from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Settings
from .db import stream_query

ExportFormat = Literal["parquet", "csv", "jsonl"]


class ExportError(RuntimeError):
    """Raised when an export fails or exceeds its row guardrail."""


def export_to_parquet(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    *,
    database: str | None = None,
    batch_size: int | None = None,
    format: ExportFormat = "parquet",
) -> tuple[Path, int]:
    """Stream *sql* to a temporary file and return its row count.

    format controls the output: parquet (default), csv, or jsonl.  The suffix
    and writer are chosen accordingly; parquet keeps the existing schema-
    resolute batching, csv/jsonl stream row-by-row.
    """
    if format not in ("parquet", "csv", "jsonl"):
        raise ExportError(f"unsupported format {format!r}; use parquet, csv, or jsonl")
    # allow per-database export_max_rows / timeout via Settings helpers
    # (stream_query already uses effective timeout)
    batch_size = settings.export_row_batch_size if batch_size is None else batch_size
    if isinstance(batch_size, bool) or batch_size < 1:
        raise ExportError("batch_size must be a positive integer")

    suffix = {"parquet": ".parquet", "csv": ".csv", "jsonl": ".jsonl"}[format]
    fd, raw_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    tmp_path = Path(raw_path)
    if format in ("csv", "jsonl"):
        return _export_delimited(settings, sql, params, tmp_path, format, database=database)

    row_count = 0
    max_rows = settings.effective_export_max_rows(database)
    writer: pq.ParquetWriter | None = None
    batch_rows: list[dict[str, Any]] = []
    # Rows held back before the first write. A batch inferred in isolation
    # can type a column that's all-NULL so far as `null`, which a later,
    # populated batch can't be cast to once the Parquet schema is fixed.
    # Buffering until every column's type is resolved (or this cap is hit)
    # keeps that from locking in a wrong schema for the whole file.
    pending_rows: list[dict[str, Any]] = []
    schema_lookahead_cap = max(batch_size * 8, 50_000)

    try:
        for row in stream_query(settings, sql, params, database=database):
            row_count += 1

            if row_count > max_rows:
                raise ExportError(
                    f"export exceeds BIZDB_EXPORT_MAX_ROWS={max_rows} (effective); "
                    "narrow the query or raise the limit deliberately"
                )

            if writer is None:
                pending_rows.append(row)
                ready = len(pending_rows) >= schema_lookahead_cap
                if not ready and len(pending_rows) % batch_size == 0:
                    ready = not _has_unresolved_columns(pending_rows)
                if ready:
                    writer = _write_batch(tmp_path, None, pending_rows)
                    pending_rows = []
                continue

            batch_rows.append(row)
            if len(batch_rows) >= batch_size:
                writer = _write_batch(tmp_path, writer, batch_rows)
                batch_rows = []

        if pending_rows:
            writer = _write_batch(tmp_path, writer, pending_rows)
        if batch_rows:
            writer = _write_batch(tmp_path, writer, batch_rows)

        if writer is None:
            # Produce a valid, readable file for a zero-row result.
            pq.write_table(pa.Table.from_pylist([]), tmp_path)
        else:
            writer.close()

        return tmp_path, row_count
    except Exception:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        tmp_path.unlink(missing_ok=True)
        raise


def _export_delimited(
    settings: Settings,
    sql: str,
    params: list[Any] | None,
    tmp_path: Path,
    fmt: ExportFormat,
    *,
    database: str | None = None,
) -> tuple[Path, int]:
    max_rows = settings.effective_export_max_rows(database)
    row_count = 0
    try:
        if fmt == "csv":
            with tmp_path.open("w", newline="", encoding="utf-8") as fh:
                writer = None
                for row in stream_query(settings, sql, params, database=database):
                    row_count += 1
                    if row_count > max_rows:
                        raise ExportError(f"export exceeds BIZDB_EXPORT_MAX_ROWS={max_rows} (effective)")
                    if writer is None:
                        writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
                        writer.writeheader()
                    writer.writerow(row)
                if row_count == 0:
                    # still produce a file so upload succeeds
                    pass
        else:  # jsonl
            with tmp_path.open("w", encoding="utf-8") as fh:
                for row in stream_query(settings, sql, params, database=database):
                    row_count += 1
                    if row_count > max_rows:
                        raise ExportError(f"export exceeds BIZDB_EXPORT_MAX_ROWS={max_rows} (effective)")
                    fh.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        return tmp_path, row_count
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _has_unresolved_columns(rows: list[dict[str, Any]]) -> bool:
    """True if any column across *rows* is still typed `null` (all-NULL so far)."""
    return any(field.type == pa.null() for field in pa.Table.from_pylist(rows).schema)


def _write_batch(
    path: Path,
    writer: pq.ParquetWriter | None,
    rows: list[dict[str, Any]],
) -> pq.ParquetWriter:
    if writer is None:
        table = pa.Table.from_pylist(rows)
        writer = pq.ParquetWriter(path, table.schema)
        writer.write_table(table)
        return writer

    try:
        table = pa.Table.from_pylist(rows, schema=writer.schema)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        raise ExportError(
            f"export batch does not match the established column schema: {exc}"
        ) from exc
    writer.write_table(table)
    return writer
