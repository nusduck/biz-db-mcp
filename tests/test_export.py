import pyarrow.parquet as pq
import pytest

from biz_db_mcp import export as export_module
from biz_db_mcp.config import load_settings
from biz_db_mcp.export import ExportError, export_to_parquet


def _settings(monkeypatch, max_rows="10"):
    monkeypatch.setenv("BIZDB_MYSQL_DSN", "mysql://u:p@localhost:3306/app")
    monkeypatch.setenv("BIZDB_SANDBOX_BASE_URL", "http://sandbox:8000")
    monkeypatch.setenv("BIZDB_SANDBOX_API_TOKEN", "token")
    monkeypatch.setenv("BIZDB_EXPORT_MAX_ROWS", max_rows)
    return load_settings()


def test_export_writes_streamed_rows_in_batches(monkeypatch):
    settings = _settings(monkeypatch)

    def fake_stream_query(settings, sql, params, *, database=None):
        yield {"id": 1, "name": "a"}
        yield {"id": 2, "name": "b"}
        yield {"id": 3, "name": "c"}

    monkeypatch.setattr(export_module, "stream_query", fake_stream_query)

    path, row_count = export_to_parquet(settings, "SELECT id, name FROM users", [], batch_size=2)
    try:
        table = pq.read_table(path)
        assert row_count == 3
        assert table.num_rows == 3
        assert table.column_names == ["id", "name"]
    finally:
        path.unlink(missing_ok=True)


def test_export_handles_column_that_is_null_in_first_batch_only(monkeypatch):
    settings = _settings(monkeypatch, max_rows="100000")

    def fake_stream_query(settings, sql, params, *, database=None):
        for i in range(20000):
            yield {"id": i, "note": None}
        for i in range(20000, 20005):
            yield {"id": i, "note": "hello"}

    monkeypatch.setattr(export_module, "stream_query", fake_stream_query)

    path, row_count = export_to_parquet(
        settings, "SELECT id, note FROM t", [], batch_size=20000
    )
    try:
        table = pq.read_table(path)
        assert row_count == 20005
        assert table.num_rows == 20005
    finally:
        path.unlink(missing_ok=True)


def test_export_removes_partial_file_when_row_limit_is_exceeded(monkeypatch):
    settings = _settings(monkeypatch, max_rows="2")

    def fake_stream_query(settings, sql, params, *, database=None):
        yield {"id": 1}
        yield {"id": 2}
        yield {"id": 3}

    monkeypatch.setattr(export_module, "stream_query", fake_stream_query)

    with pytest.raises(ExportError, match="BIZDB_EXPORT_MAX_ROWS"):
        export_to_parquet(settings, "SELECT id FROM users", [])
