import base64
import hashlib
import re
from urllib.parse import quote, unquote, urlsplit

import mcp.types
import pytest

from biz_db_mcp import mcp_files
from biz_db_mcp.export import MIME_TYPES, ExportFile

EXPORT_ID = "0123456789abcdef0123456789abcdef"
EXPORT_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _export_file(
    tmp_path,
    *,
    data: bytes = b"a,b\r\n1,2\r\n",
    filename: str = "订单_2024.csv",
    fmt: str = "csv",
    row_count: int = 1,
) -> ExportFile:
    path = tmp_path / filename
    path.write_bytes(data)
    return ExportFile(
        path=path,
        filename=filename,
        format=fmt,
        mime_type=MIME_TYPES[fmt],
        row_count=row_count,
        size=len(data),
        sha256=hashlib.sha256(data).hexdigest(),
    )


def _path_parts(uri: str) -> list[str]:
    return urlsplit(uri).path.strip("/").split("/")


def test_embedded_resource_blob_is_the_exact_file_bytes(tmp_path):
    data = b"\x00\x01binary\xff\r\n"
    export = _export_file(tmp_path, data=data)

    resource = mcp_files.embedded_resource(export, export_id=EXPORT_ID)

    assert isinstance(resource, mcp.types.EmbeddedResource)
    assert resource.type == "resource"
    assert resource.resource.mimeType == "text/csv"
    assert base64.b64decode(resource.resource.blob, validate=True) == data
    assert not resource.resource.blob.startswith("data:")


def test_embedded_resource_uri_last_segment_is_the_quoted_filename(tmp_path):
    filename = "订单 2024.csv"
    export = _export_file(tmp_path, filename=filename)

    resource = mcp_files.embedded_resource(export, export_id=EXPORT_ID)
    uri = str(resource.resource.uri)
    parts = _path_parts(uri)

    assert uri.startswith(f"bizdb://export/{EXPORT_ID}/")
    assert len(parts) == 2
    assert parts[0] == EXPORT_ID
    assert parts[1] == quote(filename, safe="")
    assert unquote(parts[1]) == filename


@pytest.mark.parametrize("fmt", ["parquet", "csv", "jsonl"])
def test_embedded_resource_mime_type_follows_the_format(tmp_path, fmt):
    export = _export_file(tmp_path, filename=f"out.{fmt}", fmt=fmt)

    resource = mcp_files.embedded_resource(export, export_id=EXPORT_ID)

    assert resource.resource.mimeType == MIME_TYPES[fmt]


@pytest.mark.parametrize(
    "export_id",
    [
        "",
        "abc",
        "0123456789abcdef0123456789abcde",
        "0123456789abcdef0123456789abcdef0",
        "0123456789ABCDEF0123456789ABCDEF",
        "0123456789abcdef0123456789abcdeg",
    ],
)
def test_embedded_resource_rejects_invalid_export_ids(tmp_path, export_id):
    export = _export_file(tmp_path)

    with pytest.raises(ValueError):
        mcp_files.embedded_resource(export, export_id=export_id)


def test_export_summary_reports_metadata_without_sql(tmp_path):
    export = _export_file(tmp_path, row_count=3)

    summary = mcp_files.export_summary(export, database="primary")

    assert export.filename in summary
    assert "primary" in summary
    assert "csv" in summary
    assert f"rows={export.row_count}" in summary
    assert f"size={export.size} bytes" in summary
    assert export.sha256 in summary
    assert "truncated=false" in summary
    assert "\n" not in summary
    assert "SELECT" not in summary.upper()


def test_export_content_returns_text_then_resource_and_deletes_the_file(tmp_path):
    export = _export_file(tmp_path)
    path = export.path

    content = mcp_files.export_content(export, database="primary")

    assert len(content) == 2
    assert isinstance(content[0], mcp.types.TextContent)
    assert isinstance(content[1], mcp.types.EmbeddedResource)
    assert content[0].text == mcp_files.export_summary(export, database="primary")
    parts = _path_parts(str(content[1].resource.uri))
    assert EXPORT_ID_RE.fullmatch(parts[0])
    assert unquote(parts[1]) == export.filename
    assert not path.exists()


def test_export_content_uses_a_supplied_export_id(tmp_path):
    export = _export_file(tmp_path)

    content = mcp_files.export_content(export, database="primary", export_id=EXPORT_ID)

    assert _path_parts(str(content[1].resource.uri))[0] == EXPORT_ID
    assert not export.path.exists()


def test_export_content_deletes_the_file_when_encoding_fails(tmp_path, monkeypatch):
    export = _export_file(tmp_path)
    path = export.path

    def boom(data):
        raise RuntimeError("encoding failed")

    monkeypatch.setattr(mcp_files.base64, "b64encode", boom)

    with pytest.raises(RuntimeError, match="encoding failed"):
        mcp_files.export_content(export, database="primary")

    assert not path.exists()


def test_export_content_deletes_the_file_for_an_invalid_export_id(tmp_path):
    export = _export_file(tmp_path)
    path = export.path

    with pytest.raises(ValueError):
        mcp_files.export_content(export, database="primary", export_id="not-an-id")

    assert not path.exists()
