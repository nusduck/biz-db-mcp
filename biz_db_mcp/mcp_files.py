"""Turn a finished export file into MCP embedded-resource content.

Exports are never uploaded: the platform saves the base64 blob from the tool
result into the caller's workspace.  Two platform rules drive this module:

* the blob is standard base64 (no ``data:`` prefix);
* the filename is the last path segment of the resource URI, URL-decoded.
"""

from __future__ import annotations

import base64
import re
import urllib.parse
import uuid

import mcp.types

from .export import ExportFile

_EXPORT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_URI_PREFIX = "bizdb://export"


def embedded_resource(export: ExportFile, *, export_id: str) -> mcp.types.EmbeddedResource:
    """Wrap *export* as a base64 ``EmbeddedResource`` blob.

    *export_id* is a 32-character lowercase hex identifier; it keeps URIs
    unique without leaking filenames or business data.  The filename is
    percent-encoded as a single path segment so the platform can recover it
    with ``unquote`` even when it contains CJK characters or spaces.
    """
    if not _EXPORT_ID_RE.fullmatch(export_id):
        raise ValueError("export_id must be 32 lowercase hexadecimal characters")
    blob = base64.b64encode(export.path.read_bytes()).decode("ascii")
    uri = f"{_URI_PREFIX}/{export_id}/{urllib.parse.quote(export.filename, safe='')}"
    return mcp.types.EmbeddedResource(
        type="resource",
        resource=mcp.types.BlobResourceContents(
            uri=uri,
            mimeType=export.mime_type,
            blob=blob,
        ),
    )


def export_summary(export: ExportFile, *, database: str) -> str:
    """One human-readable line describing a generated export (never SQL)."""
    return (
        f"file {export.filename}; database {database}; format {export.format}; "
        f"rows={export.row_count}; size={export.size} bytes; sha256={export.sha256}; "
        "truncated=false; generated; the platform saves it into the workspace."
    )


def export_content(
    export: ExportFile,
    *,
    database: str,
    export_id: str | None = None,
) -> list[mcp.types.TextContent | mcp.types.EmbeddedResource]:
    """Build ``[text summary, embedded resource]`` and delete the local file.

    *export_id* defaults to a fresh ``uuid4().hex``.  The temporary file is
    removed in a ``finally`` block, so it does not survive a failed base64
    encoding, an invalid *export_id*, or any other error.
    """
    resolved_id = export_id if export_id is not None else uuid.uuid4().hex
    try:
        summary = export_summary(export, database=database)
        resource = embedded_resource(export, export_id=resolved_id)
    finally:
        export.path.unlink(missing_ok=True)
    return [
        mcp.types.TextContent(type="text", text=summary),
        resource,
    ]
