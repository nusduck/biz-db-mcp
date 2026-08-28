"""Push an export file into Sandbox as a Dataset."""

from __future__ import annotations

import uuid
from pathlib import Path
from urllib.parse import quote

import httpx

from .config import ExportIdentity, Settings


class SandboxPushError(RuntimeError):
    """Raised when pushing an exported file into Sandbox fails."""


def _dataset_filename(dataset_name: str, format: str = "parquet") -> str:
    name = dataset_name.strip()
    # strip any known extension so callers can pass "foo.parquet" or "foo.csv"
    for ext in (".parquet", ".csv", ".jsonl"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break
    if (
        not name
        or name in {".", ".."}
        or len(name) > 160
        or "/" in name
        or "\\" in name
        or any(ord(char) < 32 for char in name)
    ):
        raise SandboxPushError("dataset_name must be a safe file name")
    ext_map = {"parquet": ".parquet", "csv": ".csv", "jsonl": ".jsonl"}
    suffix = ext_map.get(format, ".parquet")
    return f"{name}{suffix}"


def _mime_for_format(format: str) -> str:
    return {
        "parquet": "application/vnd.apache.parquet",
        "csv": "text/csv",
        "jsonl": "application/x-ndjson",
    }.get(format, "application/octet-stream")


def push_dataset(
    settings: Settings,
    *,
    identity: ExportIdentity,
    dataset_name: str,
    file_path: Path,
    format: str = "parquet",
) -> dict:
    url = (
        f"{settings.sandbox_base_url}/sessions/"
        f"{quote(identity.session_id, safe='')}/datasets"
    )
    headers = {
        settings.sandbox_api_token_header: settings.sandbox_api_token,
        "X-Acting-Organization-Id": identity.org_id,
        "X-Acting-User-Id": identity.user_id,
        "Idempotency-Key": uuid.uuid4().hex,
    }
    if identity.conversation_id:
        headers["X-Conversation-Id"] = identity.conversation_id

    try:
        with file_path.open("rb") as fh:
            files = {
                "file": (
                    _dataset_filename(dataset_name, format),
                    fh,
                    _mime_for_format(format),
                )
            }
            resp = httpx.post(
                url,
                headers=headers,
                files=files,
                timeout=settings.sandbox_upload_timeout_seconds,
            )
    finally:
        file_path.unlink(missing_ok=True)

    if resp.status_code >= 400:
        raise SandboxPushError(
            f"Sandbox dataset upload failed ({resp.status_code}): {resp.text[:500]}"
        )
    try:
        result = resp.json()
    except ValueError as exc:
        raise SandboxPushError("Sandbox dataset upload returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise SandboxPushError("Sandbox dataset upload returned a non-object response")
    return result
