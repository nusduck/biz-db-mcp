"""Push an export file into Sandbox as a Dataset."""

from __future__ import annotations

import uuid
from pathlib import Path
from urllib.parse import quote

import httpx

from .config import ExportIdentity, Settings


class SandboxPushError(RuntimeError):
    """Raised when pushing an exported file into Sandbox fails."""


def _dataset_filename(dataset_name: str) -> str:
    name = dataset_name.strip()
    if name.lower().endswith(".parquet"):
        name = name[:-8]
    if (
        not name
        or name in {".", ".."}
        or len(name) > 160
        or "/" in name
        or "\\" in name
        or any(ord(char) < 32 for char in name)
    ):
        raise SandboxPushError("dataset_name must be a safe file name")
    return f"{name}.parquet"


def push_dataset(
    settings: Settings,
    *,
    identity: ExportIdentity,
    dataset_name: str,
    file_path: Path,
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
                    _dataset_filename(dataset_name),
                    fh,
                    "application/vnd.apache.parquet",
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
