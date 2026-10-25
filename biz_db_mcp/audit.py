"""Structured per-call audit logging without SQL, parameters, credentials, or data.

Records go to stderr as one JSON object per line; stdout is reserved for the
stdio MCP protocol. SQL is reduced to its SHA-256 and length; callers must not
pass parameters, file contents, or result rows as fields.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sys
import time
import uuid
from collections.abc import Iterator

from .errors import BizDbError


@contextlib.contextmanager
def log_call(tool: str, **fields: object) -> Iterator[dict[str, object]]:
    """Audit one tool call; the yielded record may be enriched (e.g. institution)."""
    record: dict[str, object] = {
        "call_id": uuid.uuid4().hex,
        "tool": tool,
        **_redact(fields),
    }
    started = time.monotonic()
    try:
        yield record
        record["status"] = "ok"
    except BizDbError as exc:
        record["status"] = "error"
        record["error_code"] = exc.code
        raise
    except Exception as exc:
        record["status"] = "error"
        record["error_type"] = type(exc).__name__
        raise
    finally:
        record["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        print(json.dumps(record, default=str), file=sys.stderr, flush=True)


def _redact(fields: dict[str, object]) -> dict[str, object]:
    out = {key: value for key, value in fields.items() if value is not None}
    sql = out.pop("sql", None)
    if isinstance(sql, str):
        out["sql_sha256"] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        out["sql_length"] = len(sql)
    return out
