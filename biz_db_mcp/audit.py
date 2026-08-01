"""Structured per-call audit logging without emitting SQL or credentials."""

from __future__ import annotations

import contextlib
import hashlib
import json
import sys
import time
import uuid
from collections.abc import Iterator


@contextlib.contextmanager
def log_call(tool: str, **fields: object) -> Iterator[None]:
    record: dict[str, object] = {
        "call_id": uuid.uuid4().hex,
        "tool": tool,
        **_redact(fields),
    }
    started = time.monotonic()
    try:
        yield
        record["status"] = "ok"
    except Exception as exc:
        record["status"] = "error"
        record["error_type"] = type(exc).__name__
        raise
    finally:
        record["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        print(json.dumps(record, default=str), file=sys.stderr, flush=True)


def _redact(fields: dict[str, object]) -> dict[str, object]:
    out = dict(fields)
    sql = out.pop("sql", None)
    if isinstance(sql, str):
        out["sql_sha256"] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        out["sql_length"] = len(sql)
    return out
