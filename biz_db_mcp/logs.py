"""Log destinations: size-rotated files and/or stderr, configured from config.yaml.

Two streams are kept apart:

* ``biz_db_mcp.audit`` — one JSON object per tool call (see ``audit.py``),
  always recorded at INFO regardless of ``logging.level``;
* everything else (this service, the MCP SDK, uvicorn) — filtered by
  ``logging.level``.

Each file is rotated by size, so disk use per file is bounded by
``(backup_count + 1) * max_bytes``. stdout is never used: it carries the
stdio MCP protocol.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from pathlib import Path

from .config import LogFileSettings, LoggingSettings

AUDIT_LOGGER = "biz_db_mcp.audit"
_APP_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


class _StderrHandler(logging.StreamHandler):
    """Writes to the current ``sys.stderr`` (tests and runtimes may swap it)."""

    def __init__(self) -> None:
        super().__init__(sys.stderr)

    @property  # type: ignore[override]
    def stream(self):  # noqa: D401 - logging.StreamHandler attribute
        return sys.stderr

    @stream.setter
    def stream(self, _value) -> None:
        pass


class _RotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Opens every file it creates (including after rotation) as mode 0640."""

    def _open(self):  # noqa: D401 - logging.FileHandler hook
        fd = os.open(self.baseFilename, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        os.fchmod(fd, 0o640)
        return os.fdopen(fd, "a", encoding=self.encoding, errors=self.errors)


def _file_handler(settings: LogFileSettings) -> logging.Handler:
    assert settings.path is not None
    path = Path(settings.path)
    path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    return _RotatingFileHandler(
        path,
        maxBytes=settings.max_bytes,
        backupCount=settings.backup_count,
        encoding="utf-8",
    )


def audit_logger() -> logging.Logger:
    """The audit logger; falls back to stderr until ``configure_logging`` runs."""
    logger = logging.getLogger(AUDIT_LOGGER)
    if not logger.handlers:
        logger.addHandler(_StderrHandler())
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def configure_logging(settings: LoggingSettings) -> None:
    """Install handlers for the audit and service logs; raises OSError if unwritable."""
    audit = logging.getLogger(AUDIT_LOGGER)
    for handler in list(audit.handlers):
        audit.removeHandler(handler)
        handler.close()
    audit.setLevel(logging.INFO)
    audit.propagate = False
    if settings.audit.path:
        audit.addHandler(_file_handler(settings.audit))
    if settings.stderr:
        audit.addHandler(_StderrHandler())
    for handler in audit.handlers:
        handler.setFormatter(logging.Formatter("%(message)s"))

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(settings.level)
    app_handlers: list[logging.Handler] = []
    if settings.app.path:
        app_handlers.append(_file_handler(settings.app))
    if settings.stderr:
        app_handlers.append(_StderrHandler())
    if not app_handlers:
        app_handlers.append(logging.NullHandler())
    for handler in app_handlers:
        handler.setFormatter(logging.Formatter(_APP_FORMAT))
        root.addHandler(handler)
    # Library loggers that install their own handlers must defer to ours.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "mcp"):
        library = logging.getLogger(name)
        library.handlers.clear()
        library.propagate = True
