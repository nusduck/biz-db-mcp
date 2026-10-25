"""Native TCP client for the private DBPM password service.

The service speaks a tiny line protocol: send
``b"\\x0e\\x02 {database} {user}\\n"`` and read one line back. A successful
response is ``OK: <password>\\n``; anything else is a failure. This module
implements the client directly so no bridge executable or vendored SDK is
needed.

Failures are reported with stable codes only. Messages and attempt records
never contain the password, the server response, or the database/user names,
so they are safe to surface or log as a category. This module itself performs
no logging.
"""

from __future__ import annotations

import socket
import time
import unicodedata
from typing import Final, NoReturn

from .config import DbpmEndpoint, DbpmSettings

MAX_NAME_BYTES: Final = 256
MIN_NAME_BYTES: Final = 1
# A whole response frame, including the terminating newline.
MAX_FRAME_BYTES: Final = 4096
_RECV_CHUNK_BYTES: Final = 4096
_OK_PREFIX: Final = "OK: "
_REQUEST_MAGIC: Final = b"\x0e\x02"

# Top-level error codes.
INVALID_ENTRY: Final = "INVALID_ENTRY"
ALL_ENDPOINTS_FAILED: Final = "ALL_ENDPOINTS_FAILED"
BUDGET_EXHAUSTED: Final = "BUDGET_EXHAUSTED"

# Per-endpoint failure categories recorded in ``DbpmError.attempts``.
CONNECT_FAILED: Final = "CONNECT_FAILED"
CONNECTION_ERROR: Final = "CONNECTION_ERROR"
TIMEOUT: Final = "TIMEOUT"
EOF: Final = "EOF"
FRAME_TOO_LARGE: Final = "FRAME_TOO_LARGE"
BAD_RESPONSE: Final = "BAD_RESPONSE"
ERROR_RESPONSE: Final = "ERROR_RESPONSE"


class DbpmError(RuntimeError):
    """A redacted DBPM password lookup failure."""

    def __init__(
        self,
        code: str,
        attempts: tuple[tuple[int, str], ...] = (),
    ) -> None:
        self.code = code
        self.attempts: tuple[tuple[int, str], ...] = tuple(attempts)
        super().__init__(_describe(code, self.attempts))


def fetch_password(settings: DbpmSettings, db_name: str, user_name: str) -> str:
    """Resolve one password from the configured DBPM endpoints.

    Endpoints are tried in order; any per-endpoint failure (connect error,
    timeout, malformed frame) fails over to the next one. The whole call is
    bounded by ``settings.budget_ms``.
    """
    _validate_name(db_name)
    _validate_name(user_name)
    request = _request_bytes(db_name, user_name)
    budget_deadline = time.monotonic() + settings.budget_ms / 1000
    attempts: list[tuple[int, str]] = []
    for index, endpoint in enumerate(settings.servers):
        try:
            return _attempt(endpoint, request, settings, budget_deadline)
        except _AttemptFailure as failure:
            attempts.append((index, failure.code))
        except _BudgetExhausted:
            attempts.append((index, TIMEOUT))
            raise DbpmError(BUDGET_EXHAUSTED, tuple(attempts)) from None
    raise DbpmError(ALL_ENDPOINTS_FAILED, tuple(attempts)) from None


def _request_bytes(db_name: str, user_name: str) -> bytes:
    return _REQUEST_MAGIC + f" {db_name} {user_name}\n".encode("utf-8")


def _validate_name(value: str) -> None:
    """Reject names that could inject a second request line."""
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise DbpmError(INVALID_ENTRY) from None
    if not MIN_NAME_BYTES <= len(encoded) <= MAX_NAME_BYTES:
        raise DbpmError(INVALID_ENTRY)
    if any(_is_whitespace(ch) or _is_control(ch) for ch in value):
        raise DbpmError(INVALID_ENTRY)


def _is_whitespace(character: str) -> bool:
    return character.isspace()


def _is_control(character: str) -> bool:
    return unicodedata.category(character) == "Cc"


# ---------------------------------------------------------------------------
# Per-attempt transport
# ---------------------------------------------------------------------------


class _AttemptFailure(Exception):
    """One endpoint failed; ``code`` is a per-endpoint failure category."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _BudgetExhausted(Exception):
    """The whole-call budget ran out before this attempt completed."""


def _remaining(deadline: float, budget_deadline: float) -> float:
    """Seconds left in the attempt, or raise the matching deadline error."""
    now = time.monotonic()
    if now >= budget_deadline:
        raise _BudgetExhausted
    remaining = deadline - now
    if remaining <= 0:
        raise _AttemptFailure(TIMEOUT)
    return remaining


def _raise_timeout(budget_deadline: float) -> NoReturn:
    """A socket timed out: attribute it to the budget when that ran out too."""
    if time.monotonic() >= budget_deadline:
        raise _BudgetExhausted
    raise _AttemptFailure(TIMEOUT)


def _attempt(
    endpoint: DbpmEndpoint,
    request: bytes,
    settings: DbpmSettings,
    budget_deadline: float,
) -> str:
    started = time.monotonic()
    if started >= budget_deadline:
        raise _BudgetExhausted
    attempt_deadline = min(
        started + settings.endpoint_timeout_ms / 1000,
        budget_deadline,
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _connect(sock, endpoint, settings, attempt_deadline, budget_deadline)
        _send(sock, request, attempt_deadline, budget_deadline)
        return _read_frame(sock, attempt_deadline, budget_deadline)
    finally:
        sock.close()


def _connect(
    sock: socket.socket,
    endpoint: DbpmEndpoint,
    settings: DbpmSettings,
    attempt_deadline: float,
    budget_deadline: float,
) -> None:
    timeout = min(
        settings.connect_timeout_ms / 1000,
        _remaining(attempt_deadline, budget_deadline),
    )
    sock.settimeout(timeout)
    try:
        sock.connect((endpoint.host, endpoint.port))
    except TimeoutError:
        _raise_timeout(budget_deadline)
    except OSError:
        raise _AttemptFailure(CONNECT_FAILED) from None


def _send(
    sock: socket.socket,
    request: bytes,
    attempt_deadline: float,
    budget_deadline: float,
) -> None:
    sock.settimeout(_remaining(attempt_deadline, budget_deadline))
    try:
        sock.sendall(request)
    except TimeoutError:
        _raise_timeout(budget_deadline)
    except OSError:
        raise _AttemptFailure(CONNECTION_ERROR) from None


def _read_frame(
    sock: socket.socket,
    attempt_deadline: float,
    budget_deadline: float,
) -> str:
    buffer = bytearray()
    while True:
        newline = buffer.find(b"\n")
        if newline >= 0:
            if newline + 1 > MAX_FRAME_BYTES:
                raise _AttemptFailure(FRAME_TOO_LARGE)
            if buffer[newline + 1 :]:
                raise _AttemptFailure(BAD_RESPONSE)
            return _parse_frame(bytes(buffer[:newline]))
        if len(buffer) >= MAX_FRAME_BYTES:
            raise _AttemptFailure(FRAME_TOO_LARGE)
        sock.settimeout(_remaining(attempt_deadline, budget_deadline))
        try:
            chunk = sock.recv(_RECV_CHUNK_BYTES)
        except TimeoutError:
            _raise_timeout(budget_deadline)
        except OSError:
            # A peer that closes without reading the request can surface as a
            # reset instead of a clean EOF. With no frame bytes yet, both mean
            # the same thing to the caller.
            raise _AttemptFailure(EOF if not buffer else CONNECTION_ERROR) from None
        if not chunk:
            raise _AttemptFailure(EOF)
        buffer.extend(chunk)


def _parse_frame(frame: bytes) -> str:
    try:
        text = frame.decode("utf-8")
    except UnicodeDecodeError:
        raise _AttemptFailure(BAD_RESPONSE) from None
    if not text.startswith(_OK_PREFIX):
        raise _AttemptFailure(ERROR_RESPONSE)
    password = text[len(_OK_PREFIX) :]
    if not password or any(_is_control(ch) for ch in password):
        raise _AttemptFailure(BAD_RESPONSE)
    return password


# ---------------------------------------------------------------------------
# Redacted error text
# ---------------------------------------------------------------------------


def _describe(code: str, attempts: tuple[tuple[int, str], ...]) -> str:
    if code == INVALID_ENTRY:
        return (
            f"{code}: database and user names must be 1-{MAX_NAME_BYTES} bytes "
            "without whitespace or control characters"
        )
    if code == BUDGET_EXHAUSTED:
        summary = _attempt_summary(attempts)
        return f"{code}: DBPM password lookup exceeded its time budget ({summary})"
    summary = _attempt_summary(attempts)
    return f"{code}: all DBPM endpoints failed ({summary})"


def _attempt_summary(attempts: tuple[tuple[int, str], ...]) -> str:
    if not attempts:
        return "no attempts"
    return ", ".join(f"{index}={failure}" for index, failure in attempts)
