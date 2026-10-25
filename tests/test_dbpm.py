"""Tests for the native TCP DBPM password client.

The fake server binds 127.0.0.1:0 in a background thread; no real network is
used. Timeouts are kept small (200-500 ms) so the suite stays fast.
"""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable

import pytest

from biz_db_mcp.config import DbpmEndpoint, DbpmSettings
from biz_db_mcp.dbpm import (
    ALL_ENDPOINTS_FAILED,
    BAD_RESPONSE,
    BUDGET_EXHAUSTED,
    EOF,
    ERROR_RESPONSE,
    FRAME_TOO_LARGE,
    INVALID_ENTRY,
    TIMEOUT,
    DbpmError,
    fetch_password,
)

Handler = Callable[[socket.socket, "FakeServer"], None]


class FakeServer:
    """A one-thread TCP server that runs *handler* per accepted connection."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self._listener.settimeout(0.05)
        self.host, self.port = self._listener.getsockname()
        self.stopped = threading.Event()
        self.requests: list[bytes] = []
        self.connections = 0
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    @property
    def endpoint(self) -> DbpmEndpoint:
        return DbpmEndpoint(host=self.host, port=self.port)

    def _serve(self) -> None:
        while not self.stopped.is_set():
            try:
                conn, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self.connections += 1
            try:
                self._handler(conn, self)
            except OSError:
                pass
            finally:
                conn.close()

    def close(self) -> None:
        self.stopped.set()
        try:
            self._listener.close()
        except OSError:
            pass
        self._thread.join(timeout=2)

    def __enter__(self) -> "FakeServer":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.close()
        return False


def _read_request(conn: socket.socket) -> bytes:
    received = b""
    while b"\n" not in received:
        chunk = conn.recv(4096)
        if not chunk:
            break
        received += chunk
    return received


def _respond(
    payload: bytes,
    *,
    chunk_size: int | None = None,
    delay: float = 0.0,
) -> Handler:
    """Send *payload* (optionally fragmented) after recording the request."""

    def handler(conn: socket.socket, server: FakeServer) -> None:
        server.requests.append(_read_request(conn))
        if chunk_size is None:
            conn.sendall(payload)
            return
        for start in range(0, len(payload), chunk_size):
            conn.sendall(payload[start : start + chunk_size])
            if delay:
                time.sleep(delay)

    return handler


def _drip(payload: bytes, delay: float) -> Handler:
    """Send one byte at a time, slowly enough to trip a deadline."""

    def handler(conn: socket.socket, server: FakeServer) -> None:
        server.requests.append(_read_request(conn))
        give_up = time.monotonic() + 3.0
        for byte in payload:
            if server.stopped.is_set() or time.monotonic() > give_up:
                return
            try:
                conn.sendall(bytes([byte]))
            except OSError:
                return
            time.sleep(delay)

    return handler


def _hold_open(conn: socket.socket, server: FakeServer) -> None:
    """Read the request, then stay silent until the client gives up."""
    server.requests.append(_read_request(conn))
    server.stopped.wait(2.0)


def _closed_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _settings(
    *endpoints: DbpmEndpoint,
    connect_timeout_ms: int = 300,
    endpoint_timeout_ms: int = 500,
    budget_ms: int = 3_000,
) -> DbpmSettings:
    return DbpmSettings(
        enabled=True,
        servers=tuple(endpoints),
        connect_timeout_ms=connect_timeout_ms,
        endpoint_timeout_ms=endpoint_timeout_ms,
        budget_ms=budget_ms,
    )


# ---------------------------------------------------------------------------
# Success paths
# ---------------------------------------------------------------------------


def test_fetches_password_and_sends_exact_request_bytes() -> None:
    with FakeServer(_respond(b"OK: s3cret pass\n")) as server:
        payload = fetch_password(_settings(server.endpoint), "app", "reader")

    assert payload == "s3cret pass"
    assert server.requests == [b"\x0e\x02 app reader\n"]


def test_password_keeps_surrounding_spaces() -> None:
    with FakeServer(_respond(b"OK:  padded \n")) as server:
        assert fetch_password(_settings(server.endpoint), "app", "user") == " padded "


def test_python_unicode_password_is_returned() -> None:
    password = "pässwörd-✓"
    with FakeServer(_respond(f"OK: {password}\n".encode("utf-8"))) as server:
        assert fetch_password(_settings(server.endpoint), "app", "user") == password


def test_fragmented_response_is_reassembled() -> None:
    with FakeServer(
        _respond(b"OK: fragmented-password\n", chunk_size=3, delay=0.005)
    ) as server:
        assert fetch_password(_settings(server.endpoint), "app", "user") == "fragmented-password"


def test_frame_at_the_size_limit_is_accepted() -> None:
    password = "a" * 4091  # 4-byte prefix + 4091 + newline == 4096
    with FakeServer(_respond(f"OK: {password}\n".encode("utf-8"))) as server:
        assert fetch_password(_settings(server.endpoint), "app", "user") == password


def test_max_length_names_are_accepted() -> None:
    db_name = "d" * 256
    user_name = "u" * 256
    with FakeServer(_respond(b"OK: pw\n")) as server:
        assert fetch_password(_settings(server.endpoint), db_name, user_name) == "pw"
    expected = b"\x0e\x02 " + db_name.encode() + b" " + user_name.encode() + b"\n"
    assert server.requests == [expected]


# ---------------------------------------------------------------------------
# Failover and endpoint failures
# ---------------------------------------------------------------------------


def test_failover_when_first_endpoint_refuses_connections() -> None:
    refused = DbpmEndpoint(host="127.0.0.1", port=_closed_port())
    with FakeServer(_respond(b"OK: from-second\n")) as server:
        password = fetch_password(_settings(refused, server.endpoint), "app", "user")

    assert password == "from-second"


def test_failover_when_first_endpoint_returns_an_error() -> None:
    with FakeServer(_respond(b"DENIED: not today\n")) as first:
        with FakeServer(_respond(b"OK: from-second\n")) as second:
            password = fetch_password(_settings(first.endpoint, second.endpoint), "app", "user")

    assert password == "from-second"
    assert first.connections == 1


def test_error_response_is_reported() -> None:
    with FakeServer(_respond(b"DENIED: no such app\n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.code == ALL_ENDPOINTS_FAILED
    assert info.value.attempts == ((0, ERROR_RESPONSE),)
    assert "DENIED" not in str(info.value)


def test_all_endpoints_failed_reports_attempt_categories() -> None:
    first = DbpmEndpoint(host="127.0.0.1", port=_closed_port())
    second = DbpmEndpoint(host="127.0.0.1", port=_closed_port())
    with pytest.raises(DbpmError) as info:
        fetch_password(_settings(first, second), "app", "user")

    assert info.value.code == ALL_ENDPOINTS_FAILED
    assert info.value.attempts == ((0, "CONNECT_FAILED"), (1, "CONNECT_FAILED"))
    assert "127.0.0.1" not in str(info.value)


def test_frame_too_large_without_a_newline() -> None:
    payload = b"OK: " + b"a" * 5000 + b"\n"
    with FakeServer(_respond(payload)) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.code == ALL_ENDPOINTS_FAILED
    assert info.value.attempts == ((0, FRAME_TOO_LARGE),)


def test_eof_before_newline() -> None:
    with FakeServer(_respond(b"OK: partial")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, EOF),)


def test_peer_closing_without_reading_reports_eof() -> None:
    def close_without_reading(conn: socket.socket, server: FakeServer) -> None:
        server.requests.append(b"")
        # Returning closes the socket with the request still unread.

    with FakeServer(close_without_reading) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, EOF),)


def test_trailing_bytes_after_newline_are_rejected() -> None:
    with FakeServer(_respond(b"OK: secret\nextra\n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, BAD_RESPONSE),)
    assert "secret" not in str(info.value)


def test_invalid_utf8_is_rejected() -> None:
    with FakeServer(_respond(b"OK: \xff\xfe\n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, BAD_RESPONSE),)


def test_empty_password_is_rejected() -> None:
    with FakeServer(_respond(b"OK: \n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, BAD_RESPONSE),)


def test_password_containing_carriage_return_is_rejected() -> None:
    with FakeServer(_respond(b"OK: pass\rword\n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "app", "user")

    assert info.value.attempts == ((0, BAD_RESPONSE),)
    assert "pass" not in str(info.value)


# ---------------------------------------------------------------------------
# Deadlines
# ---------------------------------------------------------------------------


def test_silent_server_times_out() -> None:
    with FakeServer(_hold_open) as server:
        started = time.monotonic()
        with pytest.raises(DbpmError) as info:
            fetch_password(
                _settings(server.endpoint, endpoint_timeout_ms=200),
                "app",
                "user",
            )
        elapsed = time.monotonic() - started

    assert info.value.attempts == ((0, TIMEOUT),)
    assert elapsed < 1.5


def test_slow_drip_exceeding_endpoint_timeout_reports_timeout() -> None:
    with FakeServer(_drip(b"OK: " + b"x" * 200 + b"\n", delay=0.05)) as server:
        started = time.monotonic()
        with pytest.raises(DbpmError) as info:
            fetch_password(
                _settings(server.endpoint, endpoint_timeout_ms=200),
                "app",
                "user",
            )
        elapsed = time.monotonic() - started

    assert info.value.attempts == ((0, TIMEOUT),)
    assert elapsed < 1.5


def test_budget_exhaustion_across_two_slow_endpoints() -> None:
    payload = b"OK: " + b"y" * 100 + b"\n"
    with FakeServer(_drip(payload, delay=0.05)) as first:
        with FakeServer(_drip(payload, delay=0.05)) as second:
            started = time.monotonic()
            with pytest.raises(DbpmError) as info:
                fetch_password(
                    _settings(
                        first.endpoint,
                        second.endpoint,
                        endpoint_timeout_ms=150,
                        budget_ms=250,
                    ),
                    "app",
                    "user",
                )
            elapsed = time.monotonic() - started

    assert info.value.code == BUDGET_EXHAUSTED
    assert elapsed < 1.5


# ---------------------------------------------------------------------------
# Entry validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("db_name", "user_name"),
    [
        ("app db", "user"),
        ("app", "us\ner"),
        ("app", "user\r"),
        ("app", "user\t"),
        ("app\x00", "user"),
        ("app\u00a0db", "user"),
        ("", "user"),
        ("app", ""),
        ("a" * 257, "user"),
        ("app", "u" * 257),
        ("é" * 129, "user"),  # 258 UTF-8 bytes
    ],
)
def test_invalid_names_are_rejected_without_connecting(
    db_name: str,
    user_name: str,
) -> None:
    with FakeServer(_respond(b"OK: never\n")) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), db_name, user_name)

        # Give a hypothetical background connect a moment to reach the server.
        time.sleep(0.05)

    assert info.value.code == INVALID_ENTRY
    assert info.value.attempts == ()
    assert server.connections == 0
    assert server.requests == []


def test_error_text_never_contains_names_password_or_response() -> None:
    response = b"DENIED hunter2 for secret-db/secret-user\n"
    with FakeServer(_respond(response)) as server:
        with pytest.raises(DbpmError) as info:
            fetch_password(_settings(server.endpoint), "secret-db", "secret-user")

    message = str(info.value)
    assert info.value.code == ALL_ENDPOINTS_FAILED
    assert info.value.attempts == ((0, ERROR_RESPONSE),)
    assert "hunter2" not in message
    assert "DENIED" not in message
    assert "secret-db" not in message
    assert "secret-user" not in message
