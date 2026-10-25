"""Streamable HTTP deployment: bearer authentication and request size bound.

Every HTTP request (initialize, tools/list, tools/call, GET/DELETE on the
endpoint) must carry ``Authorization: Bearer <token>``. Only the platform holds
the token, which is what makes the host-injected ``instituteid`` trustworthy.
"""

from __future__ import annotations

import hmac
import json
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RequestTooLarge(Exception):
    """Raised inside the ASGI receive wrapper when the body exceeds the bound."""


async def _respond(
    send: Send, status: int, code: str, headers: tuple[tuple[bytes, bytes], ...] = ()
) -> None:
    body = json.dumps({"error": code}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                *headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BearerAuthMiddleware:
    """Reject unauthenticated or oversized HTTP requests before the MCP app sees them."""

    def __init__(self, app: ASGIApp, *, token: str, max_request_bytes: int) -> None:
        if not token.strip():
            raise ValueError("bearer token must not be empty")
        self._app = app
        self._expected = f"Bearer {token}".encode()
        self._max_request_bytes = max_request_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            # Lifespan events carry no request; pass them through.
            await self._app(scope, receive, send)
            return
        if not self._authorized(scope):
            await _respond(send, 401, "UNAUTHORIZED", ((b"www-authenticate", b"Bearer"),))
            return
        declared = _content_length(scope)
        if declared is not None and declared > self._max_request_bytes:
            await _respond(send, 413, "TOO_LARGE")
            return
        started = False

        async def tracking_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self._app(scope, self._bounded(receive), tracking_send)
        except RequestTooLarge:
            if not started:
                await _respond(send, 413, "TOO_LARGE")

    def _authorized(self, scope: Scope) -> bool:
        values = [v for k, v in scope.get("headers", []) if k == b"authorization"]
        if len(values) != 1:
            return False
        return hmac.compare_digest(values[0], self._expected)

    def _bounded(self, receive: Receive) -> Receive:
        seen = 0
        limit = self._max_request_bytes

        async def wrapped() -> Message:
            nonlocal seen
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > limit:
                    raise RequestTooLarge()
            return message

        return wrapped


def _content_length(scope: Scope) -> int | None:
    for key, value in scope.get("headers", []):
        if key == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


def build_http_app(mcp: Any, *, token: str, max_request_bytes: int) -> ASGIApp:
    """Wrap FastMCP's streamable HTTP Starlette app with authentication."""
    return BearerAuthMiddleware(
        mcp.streamable_http_app(), token=token, max_request_bytes=max_request_bytes
    )
