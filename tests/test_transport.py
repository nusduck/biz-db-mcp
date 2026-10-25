import pytest
from starlette.testclient import TestClient

from biz_db_mcp.server import create_server
from biz_db_mcp.transport import build_http_app
from conftest import make_settings

TOKEN = "test-token-value"
HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}
INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}


@pytest.fixture
def client():
    settings = make_settings()
    app = build_http_app(create_server(settings), token=TOKEN, max_request_bytes=4096)
    with TestClient(app) as test_client:
        yield test_client


def _post(client, payload, token=TOKEN, **kw):
    headers = dict(HEADERS)
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    return client.post("/mcp", json=payload, headers=headers, **kw)


def _json_body(response):
    import json

    text = response.text
    if response.headers.get("content-type", "").startswith("text/event-stream"):
        data = [line[5:].strip() for line in text.splitlines() if line.startswith("data:")]
        return json.loads(data[-1])
    return response.json()


def test_missing_or_wrong_token_is_rejected(client):
    assert _post(client, INIT, token=None).status_code == 401
    assert _post(client, INIT, token="wrong").status_code == 401
    assert client.get("/mcp").status_code == 401


def test_oversized_request_is_rejected(client):
    big = dict(INIT, params=dict(INIT["params"], pad="x" * 5000))
    assert _post(client, big).status_code == 413


def test_initialize_and_tools_list_with_token(client):
    assert _post(client, INIT).status_code == 200
    listed = _json_body(_post(client, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}))
    tools = {tool["name"]: tool for tool in listed["result"]["tools"]}
    assert {"list_databases", "list_tables", "describe_table", "query", "explain", "export"} <= set(tools)
    assert "execute" not in tools and "import_rows" not in tools
    for tool in tools.values():
        schema = tool["inputSchema"]
        assert schema["properties"]["instituteid"]["type"] == "string"
        assert "instituteid" in schema["required"]


def test_unknown_institution_is_refused(client):
    _post(client, INIT)
    called = _json_body(
        _post(
            client,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "list_databases", "arguments": {"instituteid": "0899999999"}},
            },
        )
    )
    result = called["result"]
    assert result["isError"] is True
    assert "FORBIDDEN_INSTITUTION" in result["content"][0]["text"]


def test_length_prefixed_and_bare_codes_are_the_same_institution(client):
    _post(client, INIT)
    for raw in ("0800002000", "00002000"):
        called = _json_body(
            _post(
                client,
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {"name": "list_databases", "arguments": {"instituteid": raw}},
                },
            )
        )
        assert called["result"]["isError"] is False
        assert '"primary"' in called["result"]["content"][0]["text"]
