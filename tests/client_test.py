"""Tests for the MCP Events client (no live server; httpx MockTransport)."""

from __future__ import annotations

import json
from typing import Any, Callable

import httpx
import pytest

from drasi_mcp_agent.mcp_events.client import (
    McpEventsClient,
    McpProtocolError,
    McpRpcError,
)

Handler = Callable[[httpx.Request], httpx.Response]


def make_client(handler: Handler, bearer: str | None = "devtoken") -> McpEventsClient:
    return McpEventsClient(
        "http://mcp.test/mcp",
        bearer=bearer,
        transport=httpx.MockTransport(handler),
    )


def _body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


def _ok(req_id: Any, result: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, json={"jsonrpc": "2.0", "id": req_id, "result": result})


async def test_initialize_handshake_and_session_capture() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        body = _body(request)
        if body["method"] == "initialize":
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": body["id"],
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"events": {"listChanged": False}},
                        "serverInfo": {"name": "mcp-events-server", "version": "0.1.0"},
                    },
                },
                headers={"mcp-session-id": "sess-123"},
            )
        if body["method"] == "notifications/initialized":
            return httpx.Response(202)
        return httpx.Response(500)

    client = make_client(handler)
    caps = await client.initialize()
    assert caps == {"events": {"listChanged": False}}

    assert len(requests) == 2
    for request in requests:
        assert request.headers["accept"] == "application/json, text/event-stream"
        assert request.headers["mcp-protocol-version"] == "2025-11-25"
        assert request.headers["authorization"] == "Bearer devtoken"

    init_body = _body(requests[0])
    assert init_body["method"] == "initialize"
    assert "id" in init_body
    assert init_body["params"]["protocolVersion"] == "2025-11-25"

    notif_body = _body(requests[1])
    assert notif_body["method"] == "notifications/initialized"
    assert "id" not in notif_body  # a JSON-RPC notification carries no id
    # The session id issued on initialize is echoed on the follow-up request.
    assert requests[1].headers.get("mcp-session-id") == "sess-123"

    await client.aclose()


async def test_initialized_notification_requires_202() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = _body(request)
        if body["method"] == "initialize":
            return _ok(body["id"], {"capabilities": {}})
        return httpx.Response(200)  # wrong: should be 202 Accepted

    client = make_client(handler)
    with pytest.raises(McpProtocolError):
        await client.initialize()
    await client.aclose()


async def test_subscribe_request_body_shape() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = _body(request)
        captured["body"] = body
        return _ok(
            body["id"],
            {
                "id": "sub_a3f1c8e2",
                "refreshBefore": "2026-02-19T16:30:00Z",
                "cursor": "cursor_start_001",
                "truncated": False,
            },
        )

    client = make_client(handler)
    result = await client.subscribe(
        name="high-value-orders.changed",
        params={"min_total": 1000},
        callback_url="http://127.0.0.1:8001/mcp-events/webhook",
        secret="whsec_dGhpc2lzMzJieXRlc29mc2VjcmV0Zm9ydGVzdA==",
        ttl_ms=60000,
        cursor=None,
    )

    body = captured["body"]
    assert body["method"] == "events/subscribe"
    params = body["params"]
    assert params["name"] == "high-value-orders.changed"
    # delivery object must be exactly {mode, url, secret}.
    assert params["delivery"] == {
        "mode": "webhook",
        "url": "http://127.0.0.1:8001/mcp-events/webhook",
        "secret": "whsec_dGhpc2lzMzJieXRlc29mc2VjcmV0Zm9ydGVzdA==",
    }
    assert params["params"] == {"min_total": 1000}
    assert params["ttlMs"] == 60000
    # cursor is present even when null ("start from now").
    assert "cursor" in params
    assert params["cursor"] is None

    assert result == {
        "id": "sub_a3f1c8e2",
        "refreshBefore": "2026-02-19T16:30:00Z",
        "cursor": "cursor_start_001",
        "truncated": False,
    }
    await client.aclose()


async def test_subscribe_omits_optionals_and_forwards_cursor() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = _body(request)
        captured["body"] = body
        return _ok(body["id"], {"id": "s", "refreshBefore": None, "cursor": None, "truncated": False})

    client = make_client(handler)
    await client.subscribe(
        name="high-value-orders.changed",
        params=None,
        callback_url="http://hook/cb",
        secret="whsec_x",
        ttl_ms=None,
        cursor="cursor-7",
    )

    params = captured["body"]["params"]
    assert "params" not in params  # omitted when None
    assert "ttlMs" not in params  # omitted (server default) when None
    assert params["cursor"] == "cursor-7"
    await client.aclose()


async def test_unsubscribe_body_shape_and_returns_none() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = _body(request)
        captured["body"] = body
        return _ok(body["id"], {})

    client = make_client(handler)
    out = await client.unsubscribe(
        name="high-value-orders.changed",
        params={"min_total": 1000},
        callback_url="http://hook/cb",
    )
    assert out is None

    body = captured["body"]
    assert body["method"] == "events/unsubscribe"
    delivery = body["params"]["delivery"]
    assert delivery == {"url": "http://hook/cb"}  # no mode, no secret
    assert body["params"]["params"] == {"min_total": 1000}
    await client.aclose()


async def test_json_rpc_error_maps_to_mcp_rpc_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = _body(request)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": body["id"],
                "error": {
                    "code": -32014,
                    "message": "Unsupported",
                    "data": {"feature": "deliveryMode", "value": "webhook"},
                },
            },
        )

    client = make_client(handler)
    with pytest.raises(McpRpcError) as excinfo:
        await client.subscribe(
            name="e",
            params=None,
            callback_url="http://hook/cb",
            secret="whsec_x",
            ttl_ms=1000,
        )
    err = excinfo.value
    assert err.code == -32014
    assert err.message == "Unsupported"
    assert err.data == {"feature": "deliveryMode", "value": "webhook"}
    await client.aclose()


async def test_sse_unary_response_extraction() -> None:
    """A unary POST answered as text/event-stream: response sits in a data frame."""

    def handler(request: httpx.Request) -> httpx.Response:
        req_id = _body(request)["id"]
        frame = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "events": [
                        {"name": "high-value-orders.changed", "delivery": ["webhook"]}
                    ]
                },
            }
        )
        sse = f"event: message\ndata: {frame}\n\n"
        return httpx.Response(
            200, content=sse.encode(), headers={"content-type": "text/event-stream"}
        )

    client = make_client(handler)
    events = await client.list_events()
    assert events == [{"name": "high-value-orders.changed", "delivery": ["webhook"]}]
    await client.aclose()


async def test_sse_unary_error_maps_to_mcp_rpc_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        req_id = _body(request)["id"]
        frame = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32011, "message": "NotFound", "data": {"kind": "event"}},
            }
        )
        sse = f"data: {frame}\n\n"
        return httpx.Response(
            200,
            content=sse.encode(),
            headers={"content-type": "text/event-stream; charset=utf-8"},
        )

    client = make_client(handler)
    with pytest.raises(McpRpcError) as excinfo:
        await client.list_events()
    assert excinfo.value.code == -32011
    assert excinfo.value.data == {"kind": "event"}
    await client.aclose()


async def test_sse_skips_non_response_frames_and_matches_id() -> None:
    """A leading notification frame must be skipped; the id-matching frame wins."""

    def handler(request: httpx.Request) -> httpx.Response:
        req_id = _body(request)["id"]
        notif = json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/message", "params": {"x": 1}}
        )
        # A response-shaped frame with a non-matching id, then the real one.
        wrong = json.dumps({"jsonrpc": "2.0", "id": 999, "result": {"events": [{"n": "x"}]}})
        right = json.dumps({"jsonrpc": "2.0", "id": req_id, "result": {"events": []}})
        sse = f"data: {notif}\n\ndata: {wrong}\n\ndata: {right}\n\n"
        return httpx.Response(
            200, content=sse.encode(), headers={"content-type": "text/event-stream"}
        )

    client = make_client(handler)
    events = await client.list_events()
    assert events == []  # the id-matching frame, not the id=999 decoy
    await client.aclose()


async def test_undecodable_response_raises_protocol_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", headers={"content-type": "application/json"})

    client = make_client(handler)
    with pytest.raises(McpProtocolError):
        await client.list_events()
    await client.aclose()
