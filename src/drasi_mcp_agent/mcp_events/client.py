"""Async MCP Events client over Streamable HTTP.

Speaks the draft MCP Events extension to the Rust ``mcp-events-server``
(``../drasi-mcp-events``). Every method is a single JSON-RPC request issued as a
``POST`` to the server's ``/mcp`` endpoint. The server answers a unary request
with either an ``application/json`` JSON-RPC body or a ``text/event-stream`` body
that carries the JSON-RPC response inside a single SSE ``data:`` frame; this
client handles both framings (see ``_result_from_response`` /
``_extract_jsonrpc_from_sse``).

Concurrency note: :meth:`McpEventsClient.subscribe` blocks while the server calls
back to the agent's own ``/mcp-events/webhook`` to run the verification handshake
that unblocks ``events/subscribe``. Because this client uses
:class:`httpx.AsyncClient`, the awaiting coroutine yields the event loop so the
agent's FastAPI app can service that inbound verification ``POST`` while the
``subscribe`` await is in flight.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

#: MCP protocol version this client negotiates (matches the Rust server's
#: ``PROTOCOL_VERSION`` in ``mcp-events-wire``).
PROTOCOL_VERSION = "2025-11-25"
JSONRPC_VERSION = "2.0"

#: Sent on every request so the server may stream a single-frame SSE response.
_ACCEPT = "application/json, text/event-stream"

#: Generous default: ``subscribe`` blocks on a server-to-agent verification
#: round-trip, so the per-request budget must exceed a normal RPC.
_DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


class McpRpcError(Exception):
    """A JSON-RPC error response from the server (``error`` member present)."""

    def __init__(self, code: int, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(f"JSON-RPC error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class McpProtocolError(Exception):
    """A transport/framing fault that is *not* a JSON-RPC error.

    Raised when the HTTP response cannot be decoded into a JSON-RPC response
    (unexpected content type, empty body, no response frame in an SSE stream,
    or a notification ack with an unexpected status). Distinct from
    :class:`McpRpcError`, which carries a well-formed server error.
    """


class McpEventsClient:
    """Async client for the MCP Events extension over Streamable HTTP."""

    def __init__(
        self,
        base_url: str,
        bearer: str | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url
        self._bearer = bearer
        self._id = 0
        # Captured from the ``initialize`` response and echoed on every
        # subsequent request. The reference server issues but does not enforce
        # it; a stricter MCP server would reject requests that omit it.
        self._session_id: str | None = None
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": _ACCEPT,
            "MCP-Protocol-Version": PROTOCOL_VERSION,
        }
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=_DEFAULT_TIMEOUT,
            transport=transport,
        )

    async def initialize(self) -> dict[str, Any]:
        """Run the MCP handshake; return the server's capabilities object.

        Sends ``initialize`` and, on success, the ``notifications/initialized``
        notification (which the server acks with ``202`` and no body).
        """
        result = await self._request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "drasi-mcp-agent", "version": "0.1.0"},
            },
        )
        await self._notify("notifications/initialized")
        capabilities = result.get("capabilities", {})
        return capabilities if isinstance(capabilities, dict) else {}

    async def list_events(self) -> list[dict[str, Any]]:
        """Call ``events/list``; return the ``events[]`` array of definitions."""
        result = await self._request("events/list", None)
        events = result.get("events", [])
        return events if isinstance(events, list) else []

    async def subscribe(
        self,
        *,
        name: str,
        params: dict[str, Any] | None,
        callback_url: str,
        secret: str,
        ttl_ms: int | None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Register (or refresh) a webhook subscription via ``events/subscribe``.

        Returns the raw result object:
        ``{id, refreshBefore, cursor, truncated, deliveryStatus?}``.
        ``cursor`` is always sent (``null`` = "start from now"); ``ttlMs`` is
        omitted when ``ttl_ms is None`` (server default), and ``params`` is
        omitted when ``None``.
        """
        rpc_params: dict[str, Any] = {
            "name": name,
            "delivery": {"mode": "webhook", "url": callback_url, "secret": secret},
            "cursor": cursor,
        }
        if params is not None:
            rpc_params["params"] = params
        if ttl_ms is not None:
            rpc_params["ttlMs"] = ttl_ms
        return await self._request("events/subscribe", rpc_params)

    async def unsubscribe(
        self,
        *,
        name: str,
        params: dict[str, Any] | None,
        callback_url: str,
    ) -> None:
        """Eagerly tear down a subscription via ``events/unsubscribe``.

        The subscription key is ``(principal, url, name, params)``; the secret
        is not part of the unsubscribe ``delivery`` object.
        """
        rpc_params: dict[str, Any] = {"name": name, "delivery": {"url": callback_url}}
        if params is not None:
            rpc_params["params"] = params
        await self._request("events/unsubscribe", rpc_params)

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # -- internals ---------------------------------------------------------

    async def _request(self, method: str, params: dict[str, Any] | None) -> dict[str, Any]:
        self._id += 1
        req_id = self._id
        payload: dict[str, Any] = {"jsonrpc": JSONRPC_VERSION, "id": req_id, "method": method}
        if params is not None:
            payload["params"] = params
        response = await self._send(payload)
        return self._result_from_response(response, req_id)

    async def _notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {"jsonrpc": JSONRPC_VERSION, "method": method}
        if params is not None:
            payload["params"] = params
        response = await self._send(payload)
        if response.status_code != 202:
            raise McpProtocolError(
                f"notification {method!r} expected 202 Accepted, got {response.status_code}"
            )

    async def _send(self, payload: dict[str, Any]) -> httpx.Response:
        body = json.dumps(payload).encode("utf-8")
        headers: dict[str, str] = {}
        if self._session_id is not None:
            headers["Mcp-Session-Id"] = self._session_id
        response = await self._client.post(self._base_url, content=body, headers=headers)
        session_id = response.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id
        return response

    def _result_from_response(self, response: httpx.Response, req_id: int) -> dict[str, Any]:
        content_type = response.headers.get("content-type", "").lower()
        text = response.text
        message: Any
        if "text/event-stream" in content_type:
            message = _extract_jsonrpc_from_sse(text, req_id)
        elif "application/json" in content_type:
            message = _loads_or_none(text)
        else:
            # Unknown content type: be lenient — try JSON, then SSE.
            message = _loads_or_none(text)
            if message is None and "data:" in text:
                message = _extract_jsonrpc_from_sse(text, req_id)
        if message is None:
            raise McpProtocolError(
                f"undecodable response (HTTP {response.status_code}, "
                f"content-type {content_type!r})"
            )
        return _unwrap_rpc(message)


def _loads_or_none(text: str) -> Any | None:
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _extract_jsonrpc_from_sse(text: str, req_id: int | None) -> dict[str, Any] | None:
    """Pull a JSON-RPC response out of a Server-Sent-Events body.

    A unary ``POST`` answered with ``text/event-stream`` carries the response in
    one ``data:`` frame (``event:``/``id:``/``retry:`` lines are ignored, and
    multiple ``data:`` lines in a frame are joined with newlines). The frame
    matching ``req_id`` wins; otherwise the first response-shaped frame is used.
    """
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    fallback: dict[str, Any] | None = None
    for block in normalized.split("\n\n"):
        data_lines: list[str] = []
        for line in block.split("\n"):
            if line.startswith("data:"):
                value = line[len("data:") :]
                if value.startswith(" "):  # SSE strips exactly one leading space
                    value = value[1:]
                data_lines.append(value)
        if not data_lines:
            continue
        payload = _loads_or_none("\n".join(data_lines))
        if not isinstance(payload, dict):
            continue
        if "result" not in payload and "error" not in payload:
            continue  # a notification or some other non-response frame
        if req_id is not None and payload.get("id") == req_id:
            return payload
        if fallback is None:
            fallback = payload
    return fallback


def _unwrap_rpc(message: Any) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise McpProtocolError("response is not a JSON-RPC object")
    error = message.get("error")
    if error is not None:
        if not isinstance(error, dict):
            raise McpProtocolError(f"malformed JSON-RPC error member: {error!r}")
        data = error.get("data")
        raise McpRpcError(
            int(error.get("code", 0)),
            str(error.get("message", "")),
            data if isinstance(data, dict) else None,
        )
    if "result" not in message:
        raise McpProtocolError("JSON-RPC response has neither result nor error")
    result = message["result"]
    if not isinstance(result, dict):
        raise McpProtocolError("JSON-RPC result is not an object")
    return result
