"""Tests for ``drasi_mcp_agent.receiver`` — the inbound webhook handler.

No Dapr, no network. We use the *real* :class:`AgentEventState` seeded with a
known secret (the cleanest "fake": fully in-memory) and craft genuinely signed
requests with :func:`drasi_mcp_agent.mcp_events.wire.sign`, so the receiver
verifies them exactly as it would a delivery from the reference Rust server.

Coverage: verification echo, signature/tamper rejection, timestamp freshness,
dedup, gap cursor capture, terminated re-subscribe signal, and the happy-path
event scheduling exactly once.
"""

from __future__ import annotations

import json
import time

import pytest
from starlette.requests import Request
from starlette.responses import Response

from drasi_mcp_agent.mcp_events.wire import (
    MCP_SUBSCRIPTION_ID,
    WEBHOOK_ID,
    WEBHOOK_SIGNATURE,
    WEBHOOK_TIMESTAMP,
    EventOccurrence,
    sign,
)
from drasi_mcp_agent.receiver import (
    RESUBSCRIBE_ATTR,
    handle_webhook,
    make_webhook_route,
)
from drasi_mcp_agent.state import AgentEventState

# A fixed, known 32-byte HMAC key (already whsec_-decoded, as stored in state).
SECRET = bytes(range(1, 33))
EVENT_NAME = "high-value-orders.changed"
SUB_ID = "sub_demo_a3f1c8e2"


# --- helpers -----------------------------------------------------------------


def make_state(*, secret: bytes | None = SECRET, pending: bool = True) -> AgentEventState:
    """Real in-memory state, optionally pre-seeded with a pending subscription."""
    st = AgentEventState()
    if pending and secret is not None:
        st.set_pending(EVENT_NAME, {"min": 1000}, secret)
    return st


def build_request(body: bytes, headers: dict[str, str]) -> Request:
    """Construct a minimal ASGI POST :class:`Request` carrying ``body``."""
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "path": "/mcp-events/webhook",
        "raw_path": b"/mcp-events/webhook",
        "query_string": b"",
        "headers": raw_headers,
        "scheme": "http",
        "server": ("127.0.0.1", 8001),
        "client": ("127.0.0.1", 54321),
    }
    state = {"sent": False}

    async def receive() -> dict:
        if state["sent"]:
            return {"type": "http.disconnect"}
        state["sent"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def signed_request(
    body: bytes,
    *,
    secret: bytes = SECRET,
    msg_id: str = "evt_1",
    ts: int | None = None,
    sub_id: str | None = SUB_ID,
    signature: str | None = None,
    drop_signature: bool = False,
) -> Request:
    """Build a Standard-Webhooks-signed request over the exact ``body`` bytes."""
    if ts is None:
        ts = int(time.time())
    if signature is None:
        signature = sign(secret, msg_id, ts, body)
    headers = {
        WEBHOOK_ID: msg_id,
        WEBHOOK_TIMESTAMP: str(ts),
        "content-type": "application/json",
    }
    if not drop_signature:
        headers[WEBHOOK_SIGNATURE] = signature
    if sub_id is not None:
        headers[MCP_SUBSCRIPTION_ID] = sub_id
    return build_request(body, headers)


def event_body(event_id: str = "evt_42", cursor: str | None = "cursor_7") -> bytes:
    """A canonical ``EventOccurrence`` delivery body (no top-level ``type``)."""
    payload = {
        "eventId": event_id,
        "name": EVENT_NAME,
        "timestamp": "2026-06-11T16:00:00Z",
        "data": {"orderId": 42, "customer": "alice", "total": 5000, "change": "ADDED"},
    }
    if cursor is not None:
        payload["cursor"] = cursor
    return json.dumps(payload).encode()


def collecting_schedule() -> tuple[list[EventOccurrence], object]:
    """A schedule sink capturing each occurrence and returning an instance id."""
    seen: list[EventOccurrence] = []

    async def schedule(occ: EventOccurrence) -> str:
        seen.append(occ)
        return f"wf-{len(seen)}"

    return seen, schedule


def body_of(response: Response) -> dict:
    return json.loads(bytes(response.body))


# --- verification handshake --------------------------------------------------


@pytest.mark.asyncio
async def test_verification_echoes_challenge() -> None:
    st = make_state()
    body = json.dumps({"type": "verification", "challenge": "nonce-abc123"}).encode()
    req = signed_request(body, msg_id="msg_verification_xyz")

    resp = await handle_webhook(req, st)

    assert resp.status_code == 200
    # Exact echo: the server compares this to its nonce in constant time.
    assert body_of(resp) == {"challenge": "nonce-abc123"}


@pytest.mark.asyncio
async def test_verification_works_before_subscribe_id_known() -> None:
    # During the handshake the subscribe response (carrying the routing id) has
    # not returned, so sub_id is unknown to us; secret_for must still resolve.
    st = make_state()
    body = json.dumps({"type": "verification", "challenge": "n1"}).encode()
    req = signed_request(body, sub_id="an-id-we-were-not-told-about")

    resp = await handle_webhook(req, st)

    assert resp.status_code == 200
    assert body_of(resp) == {"challenge": "n1"}


# --- signature / freshness rejection -----------------------------------------


@pytest.mark.asyncio
async def test_tampered_body_rejected_401() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]

    signed_over = event_body(event_id="evt_99")
    signature = sign(SECRET, "evt_99", int(time.time()), signed_over)
    tampered = event_body(event_id="evt_99", cursor="cursor_TAMPERED")
    # Reuse the signature computed over the *original* bytes; the body differs.
    req = signed_request(tampered, msg_id="evt_99", signature=signature)

    resp = await handle_webhook(req, st)

    assert resp.status_code == 401
    assert seen == []  # never processed


@pytest.mark.asyncio
async def test_wrong_secret_rejected_401() -> None:
    st = make_state()
    body = event_body()
    # Sign with a different key than the one state holds.
    req = signed_request(body, secret=bytes(range(100, 132)))

    resp = await handle_webhook(req, st)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_stale_timestamp_rejected_401() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    body = event_body()
    old_ts = int(time.time()) - 10_000  # well outside the 5-min window
    req = signed_request(body, ts=old_ts)  # signature is valid for old_ts

    resp = await handle_webhook(req, st)

    assert resp.status_code == 401
    assert seen == []


@pytest.mark.asyncio
async def test_missing_signature_header_rejected_401() -> None:
    st = make_state()
    req = signed_request(event_body(), drop_signature=True)
    resp = await handle_webhook(req, st)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_non_integer_timestamp_rejected_401() -> None:
    st = make_state()
    body = event_body()
    headers = {
        WEBHOOK_ID: "evt_1",
        WEBHOOK_TIMESTAMP: "not-a-number",
        WEBHOOK_SIGNATURE: sign(SECRET, "evt_1", int(time.time()), body),
        MCP_SUBSCRIPTION_ID: SUB_ID,
    }
    resp = await handle_webhook(build_request(body, headers), st)
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_no_subscription_returns_404() -> None:
    st = make_state(pending=False)  # nothing tracked → no secret
    resp = await handle_webhook(signed_request(event_body()), st)
    assert resp.status_code == 404


# --- gap / terminated control envelopes --------------------------------------


@pytest.mark.asyncio
async def test_gap_captures_fresh_cursor() -> None:
    st = make_state()
    st.set_active(SUB_ID, "2026-06-11T16:30:00Z", "cursor_old")
    body = json.dumps({"type": "gap", "cursor": "cursor_fresh"}).encode()

    resp = await handle_webhook(signed_request(body, msg_id="msg_gap_1"), st)

    assert resp.status_code == 200
    assert body_of(resp) == {}
    current = st.current()
    assert current is not None
    assert current.cursor == "cursor_fresh"


@pytest.mark.asyncio
async def test_terminated_signals_resubscribe() -> None:
    st = make_state()
    st.set_active(SUB_ID, None, "cursor_x")
    calls: list[bool] = []

    async def request_resubscribe() -> None:
        calls.append(True)

    setattr(st, RESUBSCRIBE_ATTR, request_resubscribe)
    body = json.dumps(
        {
            "type": "terminated",
            "error": {"code": -32012, "message": "Forbidden", "data": {"reason": "x"}},
        }
    ).encode()

    resp = await handle_webhook(signed_request(body, msg_id="msg_terminated_1"), st)

    assert resp.status_code == 200
    assert body_of(resp) == {}
    assert calls == [True]


@pytest.mark.asyncio
async def test_terminated_without_resubscribe_hook_still_acks() -> None:
    st = make_state()
    body = json.dumps(
        {"type": "terminated", "error": {"code": -32012, "message": "Forbidden"}}
    ).encode()
    resp = await handle_webhook(signed_request(body, msg_id="msg_terminated_2"), st)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_unknown_control_type_is_acked_not_processed() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    body = json.dumps({"type": "future-thing", "foo": 1}).encode()

    resp = await handle_webhook(signed_request(body, msg_id="msg_future_1"), st)

    assert resp.status_code == 200
    assert body_of(resp) == {"ignored": "future-thing"}
    assert seen == []


# --- event delivery ----------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_event_schedules_exactly_once() -> None:
    st = make_state()
    st.set_active(SUB_ID, None, "cursor_old")
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]

    resp = await handle_webhook(signed_request(event_body(), msg_id="evt_42"), st)

    assert resp.status_code == 200
    assert body_of(resp) == {"scheduled": True}
    assert len(seen) == 1
    occ = seen[0]
    assert isinstance(occ, EventOccurrence)
    assert occ.event_id == "evt_42"
    assert occ.name == EVENT_NAME
    assert occ.data["customer"] == "alice"
    assert occ.cursor == "cursor_7"
    # Cursor persisted for resubscribe-time replay.
    current = st.current()
    assert current is not None
    assert current.cursor == "cursor_7"


@pytest.mark.asyncio
async def test_duplicate_event_id_schedules_once() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    body = event_body(event_id="evt_dup")

    first = await handle_webhook(signed_request(body, msg_id="evt_dup"), st)
    # A retry: same eventId, fresh timestamp + signature (server regenerates).
    second = await handle_webhook(signed_request(body, msg_id="evt_dup"), st)

    assert first.status_code == 200
    assert body_of(first) == {"scheduled": True}
    assert second.status_code == 200
    assert body_of(second) == {"dedup": True}
    assert len(seen) == 1  # scheduled exactly once despite two deliveries


@pytest.mark.asyncio
async def test_event_without_schedule_sink_asks_retry_503() -> None:
    st = make_state()  # st.schedule is None (activation not run)
    resp = await handle_webhook(signed_request(event_body()), st)
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_schedule_failure_does_not_dedup_or_advance_cursor() -> None:
    # Regression: if the durable schedule fails, the event must NOT be marked
    # seen or the cursor advanced, so the server's retry is processed (not
    # silently dedup-dropped). The retry, with a working sink, must schedule.
    st = make_state()
    st.set_active(SUB_ID, None, "cursor_old")

    async def boom(_occ: EventOccurrence) -> str:
        raise RuntimeError("workflow runtime down")

    st.schedule = boom  # type: ignore[assignment]
    first = await handle_webhook(signed_request(event_body(), msg_id="evt_99"), st)
    assert first.status_code == 503
    # Not committed: cursor unchanged, id not recorded.
    current = st.current()
    assert current is not None and current.cursor == "cursor_old"
    assert st.is_seen("evt_99") is False

    # Retry now succeeds and schedules.
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    second = await handle_webhook(signed_request(event_body(), msg_id="evt_99"), st)
    assert second.status_code == 200
    assert body_of(second) == {"scheduled": True}
    assert len(seen) == 1
    current = st.current()
    assert current is not None and current.cursor == "cursor_7"


@pytest.mark.asyncio
async def test_malformed_event_body_rejected_400() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    # Signed, authentic, but missing required occurrence fields and no type.
    body = json.dumps({"data": {"x": 1}}).encode()

    resp = await handle_webhook(signed_request(body, msg_id="evt_bad"), st)

    assert resp.status_code == 400
    assert seen == []


# --- route factory -----------------------------------------------------------


@pytest.mark.asyncio
async def test_make_webhook_route_binds_state() -> None:
    st = make_state()
    seen, st.schedule = collecting_schedule()  # type: ignore[assignment]
    route = make_webhook_route(st)

    resp = await route(signed_request(event_body(event_id="evt_routed"), msg_id="evt_routed"))

    assert resp.status_code == 200
    assert len(seen) == 1
    assert seen[0].event_id == "evt_routed"
