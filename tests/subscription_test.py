"""Tests for ``drasi_mcp_agent.subscription`` — the webhook lifecycle manager.

No network, no Dapr: a fake ``McpEventsClient`` records call order and lets the
tests inspect what the manager did, paired with the *real* ``AgentEventState``
so the pending-before-subscribe ordering is exercised end to end.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from drasi_mcp_agent.config import Settings
from drasi_mcp_agent.state import AgentEventState
from drasi_mcp_agent.subscription import (
    MIN_REFRESH_SECONDS,
    NO_EXPIRY_HEALTHCHECK_SECONDS,
    NoWebhookEventError,
    SubscriptionManager,
    refresh_delay_seconds,
)

EVENT = "high-value-orders.changed"


def make_settings(event_name: str = EVENT, ttl_ms: int = 60000) -> Settings:
    return Settings(
        mcp_url="http://mcp.test/mcp",
        mcp_bearer="devtoken",
        event_name=event_name,
        callback_url="http://127.0.0.1:8001/mcp-events/webhook",
        app_host="127.0.0.1",
        app_port=8001,
        ttl_ms=ttl_ms,
        anthropic_model="claude-sonnet-4-6",
        use_llm=False,
    )


class FakeClient:
    """A stand-in McpEventsClient that records calls and is fully scriptable."""

    def __init__(
        self,
        events: list[dict[str, Any]],
        *,
        st: AgentEventState | None = None,
        capabilities: dict[str, Any] | None = None,
    ) -> None:
        self._events = events
        self._st = st
        self._capabilities = capabilities if capabilities is not None else {
            "events": {"listChanged": True}
        }
        self.calls: list[str] = []
        # Observations captured during subscribe(), used by the ordering test.
        self.secret_at_subscribe: bytes | None = None
        self.subscribe_cursors: list[str | None] = []
        self.subscribe_secrets: list[str] = []
        # Scriptable subscribe response (mutate between calls in a test).
        self.subscribe_result: dict[str, Any] = {
            "id": "sub_x",
            "refreshBefore": None,
            "cursor": "cursor_start",
            "truncated": False,
        }

    async def initialize(self) -> dict[str, Any]:
        self.calls.append("initialize")
        return self._capabilities

    async def list_events(self) -> list[dict[str, Any]]:
        self.calls.append("list_events")
        return self._events

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
        self.calls.append("subscribe")
        self.subscribe_cursors.append(cursor)
        self.subscribe_secrets.append(secret)
        if self._st is not None:
            # Whatever the receiver would see for an inbound verification POST
            # at this exact instant. Non-None proves set_pending ran first.
            self.secret_at_subscribe = self._st.secret_for(None)
        return dict(self.subscribe_result)

    async def unsubscribe(
        self,
        *,
        name: str,
        params: dict[str, Any] | None,
        callback_url: str,
    ) -> None:
        self.calls.append("unsubscribe")

    async def aclose(self) -> None:
        self.calls.append("aclose")


# --- decide() ----------------------------------------------------------------


def test_decide_prefers_configured_name() -> None:
    st = AgentEventState()
    events = [
        {"name": "other.event", "delivery": ["webhook"]},
        {"name": EVENT, "delivery": ["webhook", "poll"]},
    ]
    mgr = SubscriptionManager(FakeClient(events), st, make_settings())
    assert mgr.decide(events) == (EVENT, None)


def test_decide_falls_back_to_first_webhook_capable() -> None:
    st = AgentEventState()
    events = [
        {"name": "poll.only", "delivery": ["poll"]},
        {"name": "push.only", "delivery": ["push"]},
        {"name": "hooky", "delivery": ["push", "webhook"]},
        {"name": "hooky.second", "delivery": ["webhook"]},
    ]
    # Configured event is absent, so the first webhook-capable one wins.
    mgr = SubscriptionManager(FakeClient(events), st, make_settings(event_name="absent"))
    assert mgr.decide(events) == ("hooky", None)


def test_decide_raises_when_none_webhook_capable() -> None:
    st = AgentEventState()
    events = [
        {"name": "poll.only", "delivery": ["poll"]},
        {"name": "push.only", "delivery": ["push", "poll"]},
    ]
    mgr = SubscriptionManager(FakeClient(events), st, make_settings(event_name="absent"))
    with pytest.raises(NoWebhookEventError):
        mgr.decide(events)


def test_decide_raises_on_empty_list() -> None:
    st = AgentEventState()
    mgr = SubscriptionManager(FakeClient([]), st, make_settings(event_name="absent"))
    with pytest.raises(NoWebhookEventError):
        mgr.decide([])


# --- start(): pending-before-subscribe ordering ------------------------------


async def test_start_sets_pending_before_subscribe() -> None:
    st = AgentEventState()
    events = [{"name": EVENT, "delivery": ["webhook"]}]
    client = FakeClient(events, st=st)
    mgr = SubscriptionManager(client, st, make_settings())

    await mgr.start()
    try:
        # The discovery + subscribe sequence happened in the contract order.
        assert client.calls == ["initialize", "list_events", "subscribe"]
        # The receiver had a secret to verify with the moment subscribe fired —
        # i.e. set_pending(secret) ran BEFORE client.subscribe(). This is the
        # invariant that makes the verification handshake succeed.
        assert client.secret_at_subscribe is not None
        # The first subscribe bootstraps with cursor=None ("start from now").
        assert client.subscribe_cursors == [None]

        # State was promoted to active from the subscribe response.
        sub = st.current()
        assert sub is not None
        assert sub.sub_id == "sub_x"
        assert sub.cursor == "cursor_start"
        # The secret published to state decodes back to the whsec_ we sent.
        from drasi_mcp_agent.mcp_events.wire import parse_whsec

        assert sub.secret == parse_whsec(client.subscribe_secrets[0])
    finally:
        await mgr.stop()


async def test_start_then_stop_unsubscribes() -> None:
    st = AgentEventState()
    events = [{"name": EVENT, "delivery": ["webhook"]}]
    client = FakeClient(events, st=st)
    mgr = SubscriptionManager(client, st, make_settings())

    await mgr.start()
    await mgr.stop()
    assert client.calls[-1] == "unsubscribe"


# --- refresh: cursor carried across re-subscribe -----------------------------


async def test_refresh_once_sends_persisted_cursor_and_updates_state() -> None:
    st = AgentEventState()
    events = [{"name": EVENT, "delivery": ["webhook"]}]
    client = FakeClient(events, st=st)
    mgr = SubscriptionManager(client, st, make_settings())

    await mgr.start()
    try:
        # The receiver advances the cursor from an inbound event delivery.
        st.set_cursor("cursor_from_event")
        # Next refresh response moves the watermark forward again.
        client.subscribe_result = {
            "id": "sub_x",
            "refreshBefore": "2026-06-12T00:30:00.000Z",
            "cursor": "cursor_after_refresh",
            "truncated": False,
        }
        await mgr._refresh_once()

        # The refresh re-sent the last-persisted cursor (sketch §Cursor Lifecycle).
        assert client.subscribe_cursors[-1] == "cursor_from_event"
        sub = st.current()
        assert sub is not None
        assert sub.cursor == "cursor_after_refresh"
        assert sub.refresh_before == "2026-06-12T00:30:00.000Z"
    finally:
        await mgr.stop()


# --- refresh interval computation --------------------------------------------


def test_refresh_delay_is_half_of_remaining() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # 60 s of remaining TTL → refresh at the 30 s mark.
    assert refresh_delay_seconds("2026-06-12T00:01:00.000Z", now) == pytest.approx(30.0)


def test_refresh_delay_accepts_second_precision_and_z() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # No fractional seconds, bare "Z" — still parses.
    assert refresh_delay_seconds("2026-06-12T00:10:00Z", now) == pytest.approx(300.0)


def test_refresh_delay_accepts_numeric_offset() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # +00:00 offset instead of Z; 40 s remaining → 20 s delay.
    assert refresh_delay_seconds("2026-06-12T00:00:40+00:00", now) == pytest.approx(20.0)


def test_refresh_delay_floors_short_grant() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # 2 s remaining → 1 s at 50%, floored up to MIN_REFRESH_SECONDS.
    assert refresh_delay_seconds("2026-06-12T00:00:02Z", now) == MIN_REFRESH_SECONDS


def test_refresh_delay_floors_expired_grant() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # Already past expiry → negative remaining → floored, never negative.
    assert refresh_delay_seconds("2026-06-11T23:59:00Z", now) == MIN_REFRESH_SECONDS


def test_refresh_delay_no_expiry_uses_healthcheck_cadence() -> None:
    now = datetime(2026, 6, 12, 0, 0, 0, tzinfo=timezone.utc)
    # refreshBefore: null → no finite TTL to chase; slow health-check cadence.
    assert refresh_delay_seconds(None, now) == NO_EXPIRY_HEALTHCHECK_SECONDS
