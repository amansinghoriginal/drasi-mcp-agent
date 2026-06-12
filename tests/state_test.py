"""Tests for ``drasi_mcp_agent.state`` — the async-safe receiver/manager handoff.

No Dapr, no network: ``AgentEventState`` is a plain in-memory structure. The
``schedule`` callback is duck-typed, so the tests pass a lightweight stand-in
for an ``EventOccurrence`` (the real type lives in ``mcp_events.wire``).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from drasi_mcp_agent.state import (
    DEFAULT_DEDUP_CAPACITY,
    AgentEventState,
    Subscription,
)

SECRET = b"\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff" * 2


def _occ(event_id: str = "evt_1") -> SimpleNamespace:
    """A minimal EventOccurrence stand-in for the schedule callback."""
    return SimpleNamespace(
        event_id=event_id,
        name="high-value-orders.changed",
        timestamp="2026-06-11T16:00:00Z",
        data={"orderId": 42},
        cursor="cursor_1",
    )


def test_pending_before_active_flow() -> None:
    st = AgentEventState()
    assert st.current() is None

    st.set_pending("high-value-orders.changed", {"min": 1000}, SECRET)
    sub = st.current()
    assert isinstance(sub, Subscription)
    assert sub.name == "high-value-orders.changed"
    assert sub.params == {"min": 1000}
    assert sub.secret == SECRET
    # Pending: routing id / TTL / cursor not known until subscribe returns.
    assert sub.sub_id is None
    assert sub.refresh_before is None
    assert sub.cursor is None

    st.set_active("sub_abc", "2026-06-11T16:30:00Z", "cursor_start_001")
    active = st.current()
    assert active is not None
    assert active.sub_id == "sub_abc"
    assert active.refresh_before == "2026-06-11T16:30:00Z"
    assert active.cursor == "cursor_start_001"
    # Immutable fields preserved across the promotion.
    assert active.name == "high-value-orders.changed"
    assert active.params == {"min": 1000}
    assert active.secret == SECRET


def test_set_active_without_pending_raises() -> None:
    st = AgentEventState()
    with pytest.raises(RuntimeError):
        st.set_active("sub_abc", None, None)


def test_secret_for_by_id() -> None:
    st = AgentEventState()
    st.set_pending("e", None, SECRET)
    st.set_active("sub_abc", None, None)
    assert st.secret_for("sub_abc") == SECRET


def test_secret_for_fallback_to_single_during_verification() -> None:
    # Pending subscription: sub_id is still None, but the verification POST
    # carries the derived id in X-MCP-Subscription-Id. The lookup must fall
    # back to the single held secret so verification can succeed.
    st = AgentEventState()
    st.set_pending("e", None, SECRET)
    assert st.secret_for("sub_id_we_have_not_been_told_yet") == SECRET
    # No id at all also resolves to the single secret.
    assert st.secret_for(None) == SECRET


def test_secret_for_fallback_when_active_id_differs() -> None:
    # Single-subscription demo: a non-matching id still resolves to the one
    # held secret (routing handle is informational here).
    st = AgentEventState()
    st.set_pending("e", None, SECRET)
    st.set_active("sub_abc", None, None)
    assert st.secret_for("some_other_id") == SECRET


def test_secret_for_none_when_empty() -> None:
    st = AgentEventState()
    assert st.secret_for("anything") is None
    assert st.secret_for(None) is None


def test_seen_dedup() -> None:
    st = AgentEventState()
    assert st.seen("evt_1") is False  # first sight -> not a duplicate
    assert st.seen("evt_1") is True  # redelivery -> duplicate
    assert st.seen("evt_2") is False  # different id -> not a duplicate
    assert st.seen("evt_2") is True


def test_seen_lru_eviction() -> None:
    st = AgentEventState(dedup_capacity=3)
    assert st.seen("a") is False
    assert st.seen("b") is False
    assert st.seen("c") is False  # set now [a, b, c]
    assert st.seen("d") is False  # exceeds cap -> evict oldest "a" -> [b, c, d]

    # "a" was evicted, so it is treated as new again (inserting it evicts "b").
    assert st.seen("a") is False  # [c, d, a]
    # "d" was never evicted and is still remembered.
    assert st.seen("d") is True
    # "b" got evicted when "a" was reinserted.
    assert st.seen("b") is False


def test_seen_reseeing_refreshes_recency() -> None:
    st = AgentEventState(dedup_capacity=3)
    st.seen("a")
    st.seen("b")
    st.seen("c")  # [a, b, c]
    assert st.seen("a") is True  # touch -> most recent: [b, c, a]
    st.seen("d")  # exceeds cap -> evict oldest "b" -> [c, a, d]
    assert st.seen("a") is True  # survived because it was refreshed
    assert st.seen("b") is False  # "b" was the one evicted


def test_dedup_capacity_default() -> None:
    st = AgentEventState()
    for i in range(DEFAULT_DEDUP_CAPACITY):
        assert st.seen(f"evt_{i}") is False
    # All still remembered at exactly capacity.
    assert st.seen("evt_0") is True
    # One more distinct id evicts the now-oldest entry ("evt_1", since touching
    # "evt_0" above moved it to the back).
    assert st.seen("evt_overflow") is False
    assert st.seen("evt_1") is False  # evicted


def test_invalid_capacity_rejected() -> None:
    with pytest.raises(ValueError):
        AgentEventState(dedup_capacity=0)


def test_schedule_attribute_settable_and_awaitable() -> None:
    st = AgentEventState()
    assert st.schedule is None

    captured: list[object] = []

    async def fake_schedule(occ: object) -> str:
        captured.append(occ)
        return "workflow-instance-id"

    st.schedule = fake_schedule
    assert st.schedule is fake_schedule
    assert captured == []


@pytest.mark.asyncio
async def test_schedule_invocation_returns_instance_id() -> None:
    st = AgentEventState()
    captured: list[object] = []

    async def fake_schedule(occ: object) -> str:
        captured.append(occ)
        return "workflow-instance-id"

    st.schedule = fake_schedule
    occ = _occ()
    assert st.schedule is not None
    result = await st.schedule(occ)
    assert result == "workflow-instance-id"
    assert captured == [occ]


def test_set_cursor_advances_watermark() -> None:
    st = AgentEventState()
    st.set_pending("e", None, SECRET)
    st.set_active("sub_abc", "2026-06-11T16:30:00Z", "cursor_1")
    st.set_cursor("cursor_2")
    current = st.current()
    assert current is not None
    assert current.cursor == "cursor_2"


def test_set_cursor_noop_without_subscription() -> None:
    st = AgentEventState()
    st.set_cursor("cursor_x")  # must not raise
    assert st.current() is None
