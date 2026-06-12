"""Shared, async-safe handoff between the SubscriptionManager and the receiver.

``AgentEventState`` is the single mutable rendezvous point in the agent:

* the ``SubscriptionManager`` (writer) records the pending/active subscription
  and the secret needed to verify inbound deliveries, and
* the webhook ``receiver`` (reader) looks the secret up by
  ``X-MCP-Subscription-Id`` (falling back to the single held secret during the
  verification handshake), deduplicates by ``eventId`` and wakes the agent via
  the settable :attr:`AgentEventState.schedule` callback.

Both run on the same FastAPI/asyncio event loop, but the receiver route is a
raw ``async def`` handler and the manager runs its own refresh task, so a small
``threading.Lock`` guards every mutation. The lock is only ever held for
non-awaiting, O(1) critical sections, so it never blocks the loop.

Protocol behaviour mirrored here:
``docs/design-sketch-proposal.md`` (Webhook Event Delivery, Subscription
Identity, Non-event webhook bodies) and the reference server in
``../drasi-mcp-events``.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from .mcp_events.wire import EventOccurrence

    ScheduleCallback = Callable[[EventOccurrence], Awaitable[str]]

#: Default bound on the dedup LRU. See ``seen`` and the SPEC-FINDINGS note on
#: how long a consumer must remember ``eventId``\\s.
DEFAULT_DEDUP_CAPACITY = 1024


@dataclass
class Subscription:
    """One webhook subscription as the agent tracks it.

    ``secret`` is the raw HMAC key (already ``whsec_``-decoded bytes) supplied
    by the client at subscribe time and used to verify inbound deliveries.
    ``sub_id`` is the server-derived routing id (``X-MCP-Subscription-Id``); it
    is ``None`` while the subscription is *pending* — i.e. ``events/subscribe``
    has not yet returned, which is exactly the window in which the verification
    callback arrives.
    """

    name: str
    params: dict | None
    secret: bytes
    sub_id: str | None
    refresh_before: str | None
    cursor: str | None


class AgentEventState:
    """Holds at most one :class:`Subscription` plus a bounded dedup set.

    The demo subscribes to a single event type, so the state intentionally
    tracks one subscription. ``secret_for`` therefore resolves by id when it
    can and otherwise falls back to the single held secret — which is what lets
    the verification handshake succeed before the subscribe response (carrying
    the id) has returned.
    """

    def __init__(self, dedup_capacity: int = DEFAULT_DEDUP_CAPACITY) -> None:
        if dedup_capacity < 1:
            raise ValueError("dedup_capacity must be >= 1")
        self._lock = threading.Lock()
        self._sub: Subscription | None = None
        self._dedup_capacity = dedup_capacity
        # Ordered by recency: oldest at the front, most-recent at the back.
        self._seen: OrderedDict[str, None] = OrderedDict()
        #: Set by the activation hook to schedule the DurableAgent workflow.
        #: The receiver awaits it to wake the agent and returns the instance id.
        self.schedule: ScheduleCallback | None = None

    def set_pending(self, name: str, params: dict | None, secret: bytes) -> None:
        """Record the subscription before calling ``events/subscribe``.

        Called BEFORE the subscribe round-trip so the verification callback can
        find the secret while the subscribe call is still in flight. The
        ``sub_id``/``refresh_before``/``cursor`` are filled in by
        :meth:`set_active` once the response arrives.
        """
        with self._lock:
            self._sub = Subscription(
                name=name,
                params=params,
                secret=secret,
                sub_id=None,
                refresh_before=None,
                cursor=None,
            )

    def set_active(
        self,
        sub_id: str,
        refresh_before: str | None,
        cursor: str | None,
    ) -> None:
        """Promote the pending subscription with the subscribe-response fields.

        Mutates the existing pending :class:`Subscription` in place (preserving
        ``name``/``params``/``secret``). Raises if no subscription is pending,
        since ``sub_id``/``refresh_before``/``cursor`` alone cannot reconstruct
        one.
        """
        with self._lock:
            if self._sub is None:
                raise RuntimeError("set_active called before set_pending")
            self._sub.sub_id = sub_id
            self._sub.refresh_before = refresh_before
            self._sub.cursor = cursor

    def set_cursor(self, cursor: str | None) -> None:
        """Persist the latest safe-to-persist watermark cursor.

        The receiver advances this from event/gap deliveries; the manager
        advances it from refresh responses. Tolerant of a not-yet-active
        subscription (the call is a no-op if nothing is tracked).
        """
        with self._lock:
            if self._sub is not None:
                self._sub.cursor = cursor

    def current(self) -> Subscription | None:
        """Return the tracked subscription (live reference) or ``None``."""
        with self._lock:
            return self._sub

    def secret_for(self, sub_id: str | None) -> bytes | None:
        """Resolve the HMAC secret for an inbound delivery.

        Prefers an exact ``sub_id`` match; otherwise falls back to the single
        held secret. The fallback is what makes the verification handshake work:
        the verification POST carries the derived ``X-MCP-Subscription-Id``, but
        the subscribe call that would tell us that id has not yet returned, so
        ``self._sub.sub_id`` is still ``None``. Returns ``None`` only when no
        subscription is tracked at all (receiver then replies 404).
        """
        with self._lock:
            if self._sub is None:
                return None
            # Exact match, or fall back to the single held secret (pending
            # verification window, or an id we have not been told to route yet).
            return self._sub.secret

    def seen(self, event_id: str) -> bool:
        """Return ``True`` if ``event_id`` was already seen; record it if new.

        Bounded LRU: a new id is appended and, once capacity is exceeded, the
        oldest id is evicted. Re-seeing an id refreshes its recency (moves it to
        the back) so frequently redelivered ids are not evicted out from under
        an in-flight retry storm.

        This is the combined check-and-record form. Prefer the split
        :meth:`is_seen` / :meth:`mark_seen` pair when the record must only be
        committed after a side effect (e.g. a durable schedule) succeeds, so a
        failed delivery is redelivered rather than silently dedup-dropped.
        """
        with self._lock:
            if event_id in self._seen:
                self._seen.move_to_end(event_id)
                return True
            self._seen[event_id] = None
            if len(self._seen) > self._dedup_capacity:
                self._seen.popitem(last=False)
            return False

    def is_seen(self, event_id: str) -> bool:
        """Read-only dedup check: ``True`` if already recorded (does not record).

        Refreshes recency on a hit so an id under a retry storm is not evicted,
        but never inserts — pair with :meth:`mark_seen` after the event is
        durably handled.
        """
        with self._lock:
            if event_id in self._seen:
                self._seen.move_to_end(event_id)
                return True
            return False

    def mark_seen(self, event_id: str) -> None:
        """Record ``event_id`` as handled (bounded-LRU insert + evict)."""
        with self._lock:
            self._seen[event_id] = None
            self._seen.move_to_end(event_id)
            if len(self._seen) > self._dedup_capacity:
                self._seen.popitem(last=False)
