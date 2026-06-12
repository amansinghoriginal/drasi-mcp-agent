"""Webhook subscription lifecycle for the serverless agent.

The :class:`SubscriptionManager` owns the *client side* of the draft MCP Events
webhook handshake against the reference Rust server (``../drasi-mcp-events``):

1. ``initialize`` the MCP session,
2. ``events/list`` to discover what the server offers,
3. :meth:`decide` which event type to subscribe to,
4. **record the signing secret in shared state BEFORE subscribing** so the
   in-flight verification callback can verify itself (see :meth:`start`),
5. ``events/subscribe`` (webhook delivery), then
6. run a background refresh loop that re-subscribes before the granted TTL
   (``refreshBefore``) lapses — this is what keeps the subscription alive while
   the agent is otherwise scaled to zero.

Ordering note (load-bearing): ``events/subscribe`` blocks while the server POSTs
a signed ``verification`` control envelope back to the agent's own
``/mcp-events/webhook`` endpoint and waits for the challenge echo. The receiver
verifies that POST's HMAC using the secret it finds in
:class:`~drasi_mcp_agent.state.AgentEventState`. If we subscribed *before*
publishing the secret via :meth:`AgentEventState.set_pending`, the verification
POST would arrive while the receiver still has no secret to verify it with —
``secret_for`` returns ``None`` → ``404`` → the server records a failed
challenge and ``events/subscribe`` fails with ``-32015 CallbackEndpointError``.
So :meth:`start` always calls ``set_pending`` first.

Protocol contract: ``docs/design-sketch-proposal.md`` (Webhook-Based Delivery,
Subscription TTL, Cursor Lifecycle) and ``docs/ARCHITECTURE.md`` (subscription.py).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx

from .mcp_events.client import McpProtocolError, McpRpcError
from .mcp_events.wire import gen_whsec, parse_whsec

if TYPE_CHECKING:
    from .config import Settings
    from .mcp_events.client import McpEventsClient
    from .state import AgentEventState

logger = logging.getLogger(__name__)

#: Webhook delivery mode token as it appears in an event's ``delivery[]`` array.
WEBHOOK_MODE = "webhook"

#: Refresh at ~this fraction of the remaining TTL, leaving ample margin for the
#: re-subscribe round-trip (and its own verification, if the server re-verifies).
REFRESH_FRACTION = 0.5

#: Never schedule a refresh sooner than this many seconds — guards against a
#: tight loop if the server grants a very short (or already-elapsed) TTL.
MIN_REFRESH_SECONDS = 3.0

#: Cadence for the occasional health-check re-subscribe when the server grants
#: *no expiry* (``refreshBefore: null``). Correctness no longer depends on it,
#: but re-subscribing advances the cursor during quiet periods and surfaces
#: ``deliveryStatus`` (sketch §Subscription TTL). Our reference server never
#: grants no-expiry, so this path is defensive — see SPEC-FINDINGS.
NO_EXPIRY_HEALTHCHECK_SECONDS = 300.0

#: Errors from a refresh round-trip that should be logged-and-retried rather
#: than killing the loop. ``McpRpcError`` is a well-formed JSON-RPC error,
#: ``McpProtocolError`` a framing fault, ``httpx.HTTPError`` a transport fault.
_REFRESH_ERRORS = (McpRpcError, McpProtocolError, httpx.HTTPError)


class NoWebhookEventError(RuntimeError):
    """No event type the server advertises can be delivered via webhook."""


def _parse_iso8601(value: str) -> datetime:
    """Parse an RFC 3339 / ISO 8601 timestamp into an aware UTC ``datetime``.

    The reference server renders ``refreshBefore`` as RFC 3339 with a ``Z``
    suffix and millisecond precision (e.g. ``2026-02-19T16:30:00.000Z``), but we
    also accept second precision and explicit numeric offsets. A value with no
    zone is assumed UTC.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def refresh_delay_seconds(
    refresh_before: str | None,
    now: datetime,
    *,
    fraction: float = REFRESH_FRACTION,
    floor_s: float = MIN_REFRESH_SECONDS,
    no_expiry_s: float = NO_EXPIRY_HEALTHCHECK_SECONDS,
) -> float:
    """Seconds to wait before the next re-subscribe.

    Pure function so the cadence is unit-testable without a clock. For a finite
    ``refreshBefore`` the delay is ``fraction`` of the remaining lifetime,
    floored at ``floor_s`` (so an already-expired or impractically short grant
    re-subscribes promptly but never spins). ``refresh_before is None`` means
    the server granted no expiry, so we fall back to a slow health-check cadence.
    """
    if refresh_before is None:
        return no_expiry_s
    remaining = (_parse_iso8601(refresh_before) - now).total_seconds()
    return max(remaining * fraction, floor_s)


class SubscriptionManager:
    """Drives discovery, the verified subscribe, and the TTL refresh loop."""

    def __init__(
        self,
        client: McpEventsClient,
        st: AgentEventState,
        settings: Settings,
    ) -> None:
        self._client = client
        self._st = st
        self._settings = settings
        # Filled in by start(); needed by the refresh loop and by stop()'s
        # best-effort unsubscribe (which keys on name + params + url).
        self._name: str | None = None
        self._params: dict[str, Any] | None = None
        self._secret: str | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Discover, subscribe (verified), and spawn the refresh loop.

        Steps, in the order the verification handshake requires:
        ``initialize`` → ``events/list`` → :meth:`decide` → publish the secret
        via ``set_pending`` → ``events/subscribe`` → ``set_active`` → refresh
        loop. See the module docstring for why ``set_pending`` precedes
        ``subscribe``.
        """
        capabilities = await self._client.initialize()
        if not isinstance(capabilities.get("events"), dict):
            # Not fatal: subscribe will surface -32014 if webhook is truly
            # unavailable. Worth a breadcrumb for the operator.
            logger.warning("server did not advertise an 'events' capability")

        events = await self._client.list_events()
        name, params = self.decide(events)
        self._name, self._params = name, params

        # One CSPRNG secret per subscription; keep the whsec_ string to re-send
        # on every refresh and the decoded bytes for the receiver to verify with.
        secret = gen_whsec()
        self._secret = secret

        # LOAD-BEARING ORDERING: publish the secret BEFORE subscribing so the
        # verification callback (which the server fires synchronously inside the
        # subscribe round-trip) can be HMAC-verified by the receiver.
        self._st.set_pending(name, params, parse_whsec(secret))

        result = await self._client.subscribe(
            name=name,
            params=params,
            callback_url=self._settings.callback_url,
            secret=secret,
            ttl_ms=self._settings.ttl_ms,
            cursor=None,
        )
        self._st.set_active(
            result["id"],
            result.get("refreshBefore"),
            result.get("cursor"),
        )
        logger.info(
            "subscribed to %r (id=%s, refreshBefore=%s)",
            name,
            result.get("id"),
            result.get("refreshBefore"),
        )

        self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        """Cancel the refresh loop and best-effort ``events/unsubscribe``."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self._name is not None:
            try:
                await self._client.unsubscribe(
                    name=self._name,
                    params=self._params,
                    callback_url=self._settings.callback_url,
                )
            except _REFRESH_ERRORS as exc:
                # Eager cleanup only; if it fails the subscription lapses at TTL.
                logger.warning("best-effort unsubscribe failed: %s", exc)

    def decide(self, events: list[dict[str, Any]]) -> tuple[str, dict[str, Any] | None]:
        """Choose which event type (and params) to subscribe to.

        Rule (ARCHITECTURE.md): prefer the configured ``settings.event_name`` if
        the server lists it; otherwise the first event whose ``delivery[]``
        advertises ``"webhook"``; otherwise raise :class:`NoWebhookEventError`.
        Returns ``(name, params)``; ``params`` is ``None`` for the demo (no
        filters) — see SPEC-FINDINGS on deriving params from ``inputSchema``.
        """
        configured = self._settings.event_name
        by_name = {
            e["name"]: e
            for e in events
            if isinstance(e, dict) and isinstance(e.get("name"), str)
        }
        if configured in by_name:
            logger.info(
                "the agent decided to subscribe to the configured event %r",
                configured,
            )
            return configured, None

        for event in events:
            if not isinstance(event, dict):
                continue
            name = event.get("name")
            delivery = event.get("delivery")
            if (
                isinstance(name, str)
                and isinstance(delivery, list)
                and WEBHOOK_MODE in delivery
            ):
                logger.info(
                    "configured event %r not offered; the agent decided to "
                    "subscribe to the first webhook-capable event %r",
                    configured,
                    name,
                )
                return name, None

        raise NoWebhookEventError(
            f"no webhook-capable event found (configured {configured!r} absent; "
            f"{len(events)} event(s) listed)"
        )

    async def _refresh_loop(self) -> None:
        """Re-subscribe before ``refreshBefore`` for as long as we are alive.

        Each iteration reads the current grant from shared state, sleeps until
        ~:data:`REFRESH_FRACTION` of the remaining TTL, then re-subscribes
        (passing the last-persisted cursor). A failed refresh is logged and
        retried on the next iteration rather than tearing the loop down.
        """
        while True:
            sub = self._st.current()
            refresh_before = sub.refresh_before if sub is not None else None
            delay = refresh_delay_seconds(refresh_before, _utcnow())
            logger.debug("next subscription refresh in %.1fs", delay)
            await asyncio.sleep(delay)
            try:
                await self._refresh_once()
            except _REFRESH_ERRORS as exc:
                logger.warning("subscription refresh failed: %s; will retry", exc)

    async def _refresh_once(self) -> None:
        """Re-call ``events/subscribe`` (the keep-alive) with the live cursor.

        Idempotent upsert on ``(principal, url, name, params)`` — it resets the
        TTL and advances the safe-to-persist cursor. We always send the
        last-persisted cursor (sketch §Cursor Lifecycle: "always pass the
        last-persisted cursor"); if delivery is live the server treats it as a
        no-op, and if the server restarted it becomes the replay point.
        """
        if self._name is None or self._secret is None:
            raise RuntimeError("_refresh_once called before start()")

        sub = self._st.current()
        cursor = sub.cursor if sub is not None else None
        result = await self._client.subscribe(
            name=self._name,
            params=self._params,
            callback_url=self._settings.callback_url,
            secret=self._secret,
            ttl_ms=self._settings.ttl_ms,
            cursor=cursor,
        )
        self._st.set_active(
            result["id"],
            result.get("refreshBefore"),
            result.get("cursor"),
        )

        status = result.get("deliveryStatus")
        if isinstance(status, dict) and status.get("active") is False:
            logger.warning(
                "delivery is suspended (lastError=%s, failedSince=%s); the "
                "refresh just reactivated it",
                status.get("lastError"),
                status.get("failedSince"),
            )
