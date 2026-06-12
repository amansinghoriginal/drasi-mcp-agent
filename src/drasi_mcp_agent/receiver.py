"""FastAPI webhook receiver for MCP Events *webhook* delivery (consumer side).

This is the inbound half of the agent: the always-up FastAPI endpoint that the
reference MCP Events server (``../drasi-mcp-events``) POSTs to. It implements the
receiver obligations from ``docs/design-sketch-proposal.md`` (§Webhook Event
Delivery, §Webhook Security → Signature scheme, §Non-event webhook bodies) and
matches the exact wire behaviour of that server's
``crates/mcp-events-server/src/webhook/{handlers,challenge,signer,worker}.rs``.

Request handling, in order:

1. Read the raw body bytes and headers; resolve the HMAC secret from
   :class:`~drasi_mcp_agent.state.AgentEventState` by ``X-MCP-Subscription-Id``
   (falling back to the single held secret during the verification handshake,
   before ``events/subscribe`` has returned the routing id). No secret at all ⇒
   ``404`` (per ``docs/ARCHITECTURE.md``).
2. Verify the Standard Webhooks signature over the raw bytes **and** the
   ``webhook-timestamp`` freshness window; either failing ⇒ ``401`` and the body
   is never processed.
3. Branch on the control-envelope ``type`` discriminator:
   - ``verification`` ⇒ echo ``{"challenge": <nonce>}`` in a ``2xx`` body. This
     is exactly what the server's ``challenge::verify_endpoint`` compares (in
     constant time) to activate delivery — i.e. it unblocks the in-flight
     ``events/subscribe`` call.
   - ``gap`` ⇒ persist the fresh ``cursor`` watermark and ack ``200 {}``.
   - ``terminated`` ⇒ ack ``200 {}`` and signal the subscription manager to
     re-subscribe (the subscription no longer exists server-side).
   - event (no ``type``) ⇒ dedup by ``eventId``; on a new id persist the cursor,
     schedule the agent workflow (non-blocking), and ack fast.

The handler never blocks on agent work: scheduling is
``runner.run(..., wait=False)`` behind :attr:`AgentEventState.schedule`.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from .mcp_events.wire import (
    MCP_SUBSCRIPTION_ID,
    WEBHOOK_ID,
    WEBHOOK_SIGNATURE,
    WEBHOOK_TIMESTAMP,
    control_type,
    parse_body,
    parse_occurrence,
    timestamp_fresh,
    verify,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from starlette.datastructures import Headers

    from .state import AgentEventState

logger = logging.getLogger(__name__)

# Control-envelope discriminators (sketch §Non-event webhook bodies).
CONTROL_VERIFICATION = "verification"
CONTROL_GAP = "gap"
CONTROL_TERMINATED = "terminated"

#: Optional attribute the SubscriptionManager MAY set on the shared state to be
#: re-subscribed after a ``terminated`` envelope. Mirrors how the activation
#: hook sets ``state.schedule``; absent ⇒ the receiver only logs the
#: termination. Looked up via ``getattr`` so the receiver stays decoupled from
#: the (foundation-owned) state schema.
RESUBSCRIBE_ATTR = "request_resubscribe"


def make_webhook_route(
    st: AgentEventState,
) -> Callable[[Request], Awaitable[Response]]:
    """Bind ``st`` and return the POST handler for ``/mcp-events/webhook``.

    The activation hook mounts the returned coroutine via
    ``ctx.app.add_api_route("/mcp-events/webhook", route, methods=["POST"])``.
    A raw ``Request`` handler is required (not a parsed-model route) because the
    signature is computed over the **raw** body bytes — a re-serialized JSON
    object would not verify.
    """

    async def route(request: Request) -> Response:
        return await handle_webhook(request, st)

    return route


async def handle_webhook(request: Request, st: AgentEventState) -> Response:
    """Verify and dispatch one inbound webhook delivery. See module docstring."""
    raw = await request.body()
    headers = request.headers  # case-insensitive (ASGI lowercases names)

    sub_id = headers.get(MCP_SUBSCRIPTION_ID)
    secret = st.secret_for(sub_id)
    if secret is None:
        # No subscription is being tracked at all (cold endpoint / pod restart
        # before the manager re-subscribed). Per ARCHITECTURE.md this is 404;
        # our server treats any non-413 as retryable, so a redelivery once the
        # subscription exists still lands. See SPEC-FINDINGS.
        logger.warning(
            "webhook delivery with no routable subscription (sub_id=%s)", sub_id
        )
        return JSONResponse(
            {"error": "no subscription for this delivery"}, status_code=404
        )

    if not _verify_signature(secret, raw, headers):
        # Signature mismatch, stale/missing/garbled timestamp, or missing
        # signature headers: do NOT process (sketch §Signature scheme).
        return JSONResponse({"error": "signature verification failed"}, status_code=401)

    try:
        body = parse_body(raw)
    except ValueError as exc:
        # Authentic (signed) but not a JSON object — a server-side defect.
        logger.warning("verified webhook body is not a JSON object: %s", exc)
        return JSONResponse({"error": "malformed body"}, status_code=400)

    ct = control_type(body)
    if ct == CONTROL_VERIFICATION:
        return _handle_verification(body)
    if ct == CONTROL_GAP:
        return _handle_gap(st, body)
    if ct == CONTROL_TERMINATED:
        return await _handle_terminated(st, body)
    if ct is not None:
        # Unknown future control type: ack so the server does not retry, but do
        # not act on a discriminator we do not understand (forward-compat).
        logger.info("ignoring unknown webhook control envelope type=%r", ct)
        return JSONResponse({"ignored": ct})

    return await _handle_event(st, body)


def _verify_signature(
    secret: bytes,
    raw: bytes,
    headers: Headers,
) -> bool:
    """Return ``True`` iff the Standard Webhooks signature and freshness hold.

    Both the HMAC signature (over the raw body) and the ``webhook-timestamp``
    freshness window must pass. Missing headers or a non-integer timestamp fail
    closed. Both checks run (no early return on the signature) so the combined
    accept/reject path stays uniform regardless of which condition fails.
    """
    msg_id = headers.get(WEBHOOK_ID)
    ts_raw = headers.get(WEBHOOK_TIMESTAMP)
    signature = headers.get(WEBHOOK_SIGNATURE)
    if msg_id is None or ts_raw is None or signature is None:
        return False
    try:
        ts = int(ts_raw)
    except ValueError:
        return False

    sig_ok = verify(secret, msg_id, ts, raw, signature)
    fresh = timestamp_fresh(ts, int(time.time()))
    return sig_ok and fresh


def _handle_verification(body: dict) -> Response:
    """Echo the challenge nonce to activate delivery (sketch §Endpoint
    verification).

    The server's ``challenge::verify_endpoint`` reads our 2xx body, extracts the
    ``challenge`` string, and compares it in constant time to the nonce it sent.
    We echo ``body["challenge"]`` verbatim; any other field is ignored by the
    server.
    """
    challenge = body.get("challenge")
    logger.info("webhook endpoint verification: echoing challenge")
    return JSONResponse({"challenge": challenge})


def _handle_gap(st: AgentEventState, body: dict) -> Response:
    """Persist the post-gap watermark and ack (sketch §Gaps and ``truncated``).

    A ``gap`` envelope carries a fresh ``cursor`` and is to be treated as
    ``truncated: true``: events were skipped. The consumer persists the new
    cursor so a later resubscribe resumes from the server's reset position
    rather than a stale one.
    """
    cursor = body.get("cursor")
    st.set_cursor(cursor)
    logger.warning("webhook gap: events skipped; advanced cursor to %r", cursor)
    return JSONResponse({})


async def _handle_terminated(st: AgentEventState, body: dict) -> Response:
    """Ack and request a fresh subscription (sketch §Authorization, §Non-event
    webhook bodies).

    The subscription no longer exists server-side, so a refresh would be a fresh
    subscribe (and returns ``-32012 Forbidden`` if the termination cause still
    applies). We signal the subscription manager via the optional
    :data:`RESUBSCRIBE_ATTR` callback if it is wired up.
    """
    error = body.get("error")
    logger.warning("webhook subscription terminated by server: %s", error)
    resubscribe = getattr(st, RESUBSCRIBE_ATTR, None)
    if resubscribe is not None:
        await resubscribe()
    return JSONResponse({})


async def _handle_event(st: AgentEventState, body: dict) -> Response:
    """Dedup, persist the cursor, and schedule the agent workflow (fast ack)."""
    try:
        occ = parse_occurrence(body)
    except ValueError as exc:
        logger.warning("verified event body missing required fields: %s", exc)
        return JSONResponse({"error": "malformed event"}, status_code=400)

    if st.is_seen(occ.event_id):
        # Idempotent re-delivery (retry / dual-path). Ack without rescheduling.
        logger.info("duplicate event %s ignored", occ.event_id)
        return JSONResponse({"dedup": True})

    if st.schedule is None:
        # Verified and routable, but the agent runtime is not yet wired to
        # accept work. Ask the server to retry rather than drop the event.
        logger.warning("event %s verified but no schedule sink; asking retry", occ.event_id)
        return JSONResponse({"error": "agent not ready"}, status_code=503)

    # Schedule FIRST, commit dedup + cursor only on success. If the durable
    # schedule fails, we leave the event un-seen and the cursor unadvanced and
    # return a retryable status, so the server redelivers rather than the event
    # being silently dedup-dropped (at-least-once on our side).
    try:
        instance_id = await st.schedule(occ)
    except Exception:  # noqa: BLE001 - convert any sink failure into a retry
        logger.exception("failed to schedule workflow for event %s; asking retry", occ.event_id)
        return JSONResponse({"error": "schedule failed"}, status_code=503)

    st.mark_seen(occ.event_id)
    st.set_cursor(occ.cursor)
    logger.info("scheduled agent workflow %s for event %s", instance_id, occ.event_id)
    return JSONResponse({"scheduled": True})
