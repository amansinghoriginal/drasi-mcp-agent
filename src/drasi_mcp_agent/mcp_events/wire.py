"""Standard Webhooks profile + MCP Events wire helpers (webhook-consumer side).

This module mirrors the byte-for-byte signing behaviour of the reference Rust
server in ``../drasi-mcp-events/crates/mcp-events-wire/src/webhook.rs`` so that
signatures produced by that server verify here and vice-versa.

Signing string (UTF-8 prefix + raw body bytes)::

    HMAC-SHA256(secret, f"{webhook_id}.{webhook_timestamp}." .encode() + body)

- ``secret`` is the base64-decoded bytes of the value after the ``whsec_`` prefix
  (standard base64 alphabet, padded; decoded length 24..=64 bytes).
- The ``webhook-signature`` header value for one signature is
  ``"v1," + base64(tag)`` (standard base64, padded).
- The header MAY carry multiple space-delimited signatures during secret
  rotation; verification accepts if any ``v1,`` entry matches. Non-``v1,``
  entries (e.g. asymmetric ``v1a,``) and undecodable entries are ignored.

See ``docs/design-sketch-proposal.md`` §Webhook Security and §Non-event webhook
bodies for the protocol contract.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass
from typing import Any

# --- Header name constants (lowercase; HTTP header names are case-insensitive
# and ASGI/Starlette normalises to lowercase). --------------------------------
WEBHOOK_ID = "webhook-id"
WEBHOOK_TIMESTAMP = "webhook-timestamp"
WEBHOOK_SIGNATURE = "webhook-signature"
MCP_SUBSCRIPTION_ID = "x-mcp-subscription-id"

# --- Standard Webhooks secret format -----------------------------------------
WHSEC_PREFIX = "whsec_"
WHSEC_MIN_BYTES = 24
WHSEC_MAX_BYTES = 64

# --- Signature scheme version prefix for symmetric HMAC signatures. ----------
SIGNATURE_VERSION_PREFIX = "v1,"

# Default freshness window for ``webhook-timestamp`` (seconds).
DEFAULT_SKEW_SECONDS = 300


def gen_whsec(n_bytes: int = 32) -> str:
    """Generate a Standard Webhooks symmetric secret.

    Returns ``"whsec_"`` + standard (padded) base64 of ``n_bytes`` CSPRNG bytes.
    ``n_bytes`` must be within the Standard Webhooks 24..=64 byte bounds so the
    secret round-trips through :func:`parse_whsec` (and the reference server).
    """
    if not WHSEC_MIN_BYTES <= n_bytes <= WHSEC_MAX_BYTES:
        raise ValueError(
            f"n_bytes must be {WHSEC_MIN_BYTES}..={WHSEC_MAX_BYTES}, got {n_bytes}"
        )
    raw = secrets.token_bytes(n_bytes)
    return WHSEC_PREFIX + base64.b64encode(raw).decode("ascii")


def parse_whsec(secret: str) -> bytes:
    """Decode a ``whsec_`` secret to its raw HMAC key bytes.

    Raises :class:`ValueError` if the value is missing the ``whsec_`` prefix, is
    not valid standard (padded) base64, or decodes to a length outside
    24..=64 bytes. Mirrors the server's ``parse_whsec``.
    """
    if not secret.startswith(WHSEC_PREFIX):
        raise ValueError('secret must start with "whsec_"')
    b64 = secret[len(WHSEC_PREFIX) :]
    try:
        # validate=True rejects characters outside the standard alphabet;
        # b64decode also rejects incorrect padding length.
        raw = base64.b64decode(b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"secret is not valid base64: {exc}") from exc
    if not WHSEC_MIN_BYTES <= len(raw) <= WHSEC_MAX_BYTES:
        raise ValueError(
            f"decoded secret must be {WHSEC_MIN_BYTES}..={WHSEC_MAX_BYTES} bytes, "
            f"got {len(raw)}"
        )
    return raw


def _hmac_sha256(secret: bytes, msg_id: str, ts: int, body: bytes) -> bytes:
    """Compute the raw HMAC-SHA256 tag over ``"<msg_id>.<ts>." + body``."""
    prefix = f"{msg_id}.{ts}.".encode("utf-8")
    return hmac.new(secret, prefix + body, hashlib.sha256).digest()


def sign(secret: bytes, msg_id: str, ts: int, body: bytes) -> str:
    """Compute a single ``webhook-signature`` value.

    Returns ``"v1," + base64(HMAC-SHA256(secret, f"{msg_id}.{ts}." + body))``.
    ``body`` is the raw HTTP body bytes exactly as sent.
    """
    tag = _hmac_sha256(secret, msg_id, ts, body)
    return SIGNATURE_VERSION_PREFIX + base64.b64encode(tag).decode("ascii")


def verify(
    secret: bytes,
    msg_id: str,
    ts: int,
    body: bytes,
    signature_header: str,
) -> bool:
    """Verify a (possibly multi-signature) ``webhook-signature`` header.

    The header may carry multiple whitespace-delimited signatures (secret
    rotation); returns True if any ``v1,`` entry matches. Non-``v1,`` entries
    (e.g. asymmetric ``v1a,``) and undecodable entries are ignored. Each
    candidate is compared in constant time via :func:`hmac.compare_digest`, and
    every candidate is checked (no early return) to keep timing uniform.
    """
    expected = _hmac_sha256(secret, msg_id, ts, body)
    ok = False
    for token in signature_header.split():
        if not token.startswith(SIGNATURE_VERSION_PREFIX):
            continue
        b64 = token[len(SIGNATURE_VERSION_PREFIX) :]
        try:
            candidate = base64.b64decode(b64, validate=True)
        except (binascii.Error, ValueError):
            continue
        # OR-accumulate; never short-circuit, so timing is candidate-uniform.
        ok |= hmac.compare_digest(candidate, expected)
    return ok


def timestamp_fresh(ts: int, now: int, skew_s: int = DEFAULT_SKEW_SECONDS) -> bool:
    """Return True iff ``|now - ts| <= skew_s`` (symmetric freshness window)."""
    return abs(now - ts) <= skew_s


@dataclass
class EventOccurrence:
    """A parsed ``EventOccurrence`` webhook body (a body with no top-level
    ``type`` field)."""

    event_id: str
    name: str
    timestamp: str
    data: dict[str, Any]
    cursor: str | None


def parse_body(raw: bytes) -> dict[str, Any]:
    """Parse a raw webhook body into a JSON object.

    Raises :class:`ValueError` if the body is not valid JSON or is not a JSON
    object (control envelopes and occurrences are always objects).
    """
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"webhook body is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("webhook body must be a JSON object")
    return obj


def control_type(body: dict[str, Any]) -> str | None:
    """Return the control-envelope discriminator.

    A body with a top-level string ``type`` is a control envelope
    (``"verification"`` | ``"gap"`` | ``"terminated"``); a body without one is
    an :class:`EventOccurrence` event delivery (returns ``None``).
    """
    t = body.get("type")
    return t if isinstance(t, str) else None


def parse_occurrence(body: dict[str, Any]) -> EventOccurrence:
    """Parse an event-delivery body into an :class:`EventOccurrence`.

    Required fields (``eventId``, ``name``, ``timestamp``, ``data``) are
    camelCase on the wire. ``cursor`` is optional: both an absent field and an
    explicit ``null`` collapse to ``None`` (no replay watermark to persist).
    """
    try:
        event_id = body["eventId"]
        name = body["name"]
        timestamp = body["timestamp"]
        data = body["data"]
    except KeyError as exc:
        raise ValueError(f"occurrence missing required field: {exc}") from exc
    cursor = body.get("cursor")
    return EventOccurrence(
        event_id=event_id,
        name=name,
        timestamp=timestamp,
        data=data,
        cursor=cursor,
    )
