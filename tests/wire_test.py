"""Tests for mcp_events.wire — Standard Webhooks profile + Events wire helpers.

The known-answer vector below is computed independently of the module under
test (raw stdlib HMAC in the comment) and pinned as a literal so that any drift
in the signing string, secret encoding, or base64 alphabet is caught. It must
match the reference Rust signer in
``../drasi-mcp-events/crates/mcp-events-wire/src/webhook.rs``.
"""

from __future__ import annotations

import base64

import pytest

from drasi_mcp_agent.mcp_events import wire

# --- Pinned known-answer vector ----------------------------------------------
# secret material = bytes(range(32)); body is a sketch-shaped occurrence.
KAT_SECRET = "whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
KAT_MSG_ID = "evt_789"
KAT_TS = 1739980800
KAT_BODY = (
    b'{"eventId":"evt_789","name":"incident.created",'
    b'"timestamp":"2026-02-19T16:00:00Z",'
    b'"data":{"incidentId":"INC-1234"},"cursor":"cursor_xyz"}'
)
KAT_SIGNATURE = "v1,T71DgGqlYSf2VLMcUz+I5N8N6ACcVUG52FqeE7rfMic="


def test_known_answer_sign_matches_pinned_vector() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    assert secret == bytes(range(32))
    assert wire.sign(secret, KAT_MSG_ID, KAT_TS, KAT_BODY) == KAT_SIGNATURE


def test_known_answer_verify_accepts_pinned_vector() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    assert wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, KAT_SIGNATURE)


def test_sign_and_verify_round_trip() -> None:
    secret = wire.parse_whsec(wire.gen_whsec())
    sig = wire.sign(secret, "msg_gap_abc", 1739980800, b'{"type":"gap"}')
    assert wire.verify(secret, "msg_gap_abc", 1739980800, b'{"type":"gap"}', sig)


def test_verify_rejects_tampered_body() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    tampered = KAT_BODY.replace(b"INC-1234", b"INC-9999")
    assert not wire.verify(secret, KAT_MSG_ID, KAT_TS, tampered, KAT_SIGNATURE)


def test_verify_rejects_wrong_secret() -> None:
    other = wire.parse_whsec(wire.gen_whsec())
    assert not wire.verify(other, KAT_MSG_ID, KAT_TS, KAT_BODY, KAT_SIGNATURE)


def test_verify_rejects_tampered_msg_id_or_ts() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    assert not wire.verify(secret, "evt_OTHER", KAT_TS, KAT_BODY, KAT_SIGNATURE)
    assert not wire.verify(secret, KAT_MSG_ID, KAT_TS + 1, KAT_BODY, KAT_SIGNATURE)


# --- whsec bounds ------------------------------------------------------------


def _whsec_of_len(n: int) -> str:
    return wire.WHSEC_PREFIX + base64.b64encode(bytes(n)).decode("ascii")


def test_parse_whsec_accepts_min_and_max_bytes() -> None:
    assert len(wire.parse_whsec(_whsec_of_len(24))) == 24
    assert len(wire.parse_whsec(_whsec_of_len(64))) == 64


def test_parse_whsec_rejects_below_min() -> None:
    with pytest.raises(ValueError):
        wire.parse_whsec(_whsec_of_len(23))


def test_parse_whsec_rejects_above_max() -> None:
    with pytest.raises(ValueError):
        wire.parse_whsec(_whsec_of_len(65))


def test_parse_whsec_rejects_missing_prefix() -> None:
    # Valid base64 of 32 bytes but without the whsec_ prefix.
    no_prefix = base64.b64encode(bytes(32)).decode("ascii")
    with pytest.raises(ValueError):
        wire.parse_whsec(no_prefix)


def test_parse_whsec_rejects_non_base64() -> None:
    with pytest.raises(ValueError):
        wire.parse_whsec("whsec_!!!!not base64!!!!")


def test_gen_whsec_round_trips_through_parse() -> None:
    s = wire.gen_whsec()
    assert s.startswith("whsec_")
    assert len(wire.parse_whsec(s)) == 32


def test_gen_whsec_rejects_out_of_bounds_n() -> None:
    with pytest.raises(ValueError):
        wire.gen_whsec(23)
    with pytest.raises(ValueError):
        wire.gen_whsec(65)
    assert len(wire.parse_whsec(wire.gen_whsec(24))) == 24
    assert len(wire.parse_whsec(wire.gen_whsec(64))) == 64


# --- multi-signature header --------------------------------------------------


def test_verify_accepts_when_any_signature_matches() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    header = f"v1,AAAA {KAT_SIGNATURE} v1,BBBB"
    assert wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, header)


def test_verify_rejects_when_no_signature_matches() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    header = "v1,AAAA v1,BBBBCCCC"
    assert not wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, header)


def test_verify_ignores_non_v1_and_undecodable_entries() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    # v1a, asymmetric entry + a non-base64 v1, entry are ignored; real one wins.
    header = f"v1a,ZZZZ v1,@@@notbase64@@@ {KAT_SIGNATURE}"
    assert wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, header)


def test_verify_rejects_empty_header() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    assert not wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, "")


def test_verify_handles_extra_whitespace_between_signatures() -> None:
    secret = wire.parse_whsec(KAT_SECRET)
    header = f"   v1,AAAA    {KAT_SIGNATURE}   "
    assert wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, header)


# --- constant-time usage -----------------------------------------------------


def test_verify_uses_constant_time_compare(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[bytes, bytes]] = []
    real = wire.hmac.compare_digest

    def spy(a: bytes, b: bytes) -> bool:
        calls.append((a, b))
        return real(a, b)

    monkeypatch.setattr(wire.hmac, "compare_digest", spy)
    secret = wire.parse_whsec(KAT_SECRET)
    # Two decodable v1, candidates -> both must be compared (no early return).
    header = f"v1,AAAA {KAT_SIGNATURE}"
    assert wire.verify(secret, KAT_MSG_ID, KAT_TS, KAT_BODY, header)
    assert len(calls) == 2


# --- timestamp freshness -----------------------------------------------------


def test_timestamp_fresh_within_window() -> None:
    assert wire.timestamp_fresh(1000, 1000)
    assert wire.timestamp_fresh(1000, 1000 + 300)
    assert wire.timestamp_fresh(1000, 1000 - 300)


def test_timestamp_fresh_rejects_outside_window_both_directions() -> None:
    # One-sided in the sketch (past only); we enforce symmetric ±skew.
    assert not wire.timestamp_fresh(1000, 1000 + 301)
    assert not wire.timestamp_fresh(1000, 1000 - 301)


# --- control_type discrimination ---------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ({"type": "verification", "challenge": "n0nce"}, "verification"),
        ({"type": "gap", "cursor": "c1"}, "gap"),
        ({"type": "terminated", "error": {"code": -32004}}, "terminated"),
        ({"eventId": "evt_1", "name": "x", "timestamp": "t", "data": {}}, None),
        ({"type": 123}, None),  # non-string type is not a control discriminator
    ],
)
def test_control_type_discrimination(body: dict, expected: str | None) -> None:
    assert wire.control_type(body) == expected


# --- parse_body --------------------------------------------------------------


def test_parse_body_parses_object() -> None:
    assert wire.parse_body(b'{"a": 1}') == {"a": 1}


def test_parse_body_rejects_non_object() -> None:
    with pytest.raises(ValueError):
        wire.parse_body(b"[1, 2, 3]")


def test_parse_body_rejects_invalid_json() -> None:
    with pytest.raises(ValueError):
        wire.parse_body(b"not json")


# --- parse_occurrence --------------------------------------------------------


def test_parse_occurrence_on_sketch_example() -> None:
    body = wire.parse_body(KAT_BODY)
    occ = wire.parse_occurrence(body)
    assert occ.event_id == "evt_789"
    assert occ.name == "incident.created"
    assert occ.timestamp == "2026-02-19T16:00:00Z"
    assert occ.data == {"incidentId": "INC-1234"}
    assert occ.cursor == "cursor_xyz"


def test_parse_occurrence_cursor_absent_and_null_both_none() -> None:
    base_body = {
        "eventId": "evt_1",
        "name": "x",
        "timestamp": "2026-01-01T00:00:00Z",
        "data": {},
    }
    assert wire.parse_occurrence(dict(base_body)).cursor is None
    assert wire.parse_occurrence({**base_body, "cursor": None}).cursor is None


def test_parse_occurrence_missing_required_field_raises() -> None:
    with pytest.raises(ValueError):
        wire.parse_occurrence({"name": "x", "timestamp": "t", "data": {}})
