# MCP Events Design Sketch — findings from a serverless-agent (webhook-consumer) prototype

Gaps and ambiguities in the draft MCP **Events** extension, found while building an event-driven
**Dapr agent** that consumes the **webhook** delivery mode, scales to zero (Dapr actor
deactivation), and is woken by the inbound webhook. This is the **consumer / subscriber /
serverless** vantage — complementary to, and largely non-overlapping with, the *server-side*
findings in [`../../drasi-mcp-events/SPEC-GAPS.md`](../../drasi-mcp-events/SPEC-GAPS.md).

Source: a clean-room build of this agent from `docs/design-sketch-proposal.md` and the behavior of
our own reference server, **verified end-to-end against a live stack** (Postgres → Drasi → MCP
server → signed webhook → woken agent; ADD/UPDATE/threshold-cross-DELETE all confirmed).

**Severity:** **high** = a scale-to-zero / independent consumer cannot follow the spec as written,
or two consumers would build incompatible behavior; **medium** = divergent-but-recoverable;
**low** = editorial.

---

## High — the serverless-consumer model is not covered by the spec

The headline. The sketch implicitly assumes a **single, always-on SDK** that both refreshes the
subscription and consumes events. A scale-to-zero agent splits those responsibilities across an
always-up receiver tier and a dormant compute tier, and the spec has no model for it. Each of the
six below was hit while making the demo actually work.

### H1. Verification handshake needs a live re-entrant endpoint while subscribe blocks; a cold self-hosting endpoint cannot satisfy it

**§Webhook verification** · *Underspecified*

subscribe blocks on the consumer's own challenge echo, so a cold endpoint never activates.

**What this prototype did:** deferred subscribe to a post-bind background task

### H2. ttlMs null no-expiry is the only escape from refreshing, but servers need not grant it and give no pre-subscribe signal

**§Subscription TTL null** · *Underspecified*

a server may silently downgrade null to a finite TTL, so a never-refresh consumer silently loses its sub.

**What this prototype did:** review finding

### H3. No-expiry failure-GC reclaims a legitimately-cold endpoint and the terminated notice is undeliverable to a scaled-to-zero sink

**§Subscription TTL no-expiry GC** · *Underspecified*

a cold sink looks dead so GC drops it, and the notice goes to the same dead endpoint.

**What this prototype did:** review finding

### H4. Spec models one always-on SDK that refreshes and consumes; no model for split receiver and dormant-compute tiers

**§Subscription TTL refresh owner** · *Underspecified*

never says which tier is the client or who refreshes when compute is dormant.

**What this prototype did:** always-up receiver refreshes

### H5. eventId dedup window unspecified, not bounded by the freshness window, and lost on deactivation

**§Webhook dedup window** · *Underspecified*

duplicates arrive far apart and an auto-acting agent double-acts.

**What this prototype did:** bounded in-memory LRU, no time bound

### H6. No defined response for a verified-but-not-ready scale-to-zero sink; retryable vs permanent is contradictory

**§Webhook delivery race** · *Contradiction*

a reactivating sink 2xx drops the event; a missing secret reads as a permanent not-found.

**What this prototype did:** returned a retryable status when not ready

---

## Medium — consumer-side behavior the sketch leaves open

Deduplicated from findings across the receiver, subscription manager, and client. Several overlap
with the server-side `SPEC-GAPS.md`; recorded here from the consumer's side, where the consequence
differs.

### M1. Verification echo response shape is unspecified
The sketch says the endpoint "echoes `challenge` in a `2xx` body" but never pins the HTTP status
(200 vs 204), the `Content-Type`, whether extra body fields are tolerated, or whether the endpoint
must verify the delivery's HMAC *before* echoing. Two consumers can implement mutually-rejecting
handshakes. *This prototype:* `200`, `application/json`, body `{"challenge": <nonce>}`, signature
verified before echo.

### M2. A refresh re-subscribe can itself trigger a fresh verification callback
Verification is cached per `(principal, url)`, but the spec does not guarantee a refresh skips the
challenge. If a server re-challenges on refresh, the refreshing tier must again service an inbound
callback while its own `events/subscribe` call blocks — the same re-entrancy the initial subscribe
needs (see H1), now on every refresh. *Assumed:* the receiver tier is always able to answer.

### M3. Control envelopes carry no subscription identity in the body
`gap` / `terminated` / `verification` bodies have no `subscriptionId` field; the consumer must rely
on the `X-MCP-Subscription-Id` header (Standard-Webhooks headers are about the *message*, not the
subscription). A consumer that logs/persists only bodies cannot attribute a control event.

### M4. Control-envelope dedup vs event dedup is conflated
`webhook-id` is the dedup key, but it is the `eventId` for events and `msg_<type>_<random>` for
control envelopes. The sketch never says a consumer should keep separate dedup spaces; mixing them
in one LRU is what an implementer naturally does and is probably fine, but it is unstated.

### M5. `ttlMs` is tri-state but has no idiomatic config encoding
Omitted (server default) / finite int (suggestion) / explicit `null` (no expiry) are three distinct
request semantics. A plain integer config field — the obvious choice — can only ever express the
finite case, silently foreclosing the no-expiry escape hatch that scale-to-zero most needs (see H2).

### M6. `deliveryStatus` is absent on the first subscribe
It appears "when refreshing an existing subscription," so a consumer gets no delivery health signal
until the *second* call. A short-lived agent that subscribes once and sleeps never sees it.

### M7. Unary response framing (JSON vs SSE) is unspecified for consumers
A server may answer a unary `events/*` POST with `application/json` or a `text/event-stream` body
carrying the response frame. A consumer that only accepts one framing cannot talk to a server using
the other. *This prototype:* accepts both. (Confirmed cross-implementation in the server repo's interop.)

### M8. Cursor persistence across sleep has no home
The sketch says "the client persists the cursor," but for a scale-to-zero agent the receiver tier is
in-memory (lost on cold restart) and the durable store belongs to the compute tier. *Where* a
consumer should persist the cursor, and whether losing it is acceptable, is undefined.

### M9. No re-subscribe policy after `terminated`
On `terminated` the subscription is gone server-side. Whether the consumer should auto re-subscribe,
back off, or stop is left entirely open — and for an unattended agent this is a behavioral fork with
real cost (re-subscribe storms vs. silent deafness).

### M10. Webhook change-kind has no protocol discriminator
For a result-set change, *added/updated/deleted* lives only inside the server-specific `data` (our
server puts a `changeType` field there). There is no protocol-level field distinguishing the kind of
change, so every consumer must learn each server's payload convention.

---

## Low — editorial

- **L1. `https` MUST vs a loopback scale-to-zero agent.** The TLS-required rule is right for
  production but blocks the simplest local topology (a loopback `http://` callback) without the
  long-lived TLS-terminating proxy that webhook mode exists to avoid. A documented local-dev carve-out
  would help. *(This prototype uses an `allowInsecureUrls` server flag — explicitly nonconformant.)*
- **L2. Timestamp freshness is one-sided.** "Reject if more than 5 minutes old" addresses only past
  timestamps; a far-future timestamp is unaddressed. *Assumed:* symmetric ±300s window.
- **L3. Secret / signature base64 alphabet and padding unpinned;** multi-signature delimiter and
  unknown-entry handling underspecified; header-name casing normalization unstated. (All shared with
  the server-side findings; they bite a consumer at *verify* time rather than *sign* time.)
- **L4. `gap` cursor nullability:** the example shows a string cursor but replay-less event types use
  `null`; the type vs example disagree for a consumer deciding whether to persist it.
- **L5. No guidance on deriving subscription `params` from an event's `inputSchema`** — an agent that
  "discovers and decides" has no documented contract for constructing valid params from the schema.

---

*Generated from a multi-agent clean-room build + an adversarial spec-gap critic, then verified by a
live end-to-end run. Raw per-module findings are preserved in the build transcript.*
