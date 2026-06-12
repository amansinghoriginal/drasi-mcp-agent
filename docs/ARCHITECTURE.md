# Architecture & Build Contract — drasi-mcp-agent

This is the **binding contract** for every module. Build to these exact signatures so independently
built pieces integrate. Protocol behavior is governed by `docs/design-sketch-proposal.md` (the draft
MCP Events extension) and the running reference server `../drasi-mcp-events`.

## What this is

An **event-driven Dapr agent** that subscribes to a Drasi continuous-query's result changes via the
draft MCP Events **webhook** delivery mode, sits idle (Dapr actors deactivate → "scale to zero"),
and is **woken by an inbound signed webhook** when the query result changes.

```
Postgres ─WAL→ Drasi ─query→ SSE reaction ─→ drasi-mcp-events (Rust MCP server, our other repo)
                                                  │  holds the webhook subscription, signs + POSTs
                                                  ▼
                              ┌──────────── this repo: Dapr Agent pod (always up) ───────────┐
                              │ FastAPI :8001                                                 │
   events/subscribe ◀────────┤  POST /mcp-events/webhook  → verify HMAC, handle control       │
   (agent → MCP server)      │     envelopes (verification/gap/terminated), dedup, schedule    │
   refresh loop              │     the DurableAgent workflow, ack 2xx fast                     │
                              │  SubscriptionManager: events/list → decide → subscribe →       │
                              │     refresh before TTL                                         │
                              │  DurableAgent: processes the change (echo LLM, or Claude)      │
                              │  idle → Dapr actors deactivate (state in Redis)                │
                              └────────────────────────────────────────────────────────────────┘
```

## Ground rules

1. **Clean-room on the spec:** protocol sources are ONLY `docs/design-sketch-proposal.md` and the
   behavior of our own server `../drasi-mcp-events` (read its Rust webhook code under
   `crates/mcp-events-server/src/webhook/` for exact wire behavior — that is the server this agent
   talks to). Do NOT consult other Events implementations.
2. **Capture spec findings:** whenever the design sketch is ambiguous, silent, or contradictory
   *as seen from the webhook-consumer / serverless-agent side*, record it as a `specGap` in your
   structured output (section, what was unclear, what you assumed, kind). This is a required
   deliverable (synthesized into `docs/SPEC-FINDINGS.md`). The consumer/agent perspective is novel
   vs. our prior server-side findings — be generous but real.
3. **Python 3.11–3.13**, `uv`. dapr-agents is a local editable dep (`../dapr-agents`). Do NOT edit
   files outside this repo. Do NOT edit `pyproject.toml`, `resources/*.yaml`, or files owned by
   another component (ownership noted per section). Type-hint everything; `ruff`-clean; no bare excepts.
4. Run your tests with `uv run pytest tests/<yours>` before declaring done. Do not commit/push.
5. Your final message is consumed by an orchestrator — return ONLY the structured output.

## Confirmed environment facts (already de-risked — rely on these)

- Dapr runtime **1.18.0** self-hosted (`dapr init` done): `dapr_redis` (localhost:6379), `dapr_placement`,
  `dapr_scheduler`, `dapr_zipkin` containers up.
- A `DurableAgent` served via `AgentRunner().serve(agent, host, port)` works; `POST /agent/run`
  schedules its workflow.
- **LLM is optional via `conversation.echo`:** `llm=DaprChatClient(component_name="echo-llm")` makes the
  agent loop complete with NO API key (component `resources/echo-llm.yaml`, `type: conversation.echo`).
  If `ANTHROPIC_API_KEY` is set, use `AnthropicChatClient()` (default model `claude-sonnet-4-6`).
- Activation hook fires inside `serve()` *before* uvicorn starts; `ActivationContext` gives
  `.app` (FastAPI), `.runner`, `.agent`, `.dapr_client`, `.wf_client`. Custom routes added via
  `ctx.app.add_api_route(...)` are live. `@http_router` only yields a parsed model (no raw body), so
  the webhook route MUST be a raw `async def handler(request: Request)` added via `ctx.app`.
- `await ctx.runner.run(agent, payload={"task": <str>}, wait=False)` schedules the workflow
  non-blocking and returns the instance id.

## Demo topology & fixed values

| Thing | Value |
|---|---|
| Drasi API / SSE reaction | `http://localhost:8080` / `http://localhost:8081/events` (stack already running in `../drasi-mcp-events/drasi`) |
| Continuous query / event | `high-value-orders` → MCP event type `high-value-orders.changed` |
| MCP server (Rust) endpoint | `http://127.0.0.1:8090/mcp` (binary built from `../drasi-mcp-events`; config `deploy/mcp-server.yaml`) |
| MCP bearer token → principal | `devtoken` → `agent@demo` (set in `deploy/mcp-server.yaml` authTokens) |
| Agent FastAPI (app-port) | `127.0.0.1:8001` |
| Agent webhook callback URL | `http://127.0.0.1:8001/mcp-events/webhook` |
| Dapr sidecar HTTP port | `3540` |

The MCP server must run with **`allowInsecureUrls: true`** (loopback `http://` callback) and
**webhook enabled** — `deploy/mcp-server.yaml` sets this. SSRF guard otherwise rejects 127.0.0.1.

---

## Module layout & ownership

```
src/drasi_mcp_agent/
  config.py            [foundation]  Settings from env (dataclass)
  mcp_events/
    wire.py            [foundation]  Standard Webhooks verify, whsec gen, control-envelope + occurrence parsing
    client.py          [foundation]  async MCP Events client (initialize/list/subscribe/unsubscribe)
  state.py             [foundation]  AgentEventState (shared between receiver + subscription mgr)
  receiver.py          [receiver]    FastAPI webhook handler
  subscription.py      [receiver]    SubscriptionManager (list→decide→subscribe→refresh)
  agent.py             [agent]       build_agent(); change-processing tool + summary
  activation.py        [agent]       activation hook: mount route + start subscription mgr on FastAPI startup
  app.py               [agent]       main()
deploy/                [deploy]      mcp-server.yaml, run-demo.sh, trigger-change.sh, show-timeline.sh
tests/                 each owner writes tests/<area>_test.py
```

Dependency order: `config`/`wire`/`client`/`state` (foundation) → `receiver`/`subscription` → `agent`/`activation`/`app` → `deploy`.

---

## config.py  [foundation]

```python
from dataclasses import dataclass
@dataclass(frozen=True)
class Settings:
    mcp_url: str          # env MCP_URL default http://127.0.0.1:8090/mcp
    mcp_bearer: str       # env MCP_BEARER default devtoken
    event_name: str       # env EVENT_NAME default high-value-orders.changed
    callback_url: str     # env CALLBACK_URL default http://127.0.0.1:8001/mcp-events/webhook
    app_host: str         # env APP_HOST default 127.0.0.1
    app_port: int         # env APP_PORT default 8001
    ttl_ms: int           # env SUB_TTL_MS default 60000 (server caps at 30min; refresh well within)
    anthropic_model: str  # env ANTHROPIC_MODEL default claude-sonnet-4-6
    use_llm: bool         # True iff ANTHROPIC_API_KEY present
def load_settings() -> Settings: ...   # reads os.environ
```

## mcp_events/wire.py  [foundation]

Standard-Webhooks profile + Events wire helpers. Mirror `../drasi-mcp-events/crates/mcp-events-wire/src/webhook.rs`.

```python
def gen_whsec(n_bytes: int = 32) -> str            # "whsec_" + standard base64(padded) of n random bytes (24..64)
def parse_whsec(secret: str) -> bytes              # decode; raise ValueError if not whsec_ + base64 of 24..64 bytes
def sign(secret: bytes, msg_id: str, ts: int, body: bytes) -> str   # "v1," + b64(HMAC_SHA256(secret, f"{msg_id}.{ts}.{body}"))
def verify(secret: bytes, msg_id: str, ts: int, body: bytes, signature_header: str) -> bool
    # space-delimited multi-sig; accept if any v1, entry matches; constant-time (hmac.compare_digest); ignore non-v1, entries
def timestamp_fresh(ts: int, now: int, skew_s: int = 300) -> bool   # reject if |now-ts| > skew

# Header name constants (lowercase): WEBHOOK_ID="webhook-id", WEBHOOK_TIMESTAMP="webhook-timestamp",
#   WEBHOOK_SIGNATURE="webhook-signature", MCP_SUBSCRIPTION_ID="x-mcp-subscription-id"

@dataclass
class EventOccurrence: event_id: str; name: str; timestamp: str; data: dict; cursor: str | None
def parse_body(raw: bytes) -> dict                 # json.loads
def control_type(body: dict) -> str | None         # body.get("type") — "verification"|"gap"|"terminated"|None(=event)
def parse_occurrence(body: dict) -> EventOccurrence
```

Tests: known-answer sign/verify (pin one), whsec bounds (23 reject / 24,64 accept / 65 reject / bad prefix),
multi-sig accept, constant-time path, control_type discrimination, occurrence parse. Cross-check the signing
string format against the Rust signer so signatures verify against the real server.

## mcp_events/client.py  [foundation]

Async client over Streamable HTTP (httpx). Handles BOTH `application/json` and `text/event-stream`
unary responses (the server MAY answer a unary POST with an SSE body carrying the response frame —
parse the JSON-RPC response out of the SSE `data:` lines).

```python
class McpEventsClient:
    def __init__(self, base_url: str, bearer: str | None = None): ...
    async def initialize(self) -> dict          # initialize + notifications/initialized; returns server capabilities
    async def list_events(self) -> list[dict]   # events/list -> the events[] array (EventDefinition dicts)
    async def subscribe(self, *, name: str, params: dict | None, callback_url: str,
                        secret: str, ttl_ms: int | None, cursor: str | None = None) -> dict
        # events/subscribe with delivery={mode:"webhook",url,secret}; returns {id, refreshBefore, cursor, truncated, deliveryStatus?}
    async def unsubscribe(self, *, name: str, params: dict | None, callback_url: str) -> None
    async def aclose(self) -> None
# JSON-RPC errors raise McpRpcError(code:int, message:str, data:dict|None).
```

Note the **self-call concurrency requirement**: `subscribe()` blocks while the server calls back to the
agent's own `/mcp-events/webhook` (verification). The agent's FastAPI must service that inbound POST
while this await is in flight — so `subscribe()` MUST use async httpx (non-blocking), never a sync client.

Tests: SSE-unary extraction (response frame pulled from an `event:`/`data:` body); request shaping for
subscribe (delivery object exact); error mapping.

## state.py  [foundation]

Shared, mutable, async-safe handoff between the SubscriptionManager (writer) and the receiver (reader).

```python
@dataclass
class Subscription:
    name: str; params: dict | None; secret: bytes; sub_id: str | None
    refresh_before: str | None; cursor: str | None
class AgentEventState:
    def __init__(self) -> None: ...                 # holds at most one Subscription for the demo + a dedup set
    def set_pending(self, name, params, secret: bytes) -> None   # before subscribe (secret known for verification)
    def set_active(self, sub_id, refresh_before, cursor) -> None
    def current(self) -> Subscription | None
    def secret_for(self, sub_id: str | None) -> bytes | None     # by id, else the single pending/active secret
    def seen(self, event_id: str) -> bool           # dedup; bounded LRU (e.g. 1024)
    schedule: Callable[[EventOccurrence], Awaitable[str]] | None  # set by activation; receiver calls to wake the agent
```

---

## receiver.py  [receiver]

```python
from fastapi import Request
async def handle_webhook(request: Request, st: AgentEventState) -> Response
```
Behavior (per design sketch §Webhook Event Delivery / §Non-event webhook bodies, and our server):
1. Read raw body bytes + headers. Look up secret via `st.secret_for(x-mcp-subscription-id)`. If none → 404.
2. `verify(secret, webhook-id, webhook-timestamp, raw, webhook-signature)` and `timestamp_fresh`; on fail → 401 (do NOT process).
3. `body = parse_body(raw)`; `ct = control_type(body)`:
   - `"verification"` → return `200 {"challenge": body["challenge"]}` (echo; this is what unblocks events/subscribe).
   - `"gap"` → persist `body["cursor"]` into state, log a gap, return `200 {}`.
   - `"terminated"` → log, clear active subscription, signal the SubscriptionManager to re-subscribe, return `200 {}`.
   - event (no type) → `occ = parse_occurrence(body)`; if `st.seen(occ.event_id)` → `200 {"dedup": true}`;
     else persist cursor, `await st.schedule(occ)` (schedules the agent workflow), return `200 {"scheduled": true}` FAST.
4. Never block on agent work — `schedule` is `runner.run(..., wait=False)`.

Provide a factory `def make_webhook_route(st) -> Callable` so activation can mount it.

Tests (no Dapr needed): a fake AgentEventState; assert verification echo, signature-reject (tampered body → 401),
dedup, gap cursor capture, terminated handling, and that an event calls `st.schedule` exactly once with the occurrence.

## subscription.py  [receiver]

```python
class SubscriptionManager:
    def __init__(self, client: McpEventsClient, st: AgentEventState, settings: Settings): ...
    async def start(self) -> None       # initialize → list_events → decide() → subscribe → store → spawn refresh loop
    async def stop(self) -> None        # cancel refresh loop; best-effort unsubscribe
    def decide(self, events: list[dict]) -> tuple[str, dict | None]
        # choose which event + params to subscribe to. Rule: pick settings.event_name if present in the list,
        # else the first event advertising "webhook" in its delivery[]. Log the decision ("the agent decided to ...").
    async def _refresh_loop(self) -> None  # re-subscribe before refresh_before (parse ISO8601; refresh at ~50% of remaining)
```
`start()` ordering is critical: call `st.set_pending(name, params, secret)` BEFORE `client.subscribe(...)`
so the verification callback can find the secret. After subscribe returns, `st.set_active(id, refreshBefore, cursor)`.

Tests: `decide()` selection rule (prefers configured name, falls back to first webhook-capable, raises if none);
refresh interval computation from an ISO8601 refreshBefore; ordering (pending set before subscribe) via a fake client.

---

## agent.py  [agent]

```python
def build_agent(settings: Settings) -> DurableAgent
    # DurableAgent(name="DrasiWatcher", role="...", instructions=[...], tools=[summarize_change],
    #   llm=AnthropicChatClient() if settings.use_llm else DaprChatClient(component_name="echo-llm"),
    #   state=AgentStateConfig(store=StateStoreService(store_name="agent-workflow")))
def format_task(occ: EventOccurrence) -> str
    # Turn a change occurrence into the agent's task string, e.g.
    # "A high-value order change occurred (ADDED): order 42, customer alice, total 5000. Summarize and note any action."
@tool
def summarize_change(change_type: str, summary: str) -> str   # a trivial tool the agent may call; returns a confirmation
```
The agent's job when woken: read the change, produce a one-line decision/summary (LLM or echo). Keep it robust
with no key (echo just returns the prompt — that's fine; the DEMO point is the wake, not the prose).

## activation.py  [agent]

```python
def install(agent: DurableAgent, settings: Settings) -> AgentEventState
    # builds AgentEventState; agent.add_activation(hook); returns the state (for tests)
    # hook(ctx): if ctx.app is None: return None
    #   - st.schedule = lambda occ: ctx.runner.run(ctx.agent, payload={"task": format_task(occ)}, wait=False)
    #   - ctx.app.add_api_route("/mcp-events/webhook", make_webhook_route(st), methods=["POST"])
    #   - register a FastAPI startup handler on ctx.app that constructs McpEventsClient + SubscriptionManager
    #     and calls await mgr.start()  (so subscription happens AFTER uvicorn is listening — the server can
    #     then reach the callback). Keep mgr on app.state for shutdown.
    #   - register a shutdown handler: await mgr.stop(); await client.aclose()
```
Why startup-handler (not the hook body): the verification callback needs the FastAPI server actually
listening. The activation hook runs before uvicorn starts; the FastAPI `startup` event runs after. Start the
SubscriptionManager from `startup`.

## app.py  [agent]

```python
def main() -> None:
    settings = load_settings(); agent = build_agent(settings); install(agent, settings)
    AgentRunner().serve(agent, host=settings.app_host, port=settings.app_port)
if __name__ == "__main__": main()
```

---

## deploy/  [deploy]

- **mcp-server.yaml** — config for the Rust `mcp-events-server` (validate field names against
  `../drasi-mcp-events/crates/mcp-events-server/examples/drasi.yaml` and its README). Must set: `host: 127.0.0.1`,
  `port: 8090`; `authTokens: [{token: devtoken, principal: agent@demo}]`; `eventModeling: single`;
  `feed: {kind: drasiSse, url: http://localhost:8081/events}`; `queries: [{id: high-value-orders, ...}]`;
  `webhook: {enabled: true, allowInsecureUrls: true, ttlCapMs: 1800000, minTtlMs: 10000, maxSubscriptionsPerPrincipal: 16, suspendAfterFailures: 5}`;
  `push: {heartbeatIntervalMs: 15000}`, `poll: {nextPollMs: 2000}`.
- **run-demo.sh** — orchestrator: (1) assert the drasi stack is up (curl :8080/health) else hint `cd ../drasi-mcp-events/drasi && docker compose up -d`; (2) build the MCP server `cargo build --release --manifest-path ../drasi-mcp-events/Cargo.toml -p mcp-events-server` and run it on :8090 with this config (background, log to /tmp); (3) `dapr run --app-id drasi-agent --app-port 8001 --dapr-http-port 3540 --resources-path resources -- uv run python -m drasi_mcp_agent.app`. Print URLs. Trap to clean up.
- **trigger-change.sh** — psql one-liners into the demo Postgres (`docker exec drasi-demo-postgres psql -U demo -d demo -c ...`) to drive ADDED/UPDATED/DELETED (insert >1000; update crossing the threshold; delete). Mirror `../drasi-mcp-events/drasi/RUNBOOK.md`.
- **show-timeline.sh** — tail the agent log for the demo timeline AND poll the Dapr sidecar metrics for the active-actor count of the workflow actor type (`curl -s localhost:<metrics-port>/metrics | grep dapr_runtime_actor_active_actors`) to visibly show idle→0 then wake→>0. (Metrics port is printed in the dapr run log as "metrics server started on …"; document how to find it, or pass `--metrics-port 9095` in run-demo.sh and use that.)

## docs/SPEC-FINDINGS.md  [synthesized at the end]

All `specGap`s collected from build + integration, deduped and severity-rated, framed from the
**webhook-consumer / serverless-agent** vantage (new vs. our server-side SPEC-GAPS.md). Written by the
orchestrator from structured outputs — builders just emit findings.
