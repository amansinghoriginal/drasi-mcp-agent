# drasi-mcp-agent

An **event-driven, scale-to-zero AI agent** that subscribes to a [Drasi](https://drasi.io)
continuous-query's result changes via the draft [MCP **Events**
extension](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/pull/1)
(**webhook** delivery mode), sits idle while **Dapr actors deactivate** ("scale to zero"), and is
**woken by the inbound signed webhook** when the query result changes — no polling, no held
connection, no Kubernetes.

> Prototype for the MCP Triggers & Events Working Group. The "left half" (Drasi + the MCP Events
> server) is [`drasi-mcp-events`](https://github.com/amansinghoriginal/drasi-mcp-events); this repo
> is the **consumer**: a [Dapr Agents](https://github.com/dapr/dapr-agents) agent that registers the
> subscription and reacts. Verified end-to-end on a laptop.

## The loop

```
Postgres ─WAL→ Drasi ─continuous query→ SSE reaction ─→ drasi-mcp-events (Rust MCP server)
   INSERT/UPDATE/DELETE   "row entered / left /            │  holds the webhook subscription;
                           changed in the result set"      │  signs + POSTs each change
                                                            ▼
                          ┌──────── this repo: Dapr Agent (one always-up pod) ────────┐
   events/subscribe ──────┤  POST /mcp-events/webhook → verify HMAC, echo the          │
   (agent → server)       │    verification challenge, dedup, schedule the workflow,    │
   + self-refresh loop    │    ack 2xx fast                                             │
                          │  SubscriptionManager: events/list → decide → subscribe      │
                          │  DurableAgent: wakes, summarizes the change, idles again     │
                          │  idle ⇒ Dapr actors deactivate (state in Redis) = scale-0    │
                          └──────────────────────────────────────────────────────────────┘
```

## Verified end-to-end

A real `INSERT`/`UPDATE`/threshold-cross-`DELETE` into Postgres flows all the way to the agent
waking. From a live run (`docs/demo-transcript.txt`):

```
[idle] active workflow actors = 0
----- trigger: ADD -----  INSERT INTO orders ... ('grace', 8000, 'open')
  ┊ receiver: scheduled agent workflow … for event …
  ┊ A high-value order change occurred (ADDED): order 10, customer grace, total 8000. …
----- trigger: DELETE-cross -----  UPDATE orders SET total=100 WHERE customer='grace'
  ┊ A high-value order change occurred (DELETED): order 7, customer grace, total 8500. …
[settled] active workflow actors = 0
```

The agent is at **0 active workflow actors** before and after each event — it does no work until a
change wakes it. The verification handshake (server POSTs a challenge *back* to the agent's own
endpoint during `events/subscribe`) and Standard-Webhooks signatures both verify against the real
Rust server.

## What's in here

| Module | Role |
|---|---|
| `mcp_events/wire.py` | Standard Webhooks verify + whsec, control-envelope/occurrence parsing — signature-compatible with the Rust server |
| `mcp_events/client.py` | async MCP Events client (`initialize`/`list`/`subscribe`/`unsubscribe`; JSON **and** SSE-framed unary) |
| `receiver.py` | the webhook route: HMAC verify, challenge echo, dedup, schedule (fast ack) |
| `subscription.py` | discover (`events/list`) → **decide** → subscribe → self-refresh before TTL |
| `agent.py` | the `DurableAgent` that processes a change (echo LLM by default, Claude with a key) |
| `activation.py` | wires the webhook route + subscription into one always-up pod |

110 tests (`uv run pytest`), ruff-clean. Findings for the WG: **[docs/SPEC-FINDINGS.md](docs/SPEC-FINDINGS.md)** — 6 high-severity gaps in the spec's (missing) model of a scale-to-zero consumer, plus consumer-side wire findings.

## Run it (laptop, no Kubernetes, no API key)

Prereqs: Docker, [`dapr` CLI](https://docs.dapr.io/getting-started/install-dapr-cli/) (`dapr init`
once), [`uv`](https://docs.astral.sh/uv/), and a sibling checkout of
[`drasi-mcp-events`](https://github.com/amansinghoriginal/drasi-mcp-events).

```bash
# 1) bring up Drasi (Postgres + continuous query + SSE reaction)
(cd ../drasi-mcp-events/drasi && docker compose up -d)

# 2) one command: builds + runs the MCP server, then runs the agent under Dapr
uv sync
deploy/run-demo.sh

# 3) in another terminal: make the result set change, and watch the agent wake
deploy/trigger-change.sh add        # INSERT a high-value order  → ADDED  → wake
deploy/trigger-change.sh update     # change it                  → UPDATED → wake
deploy/trigger-change.sh delete-cross  # drop below the threshold → DELETED → wake
deploy/show-timeline.sh             # tail the wake timeline + the active-actor gauge
```

**No API key needed:** the agent uses Dapr's `conversation.echo` component, so the wake → process →
complete loop runs with zero credentials. Set `ANTHROPIC_API_KEY` (optionally `ANTHROPIC_MODEL`,
default `claude-sonnet-4-6`) to have it produce real one-line summaries and call the `summarize_change`
tool.

## Scale-to-zero, precisely

"Scale to zero" here is **Dapr virtual-actor deactivation**, not zero pods: the agent pod and its
Dapr sidecar stay up (the webhook must be reachable and the subscription refreshed), but the
workflow actors that do the reasoning are reclaimed when idle and rehydrated on the next event —
sub-second, state preserved in Redis. True zero-*pod* (KEDA) is possible but unnecessary here and is
not built. See `docs/SPEC-FINDINGS.md` H4 for why this tiering matters to the spec.

## Known prototype shortcuts

Loopback `http://` callback via the server's `allowInsecureUrls` flag (nonconformant with the spec's
TLS MUST — local only); in-memory subscription/dedup state (lost on a cold restart, recovered on the
next refresh); single subscription; the no-key `conversation.echo` path "reasons" by echoing. See
[`docs/SPEC-FINDINGS.md`](docs/SPEC-FINDINGS.md) and [`docs/INTEGRATION-NOTES.md`](docs/INTEGRATION-NOTES.md).

## License & provenance

Apache-2.0. A community prototype by a Drasi maintainer; not an official artifact of the MCP project,
Anthropic, Microsoft, the Drasi project, or Dapr. `docs/design-sketch-proposal.md` is a vendored copy
of the WG design sketch (author: Peter Alexander, Anthropic) for reference.
