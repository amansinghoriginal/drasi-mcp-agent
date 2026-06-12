# Integration notes

Framework/runtime issues hit while making the end-to-end demo actually run (dapr-agents + Dapr
1.18 + this agent), and how they were resolved. These are **build/environment** notes — distinct
from the *protocol* findings in [`SPEC-FINDINGS.md`](SPEC-FINDINGS.md).

## Environment (verified working)

- **Dapr runtime 1.18.0** self-hosted (`dapr init`) — matches dapr-agents' pin even though the
  installed `dapr` CLI was 1.15.0 (the CLI installs/runs the 1.18 `daprd`).
- **Python 3.13** via `uv`; dapr-agents installed as a local editable dependency (`../dapr-agents`)
  so the API matches what was built against.
- No Kubernetes. No API key (see below).

## Issues found and fixed

### 1. `DurableAgent` with `llm=None` still requires an LLM at runtime
The built-in agent loop always invokes the model, so `llm=None` fails mid-workflow with
`No LLM component provided`. **Fix:** default to Dapr's **`conversation.echo`** component
(`resources/echo-llm.yaml`, `DaprChatClient(component_name="echo-llm")`), which completes the loop
with no credentials. A real `AnthropicChatClient()` is used when `ANTHROPIC_API_KEY` is set.

### 2. `conversation.echo` + tools = a cosmetic malformed tool call
Echo is not a real model, so when the agent is given tools it "echoes" an invalid tool call
(`ERROR: Invalid tool_call entry …`) — logged, but the workflow still completes. **Fix:** only
attach the `summarize_change` tool when a real LLM is configured (`tools = [...] if use_llm else []`).
The no-key path then runs clean; tool use is exercised only with a real key.

### 3. Starlette ≥1.3 removed `FastAPI.add_event_handler`
dapr-agents pulls Starlette 1.3.0 / FastAPI 0.136, where `app.add_event_handler(...)` no longer
exists — the activation hook crashed hosting with `'FastAPI' object has no attribute
'add_event_handler'`. **Fix:** append directly to the router lifecycle lists,
`app.router.on_startup.append(...)` / `on_shutdown.append(...)`, which the lifespan runner still
honors.

### 4. The verification handshake vs. uvicorn startup ordering (the load-bearing one)
`events/subscribe` blocks while the server POSTs a `verification` challenge **back** to the agent's
own `/mcp-events/webhook`, and the reference server runs that handshake **once, no retries**. But
uvicorn fires FastAPI `startup` handlers *before* it opens the listening socket — so subscribing
from inside `startup` would have the challenge hit a closed port and the single attempt would fail
(`-32015`). **Fix:** the startup handler does **not** `await mgr.start()`; it schedules it as a
background task that runs once the loop is serving and the socket is up. (This is also recorded as
spec finding **H1** — the spec assumes a model where this ordering problem doesn't exist.)

### 5. Self-call concurrency
Because the agent calls `events/subscribe` *and* must service the inbound verification POST while
that call is in flight, the MCP client uses **async httpx** — a blocking client would deadlock the
single event loop. Confirmed working: the subscribe await yields, uvicorn serves the verification
request concurrently, the receiver echoes, subscribe returns.

### 6. Harmless noise
- `GET /dapr/subscribe → 404` on startup: Dapr probes for declarative pub/sub subscriptions; we use
  none. Ignored.
- Active-actor gauge reads `0` even during processing in the capture script: the workflow completes
  in well under the 0.5s poll interval, so the snapshot misses the brief activation. The meaningful
  signal is that it is `0` at idle and returns to `0` — no standing compute.

## Demo-data hygiene

The capture in `docs/demo-transcript.txt` shows two events for one `UPDATE` and an "order 7" label
for a row inserted as order 10 — **not a bug.** The demo Postgres had two rows with
`customer='grace'` (one left over from an earlier session), so `UPDATE ... WHERE customer='grace'`
legitimately changed both rows and Drasi correctly emitted one event per affected row. The bundled
`deploy/trigger-change.sh` uses distinct keys to avoid this.
