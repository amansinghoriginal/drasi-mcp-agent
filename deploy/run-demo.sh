#!/usr/bin/env bash
#
# run-demo.sh — bring up the full scale-to-zero webhook-agent demo.
#
# Topology (see docs/ARCHITECTURE.md):
#   Postgres -> Drasi (docker compose, :8080/:8081, started separately)
#     -> mcp-events-server (Rust, this script builds + runs it on :8090)
#       -> webhook POST -> this agent (Dapr, app :8001, sidecar HTTP :3540,
#          sidecar metrics :9095)
#
# Usage:  deploy/run-demo.sh
# Stop:   Ctrl-C (the trap stops the MCP server; dapr stops the agent).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRASI_EVENTS_DIR="$(cd "$REPO_ROOT/.." && pwd)/drasi-mcp-events"
DRASI_DIR="$DRASI_EVENTS_DIR/drasi"

MCP_CONFIG="$REPO_ROOT/deploy/mcp-server.yaml"
MCP_BIN="$DRASI_EVENTS_DIR/target/release/mcp-events-server"
MCP_LOG="/tmp/mcp-events-server.log"
AGENT_LOG="/tmp/drasi-agent.log"

APP_ID="drasi-agent"
APP_PORT=8001
DAPR_HTTP_PORT=3540
METRICS_PORT=9095

MCP_PID=""
cleanup() {
  if [[ -n "$MCP_PID" ]] && kill -0 "$MCP_PID" 2>/dev/null; then
    echo "[run-demo] stopping mcp-events-server (pid $MCP_PID)"
    kill "$MCP_PID" 2>/dev/null || true
    wait "$MCP_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# 1) The Drasi stack must already be up (it owns Postgres + the SSE reaction).
echo "[run-demo] checking the Drasi stack on http://localhost:8080 ..."
if ! curl -fsS http://localhost:8080/health >/dev/null 2>&1; then
  echo "[run-demo] ERROR: Drasi is not reachable at http://localhost:8080/health"
  echo "[run-demo] start it first, then re-run this script:"
  echo "    (cd \"$DRASI_DIR\" && docker compose up -d)"
  exit 1
fi
echo "[run-demo] Drasi is up."

# 2) Build and start the reference MCP Events server on :8090.
echo "[run-demo] building mcp-events-server (release) ..."
cargo build --release --manifest-path "$DRASI_EVENTS_DIR/Cargo.toml" -p mcp-events-server

echo "[run-demo] starting mcp-events-server on :8090 (log: $MCP_LOG) ..."
"$MCP_BIN" --config "$MCP_CONFIG" >"$MCP_LOG" 2>&1 &
MCP_PID=$!

echo "[run-demo] waiting for mcp-events-server /healthz ..."
for _ in $(seq 1 50); do
  if curl -fsS http://127.0.0.1:8090/healthz >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$MCP_PID" 2>/dev/null; then
    echo "[run-demo] ERROR: mcp-events-server exited early; see $MCP_LOG"
    exit 1
  fi
  sleep 0.2
done
echo "[run-demo] mcp-events-server is up (pid $MCP_PID)."

# 3) Run the agent under a Dapr sidecar. Foreground; logs also teed to a file so
#    deploy/show-timeline.sh can tail the demo timeline from another terminal.
cat <<EOF
[run-demo] starting the agent under Dapr:
    MCP server endpoint : http://127.0.0.1:8090/mcp
    agent webhook URL   : http://127.0.0.1:8001/mcp-events/webhook
    agent run endpoint  : http://127.0.0.1:8001/agent/run
    dapr sidecar HTTP   : http://127.0.0.1:3540
    dapr sidecar metrics: http://127.0.0.1:9095/metrics
    agent log (tee)     : $AGENT_LOG
  In another terminal:  deploy/show-timeline.sh   and   deploy/trigger-change.sh add
EOF

cd "$REPO_ROOT"
dapr run \
  --app-id "$APP_ID" \
  --app-port "$APP_PORT" \
  --dapr-http-port "$DAPR_HTTP_PORT" \
  --metrics-port "$METRICS_PORT" \
  --resources-path "$REPO_ROOT/resources" \
  -- uv run python -m drasi_mcp_agent.app 2>&1 | tee "$AGENT_LOG"
