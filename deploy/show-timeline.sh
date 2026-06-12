#!/usr/bin/env bash
#
# show-timeline.sh — make the scale-to-zero behaviour visible.
#
# Two signals, side by side:
#   1) the agent log (the demo timeline: subscription, webhook receipt, the
#      DurableAgent waking and summarizing the change), tailed from the file
#      run-demo.sh tees to; and
#   2) the Dapr sidecar's active-actor gauge, polled from the metrics endpoint.
#      Watch it sit at 0 (idle, actors deactivated) and tick up to >0 when a
#      webhook wakes the workflow, then fall back to 0 once it idles out.
#
# Usage:  deploy/show-timeline.sh [metrics_port] [agent_log]
#   metrics_port  default 9095 (the --metrics-port passed in run-demo.sh)
#   agent_log     default /tmp/drasi-agent.log
#
# Find the metrics port yourself from the dapr run output if you changed it:
#   look for a line like "metrics server started on :9095".
set -euo pipefail

METRICS_PORT="${1:-9095}"
AGENT_LOG="${2:-/tmp/drasi-agent.log}"
METRIC="dapr_runtime_actor_active_actors"
METRICS_URL="http://localhost:${METRICS_PORT}/metrics"

echo "[timeline] active-actor metric : $METRICS_URL ($METRIC)"
echo "[timeline] agent log           : $AGENT_LOG"
echo "[timeline] Ctrl-C to stop."
echo

TAIL_PID=""
cleanup() {
  if [[ -n "$TAIL_PID" ]] && kill -0 "$TAIL_PID" 2>/dev/null; then
    kill "$TAIL_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# Tail the agent log (the human-readable timeline) in the background if present.
if [[ -f "$AGENT_LOG" ]]; then
  ( tail -n 0 -f "$AGENT_LOG" | sed -u 's/^/[agent] /' ) &
  TAIL_PID=$!
else
  echo "[timeline] (no agent log yet at $AGENT_LOG — start deploy/run-demo.sh first)"
fi

# Poll the active-actor gauge once a second. Print only non-comment metric lines;
# when none are present the count is effectively 0 (no actors activated yet).
while true; do
  ts="$(date '+%H:%M:%S')"
  lines="$(curl -fsS "$METRICS_URL" 2>/dev/null | grep "^${METRIC}" || true)"
  if [[ -z "$lines" ]]; then
    echo "[metric $ts] ${METRIC} = 0 (idle / no active actors, or sidecar not up yet)"
  else
    while IFS= read -r line; do
      echo "[metric $ts] $line"
    done <<<"$lines"
  fi
  sleep 1
done
