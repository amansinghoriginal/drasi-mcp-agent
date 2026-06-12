#!/usr/bin/env bash
#
# trigger-change.sh — drive high-value-orders result-set changes by writing to
# the demo Postgres. Each change makes Drasi emit an SSE frame, which the
# mcp-events-server turns into a signed webhook POST that wakes the agent.
#
# Mirrors ../drasi-mcp-events/drasi/RUNBOOK.md. The query keeps `orders` rows
# with total > 1000; the seed set is {alice 1500, carol 2200}.
#
# Usage:
#   deploy/trigger-change.sh add            # INSERT erin/5000  -> result ADD
#   deploy/trigger-change.sh update         # alice 1500 -> 1800 -> result UPDATE
#   deploy/trigger-change.sh add-cross      # bob 250 -> 1200    -> result ADD (crosses threshold)
#   deploy/trigger-change.sh delete-cross   # bob 1200 -> 250    -> result DELETE (crosses threshold)
#   deploy/trigger-change.sh delete         # DELETE erin        -> result DELETE
#   deploy/trigger-change.sh all            # run a full ADD/UPDATE/DELETE sequence with pauses
set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-drasi-demo-postgres}"
ACTION="${1:-all}"

run_sql() {
  echo "[trigger] psql: $1"
  docker exec "$PG_CONTAINER" psql -U demo -d demo -c "$1"
}

do_add()          { run_sql "INSERT INTO orders (customer, total, status) VALUES ('erin', 5000, 'open');"; }
do_update()       { run_sql "UPDATE orders SET total = 1800 WHERE customer = 'alice';"; }
do_add_cross()    { run_sql "UPDATE orders SET total = 1200 WHERE customer = 'bob';"; }
do_delete_cross() { run_sql "UPDATE orders SET total = 250 WHERE customer = 'bob';"; }
do_delete()       { run_sql "DELETE FROM orders WHERE customer = 'erin';"; }

case "$ACTION" in
  add)          do_add ;;
  update)       do_update ;;
  add-cross)    do_add_cross ;;
  delete-cross) do_delete_cross ;;
  delete)       do_delete ;;
  all)
    echo "[trigger] full ADD -> UPDATE -> DELETE sequence (watch deploy/show-timeline.sh)"
    do_add;          sleep 5
    do_update;       sleep 5
    do_add_cross;    sleep 5
    do_delete_cross; sleep 5
    do_delete
    ;;
  *)
    echo "usage: $0 {add|update|add-cross|delete-cross|delete|all}" >&2
    exit 2
    ;;
esac

echo "[trigger] done: $ACTION"
