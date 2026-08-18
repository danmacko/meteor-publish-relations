#!/usr/bin/env bash
#
# test.sh — run the package's tests.
#
# The suite has two layers:
#
#   unit      tests/unit/*  — the server modules loaded into a vm with Meteor
#                             stubbed out. No Meteor, no MongoDB, milliseconds.
#                             Covers refcounting, the retraction queue, the
#                             deferred restart and the overlap guards.
#
#   tinytest  tests/*.js    — the real thing: a Meteor test server, a real
#                             MongoDB and a real DDP client. Covers DDP message
#                             order and observer lifecycle (leaks, teardown).
#
# Usage:
#   ./test.sh            unit layer only (fast, no side effects)
#   ./test.sh --full     both layers (boots a Meteor test server on $PORT)
#
set -euo pipefail
cd "$(dirname "$0")"

RELEASE="${METEOR_RELEASE:-METEOR@3.4.1}"
PORT="${PORT:-3199}"

# Prefer a system node, but only one new enough for `require('node:...')`
# (14.18+). Otherwise fall back to the one bundled with the Meteor tool.
if command -v node >/dev/null 2>&1 && node -e "require('node:path')" >/dev/null 2>&1; then
  NODE=(node)
else
  NODE=(meteor node)
fi

echo "== unit layer =="
"${NODE[@]}" tests/unit/run.js

if [[ "${1:-}" != "--full" ]]; then
  echo
  echo "Tinytest layer skipped — run './test.sh --full' to include it."
  exit 0
fi

echo
echo "== tinytest layer (meteor $RELEASE, port $PORT) =="

LOG="$(mktemp -t publish-relations-test.XXXXXX)"
cleanup () {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  pkill -f "test-packages --release $RELEASE --port $PORT" 2>/dev/null || true
  rm -f "$LOG"
}
trap cleanup EXIT

meteor test-packages --release "$RELEASE" --port "$PORT" ./ > "$LOG" 2>&1 &
SERVER_PID=$!

echo "waiting for the test server to build..."
for _ in $(seq 1 150); do
  if grep -q "App running at" "$LOG" 2>/dev/null; then break; fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "test server exited early:" >&2
    tail -30 "$LOG" >&2
    exit 1
  fi
  sleep 2
done

if ! grep -q "App running at" "$LOG" 2>/dev/null; then
  echo "test server did not start in time:" >&2
  tail -30 "$LOG" >&2
  exit 1
fi

"${NODE[@]}" tests/headless-driver.js "ws://localhost:$PORT/websocket"
