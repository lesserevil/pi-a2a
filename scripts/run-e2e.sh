#!/usr/bin/env bash
# run-e2e.sh — end-to-end delegation loop test: start the BE daemon, run the FE test, clean up on exit.
# Verifies: FE sends request → BE completes task → result returned immediately via A2A push-notification.
# Usage: npm run test:e2e
set -uo pipefail   # no -e: cleanup and reporting must still happen when the FE test fails
cd "$(dirname "$0")/.."

PRESENCE_DIR="$HOME/.pi/agent/pi-a2a-presence"
BE_DB="$HOME/.pi/agent/pi-a2a-be.db.json"
FE_DB="$HOME/.pi/agent/pi-a2a-fe-test.db.json"

cleanup() {
  [ -n "${BE:-}" ] && kill "$BE" 2>/dev/null || true
  rm -f "$PRESENCE_DIR/be-daemon-001.json" "$PRESENCE_DIR/fe-test-"*.json "$BE_DB" "$FE_DB" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# pre-cleanup (avoid interference from leftover presence)
cleanup
sleep 0.5

echo "▶ Starting BE daemon ..."
bun scripts/be-daemon.ts > /tmp/pi-a2a-be.log 2>&1 &
BE=$!
sleep 2

echo "▶ Running FE test ..."
bun scripts/e2e-fe-test.ts
RC=$?

if [ "$RC" -eq 0 ]; then
  echo ""
  echo "✅ E2E passed: the A2A delegation loop (request → task completion → immediate push result) works."
else
  echo ""
  echo "❌ E2E failed (exit=$RC). BE log:"
  tail -20 /tmp/pi-a2a-be.log 2>/dev/null || true
fi
rm -f /tmp/pi-a2a-be.log 2>/dev/null || true
exit "$RC"
