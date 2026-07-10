#!/usr/bin/env bash
# run-e2e.sh — 端到端委派闭环测试：起 BE daemon，跑 FE 测试，退出时清理。
# 验证: FE 发 request → BE 完结 task → 经 A2A push-notification 即时回 result。
# 用法: npm run test:e2e
set -uo pipefail   # 不用 -e：FE 测试失败也要清理并报告
cd "$(dirname "$0")/.."

PRESENCE_DIR="$HOME/.pi/agent/pi-a2a-presence"
BE_DB="$HOME/.pi/agent/pi-a2a-be.db.json"
FE_DB="$HOME/.pi/agent/pi-a2a-fe-test.db.json"

cleanup() {
  [ -n "${BE:-}" ] && kill "$BE" 2>/dev/null || true
  rm -f "$PRESENCE_DIR/be-daemon-001.json" "$PRESENCE_DIR/fe-test-"*.json "$BE_DB" "$FE_DB" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# 预清理（避免残留 presence 干扰）
cleanup
sleep 0.5

echo "▶ 启动 BE daemon ..."
bun scripts/be-daemon.ts > /tmp/pi-a2a-be.log 2>&1 &
BE=$!
sleep 2

echo "▶ 运行 FE 测试 ..."
bun scripts/e2e-fe-test.ts
RC=$?

if [ "$RC" -eq 0 ]; then
  echo ""
  echo "✅ E2E 通过：A2A 委派闭环（request → task 完结 → push 即时回 result）正常。"
else
  echo ""
  echo "❌ E2E 失败 (exit=$RC)。BE 日志:"
  tail -20 /tmp/pi-a2a-be.log 2>/dev/null || true
fi
rm -f /tmp/pi-a2a-be.log 2>/dev/null || true
exit "$RC"
