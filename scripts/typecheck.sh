#!/usr/bin/env bash
# typecheck.sh — 便携类型检查：自动定位全局 pi 安装、链接其类型到本地 node_modules，再跑 tsc。
# 用法: npm run typecheck
set -euo pipefail
cd "$(dirname "$0")/.."

# 1) 定位全局 pi 安装
PI="$(node -e "try{console.log(require.resolve('@earendil-works/pi-coding-agent/package.json').replace(/\/package\.json$/,''))}catch{console.log('')}" 2>/dev/null || true)"
if [ -z "$PI" ]; then
  PI="$(npm root -g 2>/dev/null)/@earendil-works/pi-coding-agent"
fi
if [ ! -d "$PI" ]; then
  echo "✗ 找不到全局 pi 安装 (@earendil-works/pi-coding-agent)。请确认 pi 已全局安装 (npm i -g @earendil-works/pi-coding-agent)。" >&2
  exit 1
fi

# 2) 幂等链接 pi 类型到本地 node_modules（node_modules 已 gitignore，不影响发布）
mkdir -p node_modules/@earendil-works node_modules/@types
ln -sfn "$PI"                                node_modules/@earendil-works/pi-coding-agent
ln -sfn "$PI/node_modules/@earendil-works/pi-agent-core" node_modules/@earendil-works/pi-agent-core
ln -sfn "$PI/node_modules/@earendil-works/pi-ai"         node_modules/@earendil-works/pi-ai
ln -sfn "$PI/node_modules/@types/node"                   node_modules/@types/node
[ -d "$PI/node_modules/typebox" ] && ln -sfn "$PI/node_modules/typebox" node_modules/typebox
echo "✓ 已链接 pi 类型: $PI"

# 3) 类型检查（仅检查 extensions/ + scripts/，noEmit）
npx tsc -p tsconfig.check.json
echo "✓ 类型检查通过 (0 errors)"
