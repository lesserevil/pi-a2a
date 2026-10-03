#!/usr/bin/env bash
# typecheck.sh — portable type check: locate the global pi install, link its types into the local node_modules, then run tsc.
# Usage: npm run typecheck
set -euo pipefail
cd "$(dirname "$0")/.."

# 1) locate the global pi install
PI="$(node -e "try{console.log(require.resolve('@earendil-works/pi-coding-agent/package.json').replace(/\/package\.json$/,''))}catch{console.log('')}" 2>/dev/null || true)"
if [ -z "$PI" ]; then
  PI="$(npm root -g 2>/dev/null)/@earendil-works/pi-coding-agent"
fi
if [ ! -d "$PI" ]; then
  echo "✗ Global pi install (@earendil-works/pi-coding-agent) not found. Make sure pi is installed globally (npm i -g @earendil-works/pi-coding-agent)." >&2
  exit 1
fi

# 2) idempotently link pi's types into the local node_modules (node_modules is gitignored, so publishing is unaffected)
mkdir -p node_modules/@earendil-works node_modules/@types
ln -sfn "$PI"                                node_modules/@earendil-works/pi-coding-agent
ln -sfn "$PI/node_modules/@earendil-works/pi-agent-core" node_modules/@earendil-works/pi-agent-core
ln -sfn "$PI/node_modules/@earendil-works/pi-ai"         node_modules/@earendil-works/pi-ai
ln -sfn "$PI/node_modules/@types/node"                   node_modules/@types/node
[ -d "$PI/node_modules/typebox" ] && ln -sfn "$PI/node_modules/typebox" node_modules/typebox
echo "✓ Linked pi types: $PI"

# 3) type check (extensions/ + scripts/ only, noEmit)
npx tsc -p tsconfig.check.json
echo "✓ Type check passed (0 errors)"
