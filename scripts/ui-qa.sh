#!/usr/bin/env bash
set -euo pipefail
# Runs every UI QA script (internal/ui/web/verify-*.mjs) against a running
# server. Each script drives headless Chrome over puppeteer-core and prints its
# own result (expected final line: "RESULT: OK").
#
# Usage: ./scripts/ui-qa.sh [pattern]       # optional filename glob, e.g. "verify-grid*"
# Requires: server running (see ./scripts/ui-restart.sh), Node 22 LTS, Chrome.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PATTERN="${1:-verify-*.mjs}"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
if [ -s "$NVM_DIR/nvm.sh" ]; then . "$NVM_DIR/nvm.sh"; fi

cd "$ROOT/internal/ui/web"

# Explicit order matters: verify-metrics writes into the logs table and must run
# before verify-admin (which splits logs and leaves a split shard behind).
SCRIPTS=(verify-metrics.mjs verify-admin.mjs verify-dash.mjs verify-dashhash.mjs verify-export.mjs verify-grid.mjs verify-logo.mjs verify-pages.mjs verify-scroll.mjs)

FAILED=0
for f in "${SCRIPTS[@]}"; do
  case $f in
    $PATTERN) ;;
    *) continue ;;
  esac
  [ -e "$f" ] || { echo "no scripts match: $PATTERN" >&2; exit 2; }
  echo "== $f"
  if node "$f"; then :; else FAILED=1; fi
done

if [ "$FAILED" -eq 0 ]; then
  echo
  echo "ALL UI QA SCRIPTS: RESULT OK"
else
  echo
  echo "SOME UI QA SCRIPTS FAILED" >&2
  exit 1
fi
