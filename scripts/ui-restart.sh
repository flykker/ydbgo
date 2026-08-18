#!/usr/bin/env bash
set -euo pipefail
# Builds the frontend + server binary and (re)starts the single-node demo server
# used by the UI QA scripts. Safe to re-run; kills any previous instance first.
#
# Usage: ./scripts/ui-restart.sh
# Env overrides:
#   YDBGO_BIN   built binary path (default ./bin/ydbgo)
#   DATA        data dir (default /tmp/ydbgo-data)
#   SQL_ADDR    gRPC SQL listen addr (default 127.0.0.1:2135)
#   HTTP_ADDR   HTTP console listen addr (default :8080)
#   RAFT_ADDR   raft listen addr (default 127.0.0.1:7001)
#   NODE_ID     node id (default n1)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"
if [ -s "$NVM_DIR/nvm.sh" ]; then . "$NVM_DIR/nvm.sh"; fi

YDBGO_BIN="${YDBGO_BIN:-$ROOT/bin/ydbgo}"
DATA="${DATA:-/tmp/ydbgo-data}"
SQL_ADDR="${SQL_ADDR:-127.0.0.1:2135}"
HTTP_ADDR="${HTTP_ADDR:-:8080}"
RAFT_ADDR="${RAFT_ADDR:-127.0.0.1:7001}"
NODE_ID="${NODE_ID:-n1}"

echo "[1/3] build frontend (internal/ui/web)"
( cd internal/ui/web && npm run build )

echo "[2/3] build server binary -> $YDBGO_BIN"
mkdir -p bin
go build -o "$YDBGO_BIN" ./cmd/ydbgo

echo "[3/3] restart server ($NODE_ID, sql=$SQL_ADDR http=$HTTP_ADDR raft=$RAFT_ADDR, data=$DATA)"
# kill in a separate step so pkill never matches this script's own command line
pkill -9 -f '[y]dbgo serve' || true
sleep 1
setsid nohup "$YDBGO_BIN" serve \
  -addr "$SQL_ADDR" -data "$DATA" -http "$HTTP_ADDR" \
  -raft-addr "$RAFT_ADDR" -node-id "$NODE_ID" -bootstrap -rf 1 \
  > /tmp/ydbgo-serve.log 2>&1 < /dev/null & disown

# build a health URL from the leading-':' listen addr form (:8080 -> http://localhost:8080)
case "$HTTP_ADDR" in
  :*) HEALTH_URL="http://localhost${HTTP_ADDR}/api/v1/health" ;;
  *)  HEALTH_URL="http://${HTTP_ADDR}/api/v1/health" ;;
esac

start_server() {
  pkill -9 -f '[y]dbgo serve' || true
  sleep 1
  setsid nohup "$YDBGO_BIN" serve \
    -addr "$SQL_ADDR" -data "$DATA" -http "$HTTP_ADDR" \
    -raft-addr "$RAFT_ADDR" -node-id "$NODE_ID" -bootstrap -rf 1 \
    > /tmp/ydbgo-serve.log 2>&1 < /dev/null & disown

  for _ in $(seq 1 10); do
    if curl -sf "$HEALTH_URL" > /dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

if start_server; then
  echo "server up: http://localhost${HTTP_ADDR}"
else
  # A hard -9 kill racing a reseed can leave the raft catalog without the demo
  # tables; for scratch /tmp data dirs just recreate them and retry once.
  if [[ "$DATA" == /tmp/* ]]; then
    echo "server failed to start; wiping demo data dir $DATA and retrying" >&2
    rm -rf "$DATA"
    if start_server; then
      echo "server up: http://localhost${HTTP_ADDR}"
    else
      echo "server failed to start; tail of /tmp/ydbgo-serve.log:" >&2
      tail -20 /tmp/ydbgo-serve.log >&2
      exit 1
    fi
  else
    echo "server failed to start; tail of /tmp/ydbgo-serve.log:" >&2
    tail -20 /tmp/ydbgo-serve.log >&2
    exit 1
  fi
fi
