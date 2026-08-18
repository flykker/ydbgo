#!/usr/bin/env bash
set -euo pipefail
# Boots a two-node cluster from a YDB-style config file and verifies topology,
# auto-join and flag-over-config precedence. Self-contained: uses its own ports
# and a temp data dir, and only ever kills its own processes (never the demo
# node from ui-restart.sh).
#
# Usage: ./scripts/qa-config.sh
# Env overrides: YDBGO_BIN  built binary path (default ./bin/ydbgo)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${YDBGO_BIN:-$ROOT/bin/ydbgo}"

# Ports: deliberately different from the demo node (2135/7001/8080) so the QA
# script can run while the UI server is up.
GRPC1=2235; RAFT1=7101; HTTP1=18080
GRPC2=2236; RAFT2=7102; HTTP2=18081
export NVM_DIR="${NVM_DIR:-$HOME/.nvm}"

WORK=$(mktemp -d /tmp/ydbgo-qa-config.XXXXXX)
PID1=""; PID2=""
cleanup() {
  for pid in "$PID1" "$PID2"; do
    if [ -n "$pid" ]; then
      kill -9 "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/cluster.yaml" <<YAML
config:
  hosts:
  - {host: 127.0.0.1, grpc: $GRPC1, raft: $RAFT1, data: $WORK/data1, id: n1, bootstrap: true}
  - {host: 127.0.0.1, grpc: $GRPC2, raft: $RAFT2, data: $WORK/data2, id: n2}
YAML

echo "[build] $BIN"
mkdir -p "$ROOT/bin"
go build -o "$BIN" "$ROOT/cmd/ydbgo"

say() { echo "== $*"; }
ok=1
check() {
  if [ "$2" = "$3" ]; then echo "  ok: $1"; else echo "  FAIL: $1 (got: $2, want: $3)"; ok=0; fi
}

wait_http() { # wait_http PORT
  for _ in $(seq 1 20); do
    if curl -sf "http://127.0.0.1:$1/api/v1/health" > /dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

say "start n2 (joiner) BEFORE the bootstrap node: requestJoin must retry"
"$BIN" serve -config "$WORK/cluster.yaml" -node-id n2 -http ":$HTTP2" > "$WORK/n2.log" 2>&1 &
PID2=$!
sleep 2

say "start n1 (bootstrap) from config; -http passed via flag to prove flags win"
"$BIN" serve -config "$WORK/cluster.yaml" -node-id n1 -http ":$HTTP1" > "$WORK/n1.log" 2>&1 &
PID1=$!
if ! wait_http "$HTTP1"; then echo "  FAIL: n1 did not become healthy"; tail -5 "$WORK/n1.log"; exit 1; fi
echo "  ok: n1 healthy on :$HTTP1"

say "n2 must have retried and joined once n1 was up"
if ! wait_http "$HTTP2"; then echo "  FAIL: n2 did not become healthy"; tail -5 "$WORK/n2.log"; exit 1; fi
echo "  ok: n2 healthy on :$HTTP2"

say "both nodes registered cluster-wide"
NODES=$(curl -s "http://127.0.0.1:$HTTP1/api/v1/nodes")
check "nodes count" "$(printf '%s' "$NODES" | grep -o '"id"' | wc -l | tr -d ' ')" "2"
printf '%s' "$NODES" | grep -q '"n1"' && echo "  ok: n1 present" || { echo "  FAIL: n1 missing"; ok=0; }
printf '%s' "$NODES" | grep -q '"n2"' && echo "  ok: n2 present" || { echo "  FAIL: n2 missing"; ok=0; }

say "cluster-wide DDL/DML/read through the config-derived addresses"
"$BIN" run -config "$WORK/cluster.yaml" "CREATE TABLE qaconf (id int64 primary key, v string)" > /dev/null
"$BIN" run -config "$WORK/cluster.yaml" -addr "127.0.0.1:$GRPC2" "INSERT INTO qaconf VALUES (1,'hello'),(2,'world')" > /dev/null
CNT=$("$BIN" run -config "$WORK/cluster.yaml" "SELECT COUNT(*) AS n FROM qaconf" | grep -Eo '2' | head -1)
check "read via bootstrap node sees n2 writes" "$CNT" "2"

say "explicit -addr overrides the config's bootstrap address"
if "$BIN" run -config "$WORK/cluster.yaml" -addr "127.0.0.1:1" "ADMIN TABLES" > /dev/null 2>&1; then
  echo "  FAIL: expected connection failure on overridden addr"; ok=0
else
  echo "  ok: flag override respected (bad -addr failed to connect)"
fi

say "unknown -node-id is rejected (config is consulted)"
if "$BIN" serve -config "$WORK/cluster.yaml" -node-id nope -http ":18999" > /dev/null 2>&1; then
  echo "  FAIL: unknown node id should be rejected"; ok=0
else
  echo "  ok: unknown -node-id rejected"
fi

echo
if [ "$ok" -eq 1 ]; then echo "RESULT: OK"; exit 0; fi
echo "RESULT: PROBLEMS"; exit 1
