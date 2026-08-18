#!/usr/bin/env bash
set -euo pipefail
# A/B benchmark: native columnar ENGINE=CSTORE2 vs the MVCC-columnar CSTORE on
# a 5-node RF=3 cluster. Loads N rows into both engines through the bench
# client, then times columnar reads (COUNT/SUM/GROUP BY/projection/point get)
# and compares on-disk sizes.
#
# Usage: ./scripts/qa-mpart.sh
# Env overrides:
#   YDBGO_BIN     built binary path (default ./bin/ydbgo)
#   QA_WORKDIR    base dir for node data dirs (default /tmp; use /dev/shm to
#                 keep node disks in RAM per the README's 5-node RF=3 protocol)
#   QA_STATEMENTS bench statements (default 2000)
#   QA_ROWS       rows per INSERT statement (default 500; 2000x500 = 1M rows)
#   QA_CONC       bench concurrency (default 8)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${YDBGO_BIN:-$ROOT/bin/ydbgo}"
STMT="${QA_STATEMENTS:-2000}"
ROWS="${QA_ROWS:-500}"
CONC="${QA_CONC:-8}"
RF=3

# Ports deliberately outside the demo range (2135/7001/8080) so the script can
# run while the demo cluster is up.
WORK=$(mktemp -d "${QA_WORKDIR:-/tmp}/ydbgo-qa-mpart.XXXXXX")
PIDS=()
cleanup() {
  if [ "${QA_KEEP:-0}" = "1" ]; then
    echo "  (QA_KEEP=1: leaving $WORK up; kill nodes with:)"
    for pid in "${PIDS[@]}"; do echo "    kill -9 $pid"; done
    return
  fi
  for pid in "${PIDS[@]}"; do
    kill -9 "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/cluster.yaml" <<YAML
config:
  hosts:
  - {host: 127.0.0.1, grpc: 2235, raft: 7101, data: $WORK/n1, id: n1, bootstrap: true}
  - {host: 127.0.0.1, grpc: 2236, raft: 7102, data: $WORK/n2, id: n2}
  - {host: 127.0.0.1, grpc: 2237, raft: 7103, data: $WORK/n3, id: n3}
  - {host: 127.0.0.1, grpc: 2238, raft: 7104, data: $WORK/n4, id: n4}
  - {host: 127.0.0.1, grpc: 2239, raft: 7105, data: $WORK/n5, id: n5}
YAML

echo "[build] $BIN"
mkdir -p "$ROOT/bin"
go build -o "$BIN" "$ROOT/cmd/ydbgo"

say() { echo "== $*"; }
fail() { echo "  FAIL: $*"; exit 1; }

wait_http() { # wait_http PORT
  for _ in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:$1/api/v1/health" > /dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

say "start 5-node cluster from generated config (RF=$RF)"
for i in 1 2 3 4 5; do
  HTTP=$((18079 + i))
  "$BIN" serve -config "$WORK/cluster.yaml" -node-id "n$i" -rf "$RF" -http ":$HTTP" > "$WORK/n$i.log" 2>&1 &
  PIDS+=("$!")
done
wait_http 18080 || fail "n1 did not become healthy"
echo "  ok: n1 healthy on :18080"

say "wait for all 5 nodes to register"
for _ in $(seq 1 60); do
  N=$(curl -s "http://127.0.0.1:18080/api/v1/nodes" | grep -o '"id"' | wc -l | tr -d ' ')
  [ "$N" = "5" ] && break
  sleep 1
done
[ "$N" = "5" ] || fail "only $N/5 nodes registered"
echo "  ok: 5 nodes registered"

ADDR="127.0.0.1:2235"
RUN() { "$BIN" run -addr "$ADDR" "$@"; }

# ms_since returns elapsed milliseconds since a date +%s%N timestamp.
ms_since() {
  local now diff
  now=$(date +%s%N)
  diff=$(( (now - $1) / 1000000 ))
  echo "$diff"
}

bench_load() { # bench_load ENGINE TABLE
  local eng=$1 tbl=$2 t0
  say "load ${ROWS}x${STMT}=$((ROWS*STMT)) rows into ENGINE=$eng (RF=$RF, conc=$CONC)"
  t0=$(date +%s%N)
  "$BIN" bench -addr "$ADDR" -table "$tbl" -engine "$eng" -n "$STMT" -rows "$ROWS" -c "$CONC"
  echo "  load wall time: $(ms_since "$t0") ms"
}

columnar_probe() { # columnar_probe TABLE
  local tbl=$1 t0
  say "columnar reads on $tbl"
  t0=$(date +%s%N); CNT=$(RUN "SELECT COUNT(*) AS c FROM $tbl" | grep -Eo '[0-9]+' | head -1); echo "  COUNT(*) = $CNT in $(ms_since "$t0") ms"
  t0=$(date +%s%N); SUM=$(RUN "SELECT SUM(id) AS s FROM $tbl" | grep -Eo '[0-9]+' | head -1); echo "  SUM(id)   = $SUM in $(ms_since "$t0") ms"
  t0=$(date +%s%N); RUN "SELECT g, COUNT(*) AS c, SUM(id) AS s FROM $tbl GROUP BY g" > /dev/null; echo "  GROUP BY g (100 groups) in $(ms_since "$t0") ms"
  t0=$(date +%s%N); RUN "SELECT id FROM $tbl ORDER BY id DESC LIMIT 10" > /dev/null; echo "  ORDER BY id DESC LIMIT 10 in $(ms_since "$t0") ms"
  t0=$(date +%s%N); RUN "SELECT v FROM $tbl WHERE id = 42" > /dev/null; echo "  point get id=42 in $(ms_since "$t0") ms"
  if [ "$CNT" != "$((ROWS*STMT))" ]; then fail "$tbl COUNT=$CNT, want $((ROWS*STMT))"; fi
}

say "--- A: CSTORE (MVCC column-major) ---"
bench_load "CSTORE" "ab_cstore"
columnar_probe "ab_cstore"

say "--- B: CSTORE2 (native mpart) ---"
bench_load "CSTORE2" "ab_cstore2"
# ClickHouse's bench runs SYSTEM SYNC REPLICA + sleep 2 after load so its
# background merges converge before probing; give our idle merge the same.
sleep 2
columnar_probe "ab_cstore2"

say "on-disk footprint per node (engine dirs)"
du -sh "$WORK/n1"/*/engine_mpart "$WORK/n1"/*/ 2>/dev/null | grep -Ei "cstore|engine|ydb" || true
RUN "ADMIN SHARDS ab_cstore" | head -20
RUN "ADMIN SHARDS ab_cstore2" | head -20

echo
echo "RESULT: OK — see throughput/latency lines above for the CSTORE vs CSTORE2 comparison"
