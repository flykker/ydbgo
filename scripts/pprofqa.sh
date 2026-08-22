#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/ydbgo"
WORK=$(mktemp -d /dev/shm/pprofqa.XXXXXX)
PIDS=()
cleanup() { for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done; }
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
for i in 1 2 3 4 5; do
  HTTP=$((18089 + i))
  PPROF=$((1606 + i))
  "$BIN" serve -config "$WORK/cluster.yaml" -node-id "n$i" -rf 3 -http ":$HTTP" -pprof ":$PPROF" > "$WORK/n$i.log" 2>&1 &
  PIDS+=("$!")
done
for i in $(seq 1 40); do curl -sf "http://127.0.0.1:18090/api/v1/health" > /dev/null && break; sleep 1; done
sleep 3
echo "WORK=$WORK" > /tmp/pprofqa.env
echo "READY"