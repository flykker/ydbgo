#!/usr/bin/env bash
set -euo pipefail
# ClickHouse comparison for the ydbgo columnar A/B: same workload as
# scripts/qa-mpart.sh (2000x500 = 1M rows, 8 concurrent writers) against a
# 5-node ClickHouse deployment: 3 data replicas (ch1-ch3) + 2 extra nodes in
# the cluster (ch4 = clickhouse-keeper, ch5 = spare server). All data dirs on
# RAM (docker tmpfs) so the single machine's disk never matters.
#
# Usage: ./scripts/qa-clickhouse.sh
# Env overrides: QA_STATEMENTS (default 2000), QA_ROWS (default 500),
#                QA_CONC (default 8)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STMT="${QA_STATEMENTS:-2000}"
ROWS="${QA_ROWS:-500}"
CONC="${QA_CONC:-8}"
IMAGE="clickhouse/clickhouse-server:24.3"
NET="chqa"
WORK=$(mktemp -d "${QA_WORKDIR:-/tmp}/ydbgo-qa-ch.XXXXXX")
PIDS=()
CONTAINERS=()
cleanup() {
  for c in "${CONTAINERS[@]:-}"; do
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
  docker network rm "$NET" >/dev/null 2>&1 || true
  [ "${QA_KEEP:-0}" = "1" ] && echo "  (QA_KEEP=1: configs left in $WORK)" || rm -rf "$WORK"
}
trap cleanup EXIT

say() { echo "== $*"; }
fail() { echo "  FAIL: $*"; exit 1; }

docker network create "$NET" >/dev/null

# --- keeper (ch4): single-node, but listen on all interfaces so the data
# --- replicas (ch1-3) can reach it over the docker network ---
cat > "$WORK/keeper.xml" <<XML
<clickhouse>
  <logger><level>warning</level></logger>
  <listen_host>0.0.0.0</listen_host>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/logs</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <coordination_settings><raft_logs_level>warning</raft_logs_level></coordination_settings>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>ch4</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>
</clickhouse>
XML

# --- shared server overlay for ch1..ch5 ---
cat > "$WORK/qa.xml" <<XML
<clickhouse>
  <remote_servers>
    <qa5>
      <shard>
        <replica><host>ch1</host><port>9000</port></replica>
        <replica><host>ch2</host><port>9000</port></replica>
        <replica><host>ch3</host><port>9000</port></replica>
      </shard>
    </qa5>
  </remote_servers>
  <zookeeper><node><host>ch4</host><port>9181</port></node></zookeeper>
  <listen_host>0.0.0.0</listen_host>
  <logger><level>warning</level><log>/var/log/clickhouse-server/clickhouse-server.log</log></logger>
  <profiles><default><max_memory_usage>6000000000</max_memory_usage></default></profiles>
</clickhouse>
XML

start_server() { # start_server NAME REPLICA HTTP HOSTPORT
  local name=$1 replica=$2 httpport=$3 hostport=$4
  mkdir -p "$WORK/$name/config.d" "$WORK/$name/users.d"
  cp "$WORK/qa.xml" "$WORK/$name/config.d/qa.xml"
  cat > "$WORK/$name/users.d/qa.xml" <<XML
<clickhouse>
  <users>
    <default>
      <password></password>
      <networks><ip>::/0</ip></networks>
    </default>
  </users>
</clickhouse>
XML
  if [ "$replica" != "none" ]; then
    cat > "$WORK/$name/config.d/macros.xml" <<XML
<clickhouse><macros><shard>s1</shard><replica>$replica</replica></macros></clickhouse>
XML
  fi
  docker run -d --name "$name" --network "$NET" \
    -p "$hostport:$httpport" \
    --tmpfs /var/lib/clickhouse:rw,size=4g,mode=1777 \
    --tmpfs /var/log/clickhouse-server:rw,size=512m \
    -v "$WORK/$name/config.d:/etc/clickhouse-server/config.d:ro" \
    -v "$WORK/$name/users.d:/etc/clickhouse-server/users.d:ro" \
    "$IMAGE" >/dev/null
  CONTAINERS+=("$name")
}

say "start 5-node ClickHouse cluster: replicas ch1-3, keeper ch4, spare ch5"
start_server ch1 r1 8123 18123
start_server ch2 r2 8124 18124
start_server ch3 r3 8125 18125
start_server ch5 none 8126 18126

docker run -d --name ch4 --network "$NET" -p 19181:9181 \
  --tmpfs /var/lib/clickhouse:rw,size=512m \
  -v "$WORK/keeper.xml:/etc/clickhouse-keeper/config.xml:ro" \
  "$IMAGE" clickhouse-keeper --config-file=/etc/clickhouse-keeper/config.xml >/dev/null
CONTAINERS+=("ch4")

chq() { # chq NODE SQL  (run a query on a server container)
  docker exec "$1" clickhouse-client -q "$2"
}
chq_wait() { # wait until NODE answers
  for _ in $(seq 1 60); do
    if docker exec "$1" clickhouse-client -q "SELECT 1" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}
keeper_wait() { # wait until the keeper accepts TCP connections
  for _ in $(seq 1 60); do
    if docker exec ch4 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9181' 2>/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

keeper_wait || { echo "  FAIL: ch4 keeper not ready"; docker logs ch4 2>&1 | tail -10; exit 1; }
for c in ch1 ch2 ch3 ch5; do
  chq_wait "$c" || { echo "  FAIL: $c not ready"; docker logs "$c" 2>&1 | tail -10; exit 1; }
done
echo "  ok: all 5 nodes up"

say "create replicated table (RF=3) + Distributed view"
for c in ch1 ch2 ch3; do
  chq "$c" "CREATE DATABASE IF NOT EXISTS qa5"
  chq "$c" "CREATE TABLE IF NOT EXISTS qa5.t (id Int64, v String, g Int64) ENGINE=ReplicatedMergeTree('/qa5/s1/t','$c') ORDER BY id"
done
chq ch1 "CREATE TABLE IF NOT EXISTS qa5.t_all AS qa5.t ENGINE=Distributed(qa5, qa5, t, id)"

# --- load: same workload as the ydbgo bench (STMT statements x ROWS rows, CONC writers) ---
say "load $((STMT*ROWS)) rows into ClickHouse ReplicatedMergeTree (conc=$CONC)"
# each parallel worker keeps ONE persistent clickhouse-client session (like the
# ydbgo bench's connection pool) and streams its share of INSERT statements.
# Worker w owns statements w, w+CONC, ... each inserting its id window
# [base, base+ROWS) server-side with a single statement.
LOAD_START=$(date +%s%N)
for w in $(seq 0 $((CONC-1))); do
  (
    for s in $(seq "$w" "$CONC" $((STMT-1))); do
      base=$((s * ROWS))
      echo "INSERT INTO qa5.t SELECT number + $base, concat('v', toString(number + $base)), (number + $base) % 100 FROM numbers($ROWS);"
    done
    ) | docker exec -i ch1 clickhouse-client -n &
done
wait
LOAD_ELAPSED=$(($(date +%s%N) - LOAD_START))
echo "  clickhouse load wall time: $((LOAD_ELAPSED/1000000)) ms"

say "wait for replication to converge on all 3 replicas"
for c in ch1 ch2 ch3; do
  docker exec "$c" clickhouse-client -q "SYSTEM SYNC REPLICA qa5.t" || true
done
sleep 2

run_ch() { # run_ch SQL  (HTTP endpoint: no per-query client-process spawn)
  curl -s --data-binary "$1" "http://127.0.0.1:18123/"
}
ch_null() { # run a query discarding the body (timing-only)
  curl -s --data-binary "$1" "http://127.0.0.1:18123/" -o /dev/null
}
ms() { # ms START_NS
  echo $(( ($(date +%s%N) - $1) / 1000000 ))
}

say "columnar reads via Distributed table (single entry point ch1, HTTP)"
t0=$(date +%s%N); CNT=$(run_ch "SELECT COUNT(*) FROM qa5.t_all" | tr -d ' \n'); echo "  COUNT(*) = $CNT in $(ms "$t0") ms"
t0=$(date +%s%N); SUM=$(run_ch "SELECT SUM(id) FROM qa5.t_all" | tr -d ' \n'); echo "  SUM(id)   = $SUM in $(ms "$t0") ms"
t0=$(date +%s%N); ch_null "SELECT g, COUNT(*), SUM(id) FROM qa5.t_all GROUP BY g FORMAT Null"; echo "  GROUP BY g (100 groups) in $(ms "$t0") ms"
t0=$(date +%s%N); ch_null "SELECT id FROM qa5.t_all ORDER BY id DESC LIMIT 10 FORMAT Null"; echo "  ORDER BY id DESC LIMIT 10 in $(ms "$t0") ms"
t0=$(date +%s%N); ch_null "SELECT v FROM qa5.t_all WHERE id = 42"; echo "  point get id=42 in $(ms "$t0") ms"

if [ "$CNT" != "$((STMT*ROWS))" ]; then fail "ClickHouse COUNT=$CNT, want $((STMT*ROWS))"; fi

say "UPDATE/DELETE mutations via replica ch1 (compare with updel bench; Distributed does not support mutations)"
MUT_START=$(date +%s%N)
run_ch "ALTER TABLE qa5.t UPDATE g = 7 WHERE id = 42 SETTINGS mutations_sync = 1"
echo "  UPDATE point id=42 (1 row) in $(ms "$MUT_START") ms"
MUT_START=$(date +%s%N)
run_ch "ALTER TABLE qa5.t UPDATE g = 7 WHERE id >= 0 AND id < 100000 SETTINGS mutations_sync = 1"
echo "  UPDATE range id<100000 (100k rows) in $(ms "$MUT_START") ms"
MUT_START=$(date +%s%N)
run_ch "ALTER TABLE qa5.t DELETE WHERE id >= 0 AND id < 100000 SETTINGS mutations_sync = 1"
echo "  DELETE range id<100000 (100k rows) in $(ms "$MUT_START") ms"
MUT_START=$(date +%s%N)
run_ch "ALTER TABLE qa5.t DELETE WHERE id = 42 SETTINGS mutations_sync = 1"
echo "  DELETE point id=42 in $(ms "$MUT_START") ms"

say "on-disk footprint per replica node (RAM)"
docker exec ch1 sh -c 'du -sh /var/lib/clickhouse/data/qa5/t' 2>/dev/null || true

echo
echo "RESULT: OK — ClickHouse 5-node RF=3 numbers above (compare with qa-mpart.sh)"
