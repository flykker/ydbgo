#!/usr/bin/env bash
set -euo pipefail
# Re-seeds the demo data used by the web console against a running server.
# Usage: ./scripts/reseed-demo.sh [sql-addr] [http-url]
#   sql-addr — gRPC SQL-address of any node (default 127.0.0.1:2135)
#   http-url — HTTP URL of the console (default http://localhost:8080)
# Binary used for bulk INSERT (default ./bin/ydbgo, override with $YDBGO).
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
A=${1:-127.0.0.1:2135}
H=${2:-http://localhost:8080}
YDBGO=${YDBGO:-"$ROOT/bin/ydbgo"}
q() { curl -s -X POST "$H/api/v1/query" -d "{\"sql\":\"$1\"}" >/dev/null; }

q "DROP TABLE IF EXISTS logs"
q "CREATE TABLE logs (ts timestamp primary key, level string, msg string, lat double) ENGINE=CSTORE"

python3 - "$A" "$YDBGO" <<'PY'
import os, subprocess, sys, time
addr, ydbgo = sys.argv[1], sys.argv[2]
rows = []
t = time.time()
base = int(t // 3600) * 3600
for i in range(300):
    rows.append([base + i * 60, 'INFO' if i % 10 else 'ERROR' if i % 5 == 0 else 'WARN', 'log line %d' % i, round(0.5 + (i % 7) * 0.7, 2)])
def ts(v):
    import datetime
    return datetime.datetime.fromtimestamp(v, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
batch = []
for r in rows:
    batch.append("('%s','%s','%s',%s)" % (ts(r[0]), r[1], r[2].replace("'", "''"), r[3]))
    if len(batch) == 50:
        sql = "INSERT INTO logs VALUES " + ",".join(batch)
        subprocess.run([ydbgo, 'run', '-addr', addr, sql], check=True, capture_output=True)
        batch = []
if batch:
    sql = "INSERT INTO logs VALUES " + ",".join(batch)
    subprocess.run([ydbgo, 'run', '-addr', addr, sql], check=True, capture_output=True)
print("seeded logs")
PY

DASH_CONFIG='{"title":"Cluster overview","refresh_interval":30,"widgets":[{"id":"w1","type":"stat","title":"Total logs","sql":"SELECT COUNT(*) AS total FROM logs","x":0,"y":0,"w":3,"h":4},{"id":"w2","type":"stat","title":"Errors","sql":"SELECT COUNT(*) AS total FROM logs WHERE level = '\''ERROR'\''","x":3,"y":0,"w":3,"h":4},{"id":"w3","type":"line","title":"Logs per 5m","sql":"SELECT time_bucket('\''5m'\'', ts) AS t, COUNT(*) AS n FROM logs GROUP BY 1","x":6,"y":0,"w":6,"h":6},{"id":"w4","type":"pie","title":"Levels","sql":"SELECT level, COUNT(*) FROM logs GROUP BY 1","x":0,"y":4,"w":3,"h":6},{"id":"w5","type":"log_viewer","title":"Recent","sql":"SELECT ts, level, msg FROM logs ORDER BY ts DESC LIMIT 20","x":3,"y":6,"w":6,"h":6},{"id":"w6","type":"gauge","title":"Danger gauge","sql":"SELECT 42 AS pct","x":9,"y":6,"w":3,"h":4}]}'

# remove old demo dashboards, create a fresh one
curl -s "$H/api/v1/dashboards" | python3 -c '
import json,sys
for d in json.load(sys.stdin)["dashboards"]:
    print(d["id"])
' | while read -r id; do
  curl -s -X DELETE "$H/api/v1/dashboards/$id" >/dev/null
done

curl -s -X POST "$H/api/v1/dashboards" -d "{\"name\":\"Cluster overview\",\"config\":$DASH_CONFIG}"
echo
echo "demo reseeded"
