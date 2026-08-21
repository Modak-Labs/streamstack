#!/usr/bin/env bash
# Run `pico bench` over a set of configurations, one fresh server each.
#
# Each row starts a server on its own ports and storage, waits for /ready,
# benches, then stops it, so runs cannot contaminate each other. Storage
# defaults to local files; point --storage/--wal at MinIO or S3 to measure
# object storage.
#
# usage: scripts/sweep.sh [duration_sec] [storage_uri_prefix]
set -uo pipefail

DURATION="${1:-10}"
BIN="${PICO_BIN:-$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/release/pico}"
PORT_BASE="${PORT_BASE:-4600}"

# record_size in_flight batch wal_batch_ms streams connections
CASES=(
  "1024 128 1 5 1 1"
  "1024 256 1 5 1 1"
  "1024 512 1 5 1 1"
  "1024 1024 1 5 1 1"
  "1024 2048 1 5 4 1"
  "1024 512 10 5 1 1"
  "65536 256 1 5 1 1"
)

printf '%-9s %-9s %-6s %-9s %-8s %-6s %-12s %-11s %-9s %-9s\n' \
  rec_size in_flight batch batch_ms streams conns write_MiB/s rec/s p50_ms p99_ms

i=0
for case in "${CASES[@]}"; do
  read -r size flight batch waln streams conns <<<"$case"
  i=$((i + 1))
  http=$((PORT_BASE + i * 2))
  admin=$((http + 1))
  dir=$(mktemp -d)

  "$BIN" serve \
    --listen "127.0.0.1:$http" --admin-listen "127.0.0.1:$admin" \
    --meta-url "sqlite:$dir/meta.db" \
    --storage "1@file://$dir/objects" \
    --wal "2@file://$dir/wal?batchInterval=$waln" \
    --wal-upload-interval-ms 200 \
    >"$dir/server.log" 2>&1 &
  server=$!

  for _ in $(seq 100); do
    curl -sf "http://127.0.0.1:$admin/ready" >/dev/null && break
    sleep 0.3
  done

  json=$("$BIN" --endpoint "http://127.0.0.1:$http" ${PICO_HTTP2:+--http2} bench \
    --output json -d "$DURATION" -b "$size" -w "$flight" -n "$batch" \
    --streams "$streams" --connections "$conns" --no-read 2>/dev/null)

  kill "$server" 2>/dev/null
  wait "$server" 2>/dev/null
  rm -rf "$dir"

  echo "$json" | python3 -c '
import json, sys
args = sys.argv[1:]
w = json.load(sys.stdin)["write"]
print("%-9s %-9s %-6s %-8s %-8s %-6s %-12.2f %-11.0f %-9.2f %-9.2f" % (
    *args, w["mib_per_sec"], w["records_per_sec"], w["p50_ms"], w["p99_ms"]))
' "$size" "$flight" "$batch" "$waln" "$streams" "$conns"
done
