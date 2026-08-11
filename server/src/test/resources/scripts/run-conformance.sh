#!/usr/bin/env bash
set -euo pipefail

# Run Durable Streams server conformance tests against a local streamstack server.
# Usage:
#   ./src/test/resources/scripts/run-conformance.sh [httpPort] [raftPort]

SERVER_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
STACK_ROOT="$(cd "${SERVER_ROOT}/.." && pwd)"
HTTP_PORT="${1:-4437}"
RAFT_PORT="${2:-18091}"
BASE_URL="http://127.0.0.1:${HTTP_PORT}"
DATA="$(mktemp -d /tmp/streamstack-conf-XXXXXX)"
WORK="$(mktemp -d /tmp/ds-conf-XXXXXX)"

free_ports() {
  lsof -tiTCP:"${HTTP_PORT}" -sTCP:LISTEN | xargs kill -9 2>/dev/null || true
  lsof -tiTCP:"${RAFT_PORT}" -sTCP:LISTEN | xargs kill -9 2>/dev/null || true
}

cleanup() {
  if [[ -f "${DATA}/server.pid" ]]; then
    kill "$(cat "${DATA}/server.pid")" 2>/dev/null || true
    wait "$(cat "${DATA}/server.pid")" 2>/dev/null || true
  fi
  free_ports
}
trap cleanup EXIT

free_ports

mkdir -p "${DATA}/data" "${DATA}/objects"
cd "${STACK_ROOT}"
mvn -pl server -am -q -DskipTests package
mvn -pl server -q org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.args="--node-id 1 --node-epoch 1 --http-host 127.0.0.1 --http-port ${HTTP_PORT} --raft-host 127.0.0.1 --raft-port ${RAFT_PORT} --data-dir ${DATA}/data --object-dir ${DATA}/objects --routing LOCAL_ALWAYS" \
  >"${DATA}/server.log" 2>&1 &
echo $! >"${DATA}/server.pid"

echo "Waiting for server at ${BASE_URL} (log: ${DATA}/server.log)"
READY=0
for _ in $(seq 1 90); do
  if curl -sf -o /dev/null -X PUT -H 'Content-Type: text/plain' "${BASE_URL}/streams/healthz"; then
    echo "Server ready"
    READY=1
    break
  fi
  if ! kill -0 "$(cat "${DATA}/server.pid")" 2>/dev/null; then
    echo "Server process exited early:" >&2
    cat "${DATA}/server.log" >&2
    exit 1
  fi
  sleep 1
done
if [[ "${READY}" -ne 1 ]]; then
  echo "Server failed to become ready within 90s:" >&2
  cat "${DATA}/server.log" >&2
  exit 1
fi

cd "${WORK}"
npm init -y >/dev/null 2>&1
npm pkg set type=module >/dev/null
npm install --silent @durable-streams/server-conformance-tests@0.3.6 vitest@4
cat > runner.test.js <<'EOF'
import { runConformanceTests } from '@durable-streams/server-conformance-tests'
const baseUrl = process.env.CONFORMANCE_TEST_URL
if (!baseUrl) throw new Error('missing CONFORMANCE_TEST_URL')
runConformanceTests({ baseUrl })
EOF
cat > vitest.config.js <<'EOF'
import { defineConfig } from 'vitest/config'
export default defineConfig({
  test: {
    include: ['runner.test.js'],
    fileParallelism: false,
    testTimeout: 120000,
    hookTimeout: 120000,
    reporters: ['verbose'],
  },
})
EOF

echo "Running conformance against ${BASE_URL}"
CONFORMANCE_TEST_URL="${BASE_URL}" npx vitest run --reporter=verbose
