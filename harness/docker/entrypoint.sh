#!/bin/sh
set -eu
set -- --topo "${TOPO:?}" --node-id "${NODE_ID:-1}" --http-host 0.0.0.0
if [ -n "${DATA_BUCKET:-}" ]; then
  set -- "$@" --storage "0@s3://${DATA_BUCKET}?region=${AWS_REGION:-us-east-1}"
fi
if [ -n "${WAL_BUCKET:-}" ]; then
  set -- "$@" --wal "0@s3://${WAL_BUCKET}?region=${AWS_REGION:-us-east-1}"
fi
exec java -jar /opt/streamstack/server.jar "$@"
