#!/bin/sh
set -eu

extra="$*"

set -- --http-host 0.0.0.0

if [ -n "${TOPO:-}" ]; then
  set -- "$@" --topo "$TOPO"
fi
if [ -n "${NODE_ID:-}" ]; then
  set -- "$@" --node-id "$NODE_ID"
fi
if [ -n "${ADMIN_PORT:-}" ]; then
  set -- "$@" --admin-port "$ADMIN_PORT"
fi
if [ -n "${DATA_DIR:-}" ]; then
  set -- "$@" --data-dir "$DATA_DIR"
fi
if [ -n "${ROUTING:-}" ]; then
  set -- "$@" --routing "$ROUTING"
fi
if [ -n "${METADATA_ARCHIVE:-}" ]; then
  set -- "$@" --metadata-archive "$METADATA_ARCHIVE"
fi
if [ -n "${RESTORE_FROM_STORAGE:-}" ]; then
  set -- "$@" --restore-from-storage "$RESTORE_FROM_STORAGE"
fi

bucket="${DATA_BUCKET:-${BUCKET_NAME:-}}"
if [ -n "$bucket" ]; then
  region="${AWS_REGION:-us-east-1}"
  endpoint="${AWS_ENDPOINT_URL_S3:-}"
  endpoint="${endpoint%/}"
  qs="region=${region}"
  if [ -n "$endpoint" ]; then
    qs="${qs}&endpoint=${endpoint}"
  fi
  if [ "${PATH_STYLE:-}" = "true" ]; then
    qs="${qs}&pathStyle=true"
  fi
  wal="${WAL_BUCKET:-$bucket}"
  set -- "$@" --storage "0@s3://${bucket}?${qs}" --wal "0@s3://${wal}?${qs}"
fi

if [ -n "$extra" ]; then
  set -- "$@" $extra
fi

if [ -z "${TOPO:-}" ] && [ -z "$extra" ]; then
  echo "TOPO or command args required" >&2
  exit 1
fi

exec java -jar /opt/streamstack/server.jar "$@"
