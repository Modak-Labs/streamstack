# Deploying StreamStack

## Prerequisites
- Java 17 minimum; **Java 21+ recommended** (virtual threads auto-enable on 21+ for long-poll/SSE concurrency)
- Maven 3.9+
- Object storage: S3-compatible (MinIO, AWS S3, etc.) for production
- Standard AWS credentials via `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, `~/.aws/credentials`, or IAM role
  (URI query params `accessKey` / `secretKey` also work)

## Topology schema (`topo.yaml`)

Optional. You can pass the same settings as CLI flags (`--storage-uri`, `--http-port`, …) instead of a file.

```yaml
global:
  clusterName: poc
  storage: 0@s3://streams-data?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true
  wal: 0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true
  config:
    walCacheSize: 209715200
    blockCacheSize: 104857600
    routing: REDIRECT
  envs:
    - name: AWS_ACCESS_KEY_ID
      value: minioadmin
    - name: AWS_SECRET_ACCESS_KEY
      value: minioadmin
nodes:
  - host: 127.0.0.1
    nodeId: 1
    httpPort: 4437
    raftPort: 8091
    dataDir: /var/streamstack/poc/n1
```

### URI formats
| Protocol | Example | Use |
|----------|---------|-----|
| `s3://` | `0@s3://bucket?region=us-east-1&endpoint=http://minio:9000&pathStyle=true` | Production / MinIO |
| `file://` | `-2@file:///var/streamstack/objects` | Local object-store shim |
| `mem://` | `0@mem://bucket` | Tests only |

WAL: omit `global.wal` to default to **object WAL on the storage URI when storage is S3**, otherwise `memory`. Explicit values: `memory`, `0@s3://...`, `0@file://...`.

### `global.config` keys

Server knobs:
| Key | Default | Notes |
|-----|---------|-------|
| `routing` | `REDIRECT` | `REDIRECT` or `LOCAL_ALWAYS` |
| `longPollTimeoutSec` | `25` | |
| `sseMaxDurationSec` | `55` | |
| `maxChunkSize` | `65536` | |

Stream knobs (omit = engine default):
| Key | Default | Notes |
|-----|---------|-------|
| `walCacheSize` | 200 MiB | Log cache size for hot / tail reads |
| `walUploadThreshold` | 100 MiB | Bytes buffered in WAL cache before uploading to object storage |
| `walUploadIntervalMs` | -1 (disabled) | Periodic WAL upload interval; `-1` disables time-based flush |
| `blockCacheSize` | 100 MiB | Block cache size for cold / catch-up reads |
| `splitSize` | 16 MiB | Threshold to split a stream out of a stream-set object into its own stream object |
| `objectBlockSize` | 1 MiB | Read/write block size for object I/O |
| `objectPartSize` | 16 MiB | Multipart upload part size |
| `objectCompaction.intervalMinutes` | 60 | How often stream-object compaction runs |
| `objectCompaction.maxSizeBytes` | 10 GiB | Max size of a compacted stream object |
| `streamSetCompaction.interval` | 5 (min) | How often stream-set object compaction runs |
| `streamSetCompaction.cacheSize` | 200 MiB | Memory budget for one stream-set compaction pass |
| `streamSetCompaction.uploadConcurrency` | 8 | Parallel uploads during stream-set compaction |
| `streamSetCompaction.splitSize` | 8 MiB | Per-stream size that forces a split during stream-set compaction |
| `streamSetCompaction.forceSplitPeriod` | 120 | Minutes before a stream is force-split from a stream-set object |
| `streamSetCompaction.maxObjectNum` | 500 | Max stream-set objects compacted in one pass |
| `maxStreamsPerSetObject` | 20000 | Max streams packed into one stream-set object |
| `maxObjectsPerCommit` | 10000 | Max stream objects committed in one metadata batch |
| `networkBaselineBandwidth` | 1 GiB/s | Assumed NIC baseline used for bandwidth throttling |
| `networkBandwidthMode` | `SEPARATE` | `SEPARATE` or `SHARED` limiter across traffic classes |
| `refillPeriodMs` | 10 | Token-bucket refill period for network limiting |
| `objectRetentionTimeInSecond` | 600 | Grace period before compacted/deleted objects are removed |
| `allocPolicy` | `POOLED_HEAP` | `POOLED_HEAP` or `POOLED_DIRECT` buffer allocator |

Example nested compaction in topo:

```yaml
config:
  splitSize: 16777216
  objectCompaction:
    intervalMinutes: 60
    maxSizeBytes: 10737418240
  streamSetCompaction:
    interval: 5
    cacheSize: 209715200
```

## Single-node MinIO quickstart

```bash
# 1. Start MinIO and create buckets
docker compose up -d minio minio-init

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# 2. Build
mvn -pl durable-streams/server -am package -DskipTests

# 3. Start node 1 (flags; or pass --topo /path/to/topo.yaml)
mkdir -p /tmp/streamstack/n1
java -cp "durable-streams/server/target/classes:$(mvn -pl durable-streams/server -q -DincludeScope=runtime -DforceStdout dependency:build-classpath)" \
  io.streamstack.server.ds.DurableStreamsServer \
  --node-id 1 \
  --node-epoch 1 \
  --http-host 127.0.0.1 \
  --http-port 4437 \
  --raft-host 127.0.0.1 \
  --raft-port 8091 \
  --data-dir /tmp/streamstack/n1 \
  --storage '0@s3://streams-data?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true' \
  --wal '0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true' \
  --routing LOCAL_ALWAYS
```

Smoke test (skips if MinIO is down):

```bash
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
export STREAMSTACK_S3_ENDPOINT=http://127.0.0.1:9000
mvn -pl durable-streams/server -Dtest=S3MinioIntegrationTest test
```

## Multi-node

Give each process a unique `nodeId`, `httpPort`, `raftPort`, and `dataDir`, and the same storage/WAL URIs. Pass peers with repeated `--peer host:port` (or a shared `topo.yaml` with `--topo` / `--node-id`).

## Stop

```bash
pkill -f 'io.streamstack.server.ds.DurableStreamsServer'
```

## Clean / uninstall

1. Stop all node processes
2. Delete each node `dataDir`
3. Empty data + WAL buckets (or `rm -rf` for `file://`)

## Tuning notes

Size heap/direct memory above `walCacheSize + blockCacheSize` or `ConfigValidator` fails at startup.
With `allocPolicy: POOLED_DIRECT`, ensure `-XX:MaxDirectMemorySize` covers both caches.
Use the stream knobs table above for cache and compaction sizing.

HTTP long-poll and SSE use blocking waits. On Java 21+, Javalin schedules those on virtual threads automatically (`useVirtualThreads`), on Java 17 the same binary falls back to the platform thread pool. Prefer Java 21+ when many concurrent live readers are expected.
