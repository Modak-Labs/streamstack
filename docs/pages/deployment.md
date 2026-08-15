# Deployment

Docker image: `harness/docker`. Deploy `StreamStack` on `local`, [aws](/deployment/aws), or [fly](/deployment/fly). Each node: raft, metadata, and the storage engine in one JVM. [Durable Streams](https://github.com/durable-streams/durable-streams) and [S2](https://github.com/s2-streamstore/s2) are protocol facades over the same engine. Pick one per deployment.

Storage configuration from the topo file (`TOPO`). `DATA_BUCKET` / `WAL_BUCKET` / `BUCKET_NAME` override the topo S3 URIs.

## Local cluster

Three DS nodes on one host, ports `4437-4439` / `8091-8093`, against local MinIO.

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.cluster.ds.yml \
  up -d --build
```

Each service is a fixed `NODE_ID` (1 / 2 / 3). Host network so they bind the ports in `local/topo.cluster.yaml`.

Bare JVM, same topo, one process per id:

```bash
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 1
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 2
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 3
```

## Topology

| File | Storage |
|------|---------|
| `local/topo.yaml` | MinIO via `host.docker.internal:9000` (container to host) |
| `local/topo.cluster.yaml` | Three nodes. MinIO via `127.0.0.1:9000` (host network / JVM) |

URI forms: `0@s3://bucket?region=us-east-1`, `-2@file:///path`, `0@mem://bucket` (for tests only). Omit `global.wal` to default WAL to the storage URI when storage is S3.

`--topo` requires `--node-id`. CLI flags override topo values and export credentials in the environment that starts the process.

## Admin and health

Every node serves an admin plane on a separate port, independent of the protocol facade. Default port is `9090`; set it per node with `adminPort` in the topo, `--admin-port` on the CLI, or `ADMIN_PORT` in the Docker image. Set `0` to disable.

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness. `200 ok` as soon as the process is up, before raft and storage are ready |
| `GET /ready` | Readiness. `200` once raft has a leader, the node is registered, and storage started; `503` otherwise |
| `GET /admin/cluster` | Cluster and raft status: leader, applied index, stream and object counts |
| `GET /admin/nodes` | Registered nodes with advertised addresses and epochs |
| `GET /admin/streams/{name}` | Owner and metadata for a stream |
| `POST /admin/peers` | Add a raft peer: `{"peer": "host:port"}` (leader only) |
| `DELETE /admin/peers/{peer}` | Remove a raft peer (leader only) |
| `POST /admin/transfer-leader` | Transfer raft leadership: `{"peer": "host:port"}` (leader only) |

The admin port also serves a read-only dashboard at `/` with cluster, node, and raft status.

Point load balancer target-group health checks at `/ready`. On shutdown the node flips `/ready` to `503` first and waits `shutdownDrainSec` (topo config, default `0`) before closing, so the load balancer drains in-flight traffic.

At startup the node verifies the storage and WAL buckets are reachable, retrying with backoff for up to ten attempts before failing; `/health` responds while it retries.