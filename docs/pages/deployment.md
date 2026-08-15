# Deployment

Docker image: `harness/docker`. Deploy `StreamStack` on `local`, [aws](/deployment/aws), or [fly](/deployment/fly). Each node: raft, metadata, and the storage engine in one JVM. [Durable Streams](/protocol/durable-streams) and [S2](/protocol/s2) are protocol facades over the same engine. Pick one per deployment.

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

Admin plane, health checks, and metadata restore: [Operations](/operations).