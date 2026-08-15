# Streamstack Harness

The server **is** also the node: `DurableStreamsServer` and `S2Server` each embed the full `StreamStackNode` (raft, metadata, storage engine) in one JVM.
Running streamstack means running one server process per node. DS and S2 are alternative protocol facades over the same engine, pick one per deployment.

`harness/docker` is the image. `local`, `aws`, and `fly` are how to run it. Storage is chosen by the topo file (`TOPO`). `DATA_BUCKET` / `WAL_BUCKET` / `BUCKET_NAME` override the topo S3 URIs.

## Local (MinIO)

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2 instead. Node listens on `127.0.0.1:4437`. Admin plane and dashboard: http://127.0.0.1:9090 (`/health`, `/ready`, `/admin/*`).

[BENCH.md](BENCH.md) has DS and S2 smoke/load tests.
MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`).

Bare JVM (`local/topo.yaml` uses `host.docker.internal` for Docker, override to `127.0.0.1` on the host):

```bash
docker compose -f harness/local/docker-compose.minio.yml up -d

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

mvn -pl frontend/ds/server -am package -DskipTests
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.yaml --node-id 1 \
  --storage "0@s3://streams-data?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true" \
  --wal "0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true"
```

Swap `frontend/ds/server` / `ds-server.jar` for `frontend/s2/server` / `s2-server.jar` to run S2.

## AWS S3

1. Create data + WAL buckets.
2. `cp harness/aws/.env.example harness/aws/.env` and fill `AWS_*`, `DATA_BUCKET`, `WAL_BUCKET`.

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2. Three nodes (ports 4437–4439):

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.cluster.ds.yml \
  up -d --build
```

Bare JVM (creds from env / `~/.aws` / IAM):

```bash
java -jar frontend/ds/server/target/ds-server.jar \
  --topo harness/aws/topo.yaml --node-id 1 \
  --storage "0@s3://${DATA_BUCKET}?region=${AWS_REGION}" \
  --wal "0@s3://${WAL_BUCKET}?region=${AWS_REGION}"
```

## Multi-node (local)

`docker-compose.cluster.ds.yml` starts three DS processes against `local/topo.cluster.yaml`. Each service is a fixed `NODE_ID` (1 / 2 / 3). Host network so they bind 4437–4439 / 8091–8093 / admin 9091–9093 as in the topo.

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.cluster.ds.yml \
  up -d --build
```

Bare JVM, same topo, one process per id:

```bash
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 1
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 2
java -jar frontend/ds/server/target/ds-server.jar --topo harness/local/topo.cluster.yaml --node-id 3
```

## Fly.io + Tigris

App name must stay `streamstack`.

```bash
fly launch --config harness/fly/fly.toml
fly storage create
fly deploy --config harness/fly/fly.toml
```

Three nodes: `--config harness/fly/fly.cluster.toml`.

## Topology

| File | Storage |
|------|---------|
| `local/topo.yaml` | MinIO via `host.docker.internal:9000` (container → host) |
| `local/topo.cluster.yaml` | Three nodes; MinIO via `127.0.0.1:9000` (host network / JVM) |
| `aws/topo.yaml` | Real `s3://` (no endpoint) |
| `aws/topo.cluster.yaml` | Three nodes on one host; real `s3://` |
| `fly/topo.yaml` | One Fly Machine; Tigris (`region=auto`) |
| `fly/topo.cluster.yaml` | Three Fly process groups `n1`–`n3` |

URI forms: `0@s3://bucket?region=…`, `-2@file:///path`, `0@mem://bucket` (tests).
Omit `global.wal` to default WAL to the storage URI when storage is S3.

`--topo` requires `--node-id`. CLI flags override topo values. Topo `global.envs` are informational — export credentials in the environment that starts the process.

## Clean up

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  down -v
```
