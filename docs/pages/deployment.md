# Deployment

`harness/docker` is the image. `local`, `aws`, and `fly` are how to run it. Each process is one node: raft, metadata, and the storage engine in one JVM. Durable Streams and S2 are protocol facades over that engine. Pick one per deployment.

Storage comes from the topo file (`TOPO`). `DATA_BUCKET` / `WAL_BUCKET` / `BUCKET_NAME` override the topo S3 URIs.

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

## AWS

Create data and WAL buckets. Copy `harness/aws/.env.example` to `harness/aws/.env` and fill `AWS_*`, `DATA_BUCKET`, and `WAL_BUCKET`.

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run the S2 facade. The node listens on `127.0.0.1:4437`.

Three nodes on one host, ports `4437-4439`:

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

## Fly.io

App name must stay `streamstack`. Storage is Tigris.

```bash
fly launch --config harness/fly/fly.toml
fly storage create
fly deploy --config harness/fly/fly.toml
```

Three nodes: `--config harness/fly/fly.cluster.toml`.

## Topology

| File | Storage |
|------|---------|
| `local/topo.yaml` | MinIO via `host.docker.internal:9000` (container to host) |
| `local/topo.cluster.yaml` | Three nodes. MinIO via `127.0.0.1:9000` (host network / JVM) |
| `aws/topo.yaml` | Real `s3://` (no endpoint) |
| `aws/topo.cluster.yaml` | Three nodes on one host. Real `s3://` |
| `fly/topo.yaml` | One Fly Machine. Tigris (`region=auto`) |
| `fly/topo.cluster.yaml` | Three Fly process groups `n1-n3` |

URI forms: `0@s3://bucket?region=us-east-1`, `-2@file:///path`, `0@mem://bucket` (tests). Omit `global.wal` to default WAL to the storage URI when storage is S3.

`--topo` requires `--node-id`. CLI flags override topo values. Export credentials in the environment that starts the process.
