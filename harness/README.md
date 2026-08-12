# Streamstack harness

The server **is** the node: `DurableStreamsServer` and `S2Server` each embed the full `StreamStackNode` (raft, metadata, storage engine) in one JVM. Running streamstack means running one server process per node. DS and S2 are alternative protocol facades over the same engine — pick one per deployment.

Storage is chosen by the topo file (`TOPO`), not by the facade compose file. Stack compose files with `-f` to combine a node with MinIO.

## Layout

- `configs/` — topo files + env samples
- `deployment/docker/` — Dockerfile + compose files

| Compose file | What it runs |
|--------------|--------------|
| `docker-compose.minio.yml` | MinIO + bucket init |
| `docker-compose.ds.yml` | `node1` running the DS facade |
| `docker-compose.s2.yml` | `node1` running the S2 facade |

## MinIO

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/deployment/docker/docker-compose.minio.yml \
  -f harness/deployment/docker/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2 instead. Node listens on `127.0.0.1:4437`. MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`).

Bare JVM:

```bash
docker compose -f harness/deployment/docker/docker-compose.minio.yml up -d

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

mvn -pl frontend/ds/server -am package -DskipTests
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.minio.yaml --node-id 1
```

Swap `frontend/ds/server` / `ds-server.jar` for `frontend/s2/server` / `s2-server.jar` to run S2.

## AWS S3

1. Create data + WAL buckets.
2. Edit `configs/topo.aws.yaml` bucket names / region.
3. `cp harness/configs/.env.example harness/configs/.env` and fill `AWS_*`.

```bash
docker compose --env-file harness/configs/.env \
  -f harness/deployment/docker/docker-compose.ds.yml \
  up -d --build
```

Or bare JVM (creds from env / `~/.aws` / IAM):

```bash
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.aws.yaml --node-id 1
```

## Multi-node

One process per `nodeId`, same topo file, same buckets:

```bash
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 1
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 2
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 3
```

Peers are derived from the topo's `nodes` list. `topo.cluster.yaml` defaults to MinIO storage; swap the URIs for AWS.

## Topology notes

| File | Storage |
|------|---------|
| `topo.minio.yaml` | S3-compatible MinIO (`endpoint` + `pathStyle`) |
| `topo.aws.yaml` | Real `s3://` (no endpoint) |
| `topo.cluster.yaml` | Three nodes; MinIO by default |

URI forms: `0@s3://bucket?region=…`, `-2@file:///path`, `0@mem://bucket` (tests).  
Omit `global.wal` to default WAL to the storage URI when storage is S3.

`--topo` requires `--node-id`. CLI flags override topo values. Topo `global.envs` are informational — export credentials in the environment that starts the process.

## Clean up

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/deployment/docker/docker-compose.minio.yml \
  -f harness/deployment/docker/docker-compose.ds.yml \
  down -v
```
