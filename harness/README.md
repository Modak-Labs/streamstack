# Streamstack Harness

The server **is** also the node: `DurableStreamsServer` and `S2Server` each embed the full `StreamStackNode` (raft, metadata, storage engine) in one JVM. 
Running streamstack means running one server process per node. DS and S2 are alternative protocol facades over the same engine, pick one per deployment.

Storage is chosen by the topo file (`TOPO`), not by the facade compose file. Stack compose files with `-f` to combine a node with MinIO. 
For AWS, set `DATA_BUCKET` / `WAL_BUCKET` to override the topo's S3 URIs without editing YAML, that rewrite drops any MinIO `endpoint` / `pathStyle`.

## Docker Compose

| Compose file | What it runs |
|--------------|--------------|
| `docker-compose.minio.yml` | MinIO + bucket init |
| `docker-compose.ds.yml` | one DS node (`NODE_ID` 1) |
| `docker-compose.s2.yml` | one S2 node (`NODE_ID` 1) |
| `docker-compose.cluster.ds.yml` | three DS nodes (`topo.cluster.yaml`) |

## MinIO

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/docker/docker-compose.minio.yml \
  -f harness/docker/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2 instead. Node listens on `127.0.0.1:4437`. 
MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`). On Linux, add `extra_hosts: ["host.docker.internal:host-gateway"]` to the node service so `topo.minio.yaml` can reach MinIO on the host.

Bare JVM (`topo.minio.yaml` uses `host.docker.internal` for Docker, override to `127.0.0.1` on the host):

```bash
docker compose -f harness/docker/docker-compose.minio.yml up -d

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1

mvn -pl frontend/ds/server -am package -DskipTests
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.minio.yaml --node-id 1 \
  --storage "0@s3://streams-data?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true" \
  --wal "0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true"
```

Swap `frontend/ds/server` / `ds-server.jar` for `frontend/s2/server` / `s2-server.jar` to run S2.

## AWS S3

1. Create data + WAL buckets.
2. `cp harness/configs/.env.example harness/configs/.env` and fill `AWS_*`, `DATA_BUCKET`, `WAL_BUCKET`.

```bash
docker compose --env-file harness/configs/.env \
  -f harness/docker/docker-compose.ds.yml \
  up -d --build
```

Or bare JVM (creds from env / `~/.aws` / IAM):

```bash
java -jar frontend/ds/server/target/ds-server.jar \
  --topo harness/configs/topo.aws.yaml --node-id 1 \
  --storage "0@s3://${DATA_BUCKET}?region=${AWS_REGION}" \
  --wal "0@s3://${WAL_BUCKET}?region=${AWS_REGION}"
```

## Multi-node

`docker-compose.cluster.ds.yml` starts three DS processes against `topo.cluster.yaml`. Each service is a fixed `NODE_ID` (1 / 2 / 3). Host network so they bind 4437–4439 / 8091–8093 as in the topo.

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/docker/docker-compose.minio.yml \
  -f harness/docker/docker-compose.cluster.ds.yml \
  up -d --build
```

Bare JVM, same topo, one process per id:

```bash
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 1
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 2
java -jar frontend/ds/server/target/ds-server.jar --topo harness/configs/topo.cluster.yaml --node-id 3
```

`topo.cluster.yaml` defaults to MinIO storage; set `DATA_BUCKET` / `WAL_BUCKET` (compose) or `--storage` / `--wal` (JVM) for AWS.

## Topology

| File | Storage |
|------|---------|
| `topo.minio.yaml` | MinIO via `host.docker.internal:9000` (container → host) |
| `topo.aws.yaml` | Real `s3://` (no endpoint) |
| `topo.cluster.yaml` | Three nodes; MinIO via `127.0.0.1:9000` (host network / JVM) |

URI forms: `0@s3://bucket?region=…`, `-2@file:///path`, `0@mem://bucket` (tests).  
Omit `global.wal` to default WAL to the storage URI when storage is S3.

`--topo` requires `--node-id`. CLI flags override topo values. Topo `global.envs` are informational — export credentials in the environment that starts the process.

## Clean up

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/docker/docker-compose.minio.yml \
  -f harness/docker/docker-compose.ds.yml \
  down -v
```
