# AWS

Create data and WAL buckets (they can be the same bucket). Copy `harness/aws/.env.example` to `harness/aws/.env` and fill `DATA_BUCKET` and `WAL_BUCKET`. `AWS_*` is optional and depends on the credential provider.

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.ds.yml \
  up -d --build
```

The node listens on `127.0.0.1:4437`.

The following example has three nodes, on ports `4437-4439`:

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.cluster.ds.yml \
  up -d --build
```

Bare JVM (credentials from env / `~/.aws` / IAM):

```bash
java -jar frontend/ds/server/target/ds-server.jar \
  --topo harness/aws/topo.yaml --node-id 1 \
  --storage "0@s3://${DATA_BUCKET}?region=${AWS_REGION}" \
  --wal "0@s3://${WAL_BUCKET}?region=${AWS_REGION}"
```

## Topology

| File | Storage |
|------|---------|
| `aws/topo.yaml` | Single node |
| `aws/topo.cluster.yaml` | Multi-node (3 nodes) |
