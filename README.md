# Stream Stack

A durable stream engine on object storage, with storage, metadata, and protocol as the three primary decoupled layers.

Join the [Discord](https://discord.gg/ETfTbRfkb2) for questions, ideas, and development updates.

<p align="left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/pages/assets/images/architecture-dark.svg">
    <img src="docs/pages/assets/images/architecture-light.svg" alt="Stream Stack architecture" width="920">
  </picture>
</p>

## Installation

Java 17+ and Maven. Docker (Optional)

```bash
mvn clean package
```

## Quickstart

Stream Stack (native protocol):

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.native.yml \
  up -d --build
```

Durable Streams:

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

Node listens on `127.0.0.1:4437`. Admin plane and dashboard: `127.0.0.1:9090`. Cluster, AWS, and Fly deployments are covered in the docs.

## Deployment

Create data and WAL S3 buckets. Copy `harness/aws/.env.example` to `harness/aws/.env` and fill `AWS_*`, `DATA_BUCKET`, and `WAL_BUCKET`.

Stream Stack (native protocol):

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.native.yml \
  up -d --build
```

Durable Streams:

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.ds.yml \
  up -d --build
```

Node listens on `127.0.0.1:4437`. Three nodes: `harness/aws/docker-compose.cluster.native.yml` or `harness/aws/docker-compose.cluster.ds.yml`.

Multi-node (3 nodes) local cluster (ports 4437–4439), swap `native` for `ds` to serve Durable Streams:

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.cluster.native.yml \
  up -d --build
```

## License

Apache 2.0.
