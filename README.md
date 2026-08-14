# Stream Stack

A durable stream engine on object storage, with storage, metadata, and protocol as the three primary decoupled layers.

<p align="left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/architecture-dark.svg">
    <img src="docs/assets/architecture-light.svg" alt="Stream Stack architecture" width="920">
  </picture>
</p>

## Installation

Java 17+ and Maven.

```bash
mvn clean package
```

## Quickstart

```bash
docker compose --env-file harness/configs/minio.env \
  -f harness/docker/docker-compose.minio.yml \
  -f harness/docker/docker-compose.ds.yml \
  up -d --build
```

Node listens on `127.0.0.1:4437`. MinIO, cluster, and topo files: [harness/README.md](harness/README.md).

## Deployment

Create data and WAL S3 buckets. Copy `harness/configs/.env.example` to `harness/configs/.env` and fill `AWS_*`, `DATA_BUCKET`, and `WAL_BUCKET`.

```bash
docker compose --env-file harness/configs/.env \
  -f harness/docker/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2 facade. Node listens on `127.0.0.1:4437`.

Multi-Node (3 nodes) cluster (ports 4437–4439):

```bash
docker compose --env-file harness/configs/.env \
  -f harness/docker/docker-compose.cluster.ds.yml \
  up -d --build
```

## License

Apache 2.0.
