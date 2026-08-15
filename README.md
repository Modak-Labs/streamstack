# Stream Stack

A durable stream engine on object storage, with storage, metadata, and protocol as the three primary decoupled layers.

<p align="left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/pages/assets/images/architecture-dark.svg">
    <img src="docs/pages/assets/images/architecture-light.svg" alt="Stream Stack architecture" width="920">
  </picture>
</p>

## Installation

Java 17+ and Maven.

```bash
mvn clean package
```

## Quickstart

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

Node listens on `127.0.0.1:4437`. MinIO, cluster, AWS, and Fly: [harness/README.md](harness/README.md).

## Deployment

Create data and WAL S3 buckets. Copy `harness/aws/.env.example` to `harness/aws/.env` and fill `AWS_*`, `DATA_BUCKET`, and `WAL_BUCKET`.

```bash
docker compose --env-file harness/aws/.env \
  -f harness/aws/docker-compose.ds.yml \
  up -d --build
```

Swap `docker-compose.ds.yml` for `docker-compose.s2.yml` to run S2 facade. Node listens on `127.0.0.1:4437`. Three nodes: `harness/aws/docker-compose.cluster.ds.yml`.

Multi-node (3 nodes) local cluster (ports 4437–4439):

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.cluster.ds.yml \
  up -d --build
```

## Documentation

The documentation source is in [`docs/pages`](docs/pages).

```bash
cd docs
npm install
npm run dev
```

Open http://localhost:5173.

## License

Apache 2.0.
