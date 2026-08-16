# Benchmark

Java 17+ (Java 21 recommended). Maven. [Docker](https://docs.docker.com/get-docker/).

## Install

```bash
mvn clean package
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

The node listens on `127.0.0.1:4437`. CLI jar: `cli/target/streamstack.jar`. MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`).

If the node comes up before the buckets exist (`NoSuchBucket`):

```bash
docker restart local-node1-1
```

If ports `9000-9001` are taken, stop leftover MinIO (`docker-minio-1` or similar) first.

## Smoke

```bash
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 64 -w 8 -d 8
```

Creates a temporary stream, pipelines 64-record JSON batches with 8 in-flight appends for 8s, live-reads them, then deletes. That path covers create, append, SSE read, and delete.

`-b` is record size in bytes, `-n` records per append, `-w` concurrent appends, `-d` duration in seconds. `-t` throttles write MiB/s (`0` is unthrottled).

## Load

```bash
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 256 -w 32 -d 20
```

Same path: 256 records per append, 32 in-flight, 20s. The node should sustain pipelined writes without 409s or timeouts. This is the run used on the homepage.

```bash
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 512 -w 64 -d 60
```
