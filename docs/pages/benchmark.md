# Benchmark

Java 17+ (Java 21 recommended). Maven. [Docker](https://docs.docker.com/get-docker/).

## Install

::: code-group

```bash [Stream Stack]
mvn clean package
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.native.yml \
  up -d --build
```

```bash [Durable Streams]
mvn clean package
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

:::

The node listens on `127.0.0.1:4437`. CLI jar: `cli/target/streamstack.jar`. MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`).

If the node comes up before the buckets exist (`NoSuchBucket`):

```bash
docker restart local-node1-1
```

If ports `9000-9001` are taken, stop leftover MinIO (`docker-minio-1` or similar) first.

## Smoke

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 64 -w 8 -d 8
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 64 -w 8 -d 8
```

:::

Creates a temporary stream, pipelines 64-record batches with 8 in-flight appends for 8s, reads them back live, then deletes. That path covers create, batch append, read, and delete on either protocol.

`-b` is record size in bytes, `-n` records per append, `-w` concurrent appends, `-d` duration in seconds. The DS bench also takes `-t` to throttle write MiB/s (`0` is unthrottled).

## Load

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 1024 -w 16 -d 20
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 256 -w 32 -d 20
```

:::

Same path, heavier: large batches for 20s. The node should sustain pipelined writes without 409s or timeouts (roughly 60 MiB/s writes on a laptop). This is the run used on the homepage.

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 2048 -w 16 -d 60
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 512 -w 64 -d 60
```

:::
