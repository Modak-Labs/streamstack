# Quickstart

Java 17+ (Java 21 recommended). Maven. [Docker](https://docs.docker.com/get-docker/).

Each node includes the `S3Stream` storage engine, the WAL, `JRaft` metadata, and the HTTP API. All of that lives in the same server.

## Start a node

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

The node listens on `127.0.0.1:4437`. MinIO console: http://127.0.0.1:9001 (`minioadmin` / `minioadmin`).

If the node comes up before the buckets exist (`NoSuchBucket`):

```bash
docker restart local-node1-1
```

## Append and tail

Create a stream, then tail in one terminal and append in another.

<div class="ss-cli">

<div class="ss-cli__row">

<span class="ss-cli__label">Create</span>

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native create \
  --endpoint http://127.0.0.1:4437 demo
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds create \
  --endpoint http://127.0.0.1:4437 demo
```

:::

</div>

<div class="ss-cli__row">

<span class="ss-cli__label">Tail</span>

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native tail \
  --endpoint http://127.0.0.1:4437 demo
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds tail -f \
  --endpoint http://127.0.0.1:4437 demo
```

:::

</div>

<div class="ss-cli__row">

<span class="ss-cli__label">Append</span>

::: code-group

```bash [Stream Stack]
java -jar cli/target/streamstack.jar native append \
  --endpoint http://127.0.0.1:4437 demo
```

```bash [Durable Streams]
java -jar cli/target/streamstack.jar ds append \
  --endpoint http://127.0.0.1:4437 demo
```

:::

</div>

</div>

`append` reads stdin. Type a line, press enter, repeat. `tail` prints each line as it lands (with its sequence number on Stream Stack).

The tabs pick the protocol. Both speak HTTP on the same port; run the node with the matching compose file from [Start a node](#start-a-node).
