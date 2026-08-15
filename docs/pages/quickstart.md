# Quickstart

Java 17+ (Java 21 recommended). Maven. [Docker](https://docs.docker.com/get-docker/).

Each node includes the `S3Stream` storage engine, the WAL, `JRaft` metadata, and a protocol facade. All of that lives in the same server.

## Start a node

```bash
mvn clean package
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

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

```bash
java -jar cli/target/streamstack.jar ds create \
  --endpoint http://127.0.0.1:4437 demo
```

</div>

<div class="ss-cli__row">

<span class="ss-cli__label">Tail</span>

```bash
java -jar cli/target/streamstack.jar ds tail -f \
  --endpoint http://127.0.0.1:4437 demo
```

</div>

<div class="ss-cli__row">

<span class="ss-cli__label">Append</span>

```bash
java -jar cli/target/streamstack.jar ds append \
  --endpoint http://127.0.0.1:4437 demo
```

</div>

</div>

`append` reads stdin. Type a line, press enter, repeat. `tail -f` prints each line as it lands.

## Protocol

This example speaks Durable Streams. Same MinIO and port, S2 instead:

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.s2.yml \
  up -d --build
```
