# Bench

CLI jar: `cli/target/streamstack.jar`. Node: `127.0.0.1:4437`.

If ports 9000–9001 are taken, stop leftover MinIO (`docker-minio-1` or similar) first.

## Start DS + Local MinIO

```bash
cp harness/local/.env.example harness/local/.env
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.minio.yml \
  -f harness/local/docker-compose.ds.yml \
  up -d --build
```

If the node starts before buckets exist (`NoSuchBucket`):

```bash
docker restart local-node1-1
```

## DS Server

```bash
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 64 -w 8 -d 8
```

Creates a temporary stream, pipelines 64-record JSON batches with 8 in-flight appends for 8s, live-reads them, then deletes. Verifies: create / appends / SSE read / delete on the durable-streams protocol.

```bash
java -jar cli/target/streamstack.jar ds bench --endpoint http://127.0.0.1:4437 \
  -b 1024 -n 256 -w 32 -d 20
```

Same path, but heavier: 256 records per append, 32 in-flight, 20s. Verifies: the node can sustain pipelined writes without 409s or timeouts (expect ~40–80 MiB/s).

## Start S2 + Local MinIO

```bash
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.ds.yml stop
docker compose --env-file harness/local/.env \
  -f harness/local/docker-compose.s2.yml up -d --build
```

Note: `Basin` name must be 8–48 chars.

## S2 Server

```bash
java -jar cli/target/streamstack.jar s2 bench --endpoint http://127.0.0.1:4437 demobasin \
  -b 1024 -n 64 -w 8 -d 8
```

Creates basin `demobasin`, then a temp stream, producer-batches 64 records with an 8 MiB in-flight window, live-reads, then tears down. Verifies: basin, stream, appends, live reads on S2 facade.

```bash
java -jar cli/target/streamstack.jar s2 bench --endpoint http://127.0.0.1:4437 demobasin \
  -b 1024 -n 256 -w 16 -d 20
```

Same path, but heavier: 256-record batches and a 16 MiB in-flight window. Verifies: a single S2 writer can pipeline without `SeqNumMismatch` or request-size errors.
