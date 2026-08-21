# picomq

PicoMQ is durable, real-time streams over HTTP, built on S3-compatible object storage.

- **`s3stream/`** the stream engine (see `s3stream/README.md`)
- **`picomq/`** the host: metadata plane, server, HTTP frontends (Pico protocol and Durable Streams), client, and the `pico` CLI

## Run a node

```bash
# single node: SQLite metadata log, local object storage
cargo run -p pico-cli -- serve \
    --meta-url sqlite:./data/meta.db \
    --storage '-2@file://./objects'

# clustered: Postgres metadata log, S3
cargo run -p pico-cli -- serve \
    --node-id 2 --protocol ds \
    --listen 0.0.0.0:4437 --http-address http://node2.internal:4437 \
    --meta-url postgres://user:pass@pg:5432/picomq \
    --storage '-2@s3://bucket?region=us-east-1'
```

Every flag has a `PICO_*` env equivalent. `/health` and `/ready` are on `--admin-listen` (default `127.0.0.1:9090`).

## Use it

```bash
pico create /streams/orders --content-type text/plain
seq 1 1000 | pico append /streams/orders --batch 100
pico read /streams/orders
pico tail /streams/orders -f
pico ls --prefix /streams/
pico close /streams/orders && pico delete /streams/orders

pico --endpoint http://node2.internal:4437 config set prod   # save a profile
pico --profile prod ls

pico --http2 bench -b 1024 -w 512 --connections 4 --streams 4 -d 60
```

## Test

```bash
cargo test --workspace

# Postgres-backed tests, env-gated
PICOMQ_PG_URL=postgres://user:pass@localhost:5432/picomq \
    cargo test -p pico-sql --test pg_contract --test pg_e2e
```
