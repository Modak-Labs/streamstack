# syntax=docker/dockerfile:1
# PicoMQ node image: the `pico` binary (serve + CLI).
# Protocol / meta / storage are runtime config (flags or PICO_* env).

FROM rust:1-bookworm AS build
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends libsqlite3-dev pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY . .
# target/ and cargo caches live on the BuildKit host so rebuilds stay incremental.
RUN --mount=type=cache,id=picomq-target,sharing=locked,target=/src/target \
    --mount=type=cache,id=picomq-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=picomq-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo build --locked --release -p pico-cli \
 && cp /src/target/release/pico /src/pico

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/pico /usr/local/bin/pico

EXPOSE 4437 9090
ENTRYPOINT ["pico"]
CMD ["serve"]
