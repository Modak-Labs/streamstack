# Fly.io

The default application name is `streamstack` (edit [`fly.toml`](https://github.com/Modak-Labs/streamstack/blob/main/harness/fly/fly.toml)). Storage is [Tigris](https://fly.io/docs/tigris/) (S3-compatible).

The protocol is set in the `fly.toml` build args:

::: code-group

```toml [Stream Stack]
[build.args]
  MODULE = "frontend/native/server"
  JAR = "native-server.jar"
```

```toml [Durable Streams]
[build.args]
  MODULE = "frontend/ds/server"
  JAR = "ds-server.jar"
```

:::

If you are new to [Fly.io](https://fly.io), create an account (it includes a 7-day trial). To deploy from this machine, install the CLI and log in:

```bash
brew install flyctl
fly auth login
```

Then:

```bash
fly launch --config harness/fly/fly.toml
fly storage create
fly deploy --config harness/fly/fly.toml
```

Multi-node (3 nodes): `--config harness/fly/fly.cluster.toml`.

## Topology

| File | Storage |
|------|---------|
| `fly/topo.yaml` | One Fly Machine. Tigris (`region=auto`) |
| `fly/topo.cluster.yaml` | Three Fly process groups `n1-n3` |
