# Operations

Every node optionally serves an admin-plane on a separate port, independent of the protocol facade. Default port is `9090`. Set it per node with `adminPort` in the topo, `--admin-port` on the CLI, or `ADMIN_PORT` in the Docker image. Set `0` to disable.

The admin APIs also power the dashboard with cluster, node, and raft status.

## Health

Point load-balancer target-group health checks at `/ready`. On shutdown the node flips `/ready` to `503` first and waits `shutdownDrainSec` (topo config, default `0`) before closing, so the load balancer drains in-flight traffic.

At startup the node verifies the storage and WAL buckets are reachable, retrying with backoff for up to ten attempts before failing. `/health` responds while it retries.
