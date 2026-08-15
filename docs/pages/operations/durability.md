# Durability

Stream data lives in object-storage, but metadata (streams, object registry, KV) lives in each node's raft `dataDir`: a periodic local snapshot and a bounded log. Raft replication covers single-node loss. The snapshot archive covers losing the data directories of a quorum.

The raft leader archives every metadata snapshot to the storage bucket under `_streamstack/metadata/{clusterId}/snapshots/`, keeping the last five. Archival is asynchronous and never blocks the snapshot itself. 

::: info
Run production nodes on durable disks (EBS, not instance storage) for `dataDir`, with a stable DNS name per node.
:::

Failures are logged and reported on `GET /admin/snapshots` and visible on the dashboard. Disable with `metadataArchive: false` in the topo config, `--metadata-archive false` on the CLI, or `METADATA_ARCHIVE=false` in the Docker image.

## Replacing a node

- Disk survived: attach the volume to a replacement machine with the same `nodeId`, DNS name and start it. The node replays its local snapshot, log and rejoins.
- Disk lost: start the replacement machine with the same `nodeId`, DNS name and an empty `dataDir`. The raft leader streams its latest snapshot to the new peer and replays the log tail. If the address changed, fix membership with `POST /admin/peers` and `DELETE /admin/peers/{peer}`.

## Restoring from archive

If a quorum of data directories is lost, bootstrap from the archive:

- Start node 1 with an empty `dataDir`, a single-peer topo, and `--restore-from-storage true` (or `RESTORE_FROM_STORAGE=true`). The node loads the latest archived snapshot before forming raft and persists it into a local raft snapshot right after election.
- Verify with `GET /admin/cluster` and `GET /admin/streams/{name}`.
- Add nodes 2 and 3 with empty data dirs via `POST /admin/peers`. They receive the snapshot from the leader.

Restore only runs on a fresh `dataDir`. If local raft state exists it is skipped, and a log without a snapshot fails with instructions to wipe first.

On graceful shutdown the node flushes the `WAL` tail, commits it to metadata, and archives a final snapshot before raft stops, so acknowledged writes always survive a planned stop followed by disk loss. 

For a hard crash where the data directories are also lost, commits since the last archived snapshot (bounded by the `30s` snapshot interval) keep their bytes in the bucket but lose their metadata entries. A crash alone, with disks intact, loses nothing because the local raft log and WAL replay cover the tail.
