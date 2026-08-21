# Delta WAL Upload Protocol (S3Storage pipeline)

Byte-compatible with the upstream Java implementation. Conformance fixtures in
`../conformance/` pin the format.

## The two-log architecture

Appends go to **two places in parallel**: the delta WAL (durability, acks) and the
LogCache (readability). The durable long-term layout is produced asynchronously by
uploading sealed LogCache blocks as objects and committing them to the metadata plane;
only then is the WAL trimmed.

```
append ──► WAL (object PUTs) ──confirm──► ack
   └─────► LogCache (mutable block)
                 │  seal at walUploadThreshold
                 ▼
        UploadWriteAheadLogTask
          prepare ► upload ► commit ► trim WAL
                 │
                 ▼
        stream set object + split stream objects (shared layout)
```

## Append path

Source: `S3Storage#append0`.

1. Admission: if LogCache is at capacity (`tryAcquirePermit`), the request goes to a
   backoff queue drained every 100 ms (RUST-NATIVE: bounded channel / async suspension
   replaces queue+timer; same admission invariant).
2. WAL append; on WAL confirm, `ConfirmWAL#onAppend` runs, then the record is put into
   the LogCache and the caller's future completes on a per-stream callback executor
   (per-stream ordering is part of the contract).
3. WAL `OverCapacityException` => backoff + force upload to free WAL space.

## Upload task state machine

Source: `DefaultUploadWriteAheadLogTask`.

- Seal: LogCache block seals when size crosses `walUploadThreshold`
  (clamped to <= 2/5 of LogCache capacity at construction — keep this clamp) or on
  force/interval triggers.
- `prepare()`: `ObjectManager::prepare_object(count, ttl)` leases object ids; uncommitted
  ids expire after ttl.
- `upload()`: partition the sealed block per stream:
  - streams with >= `streamSplitSize` bytes (or forced split) get their own
    **stream objects**;
  - the rest go into one **stream set object** (bounded by
    `maxStreamNumPerStreamSetObject`).
  Objects are written via ObjectWriter with `objectBlockSize` / `objectPartSize`.
- `commit()`: single atomic `ObjectManager::commit_stream_set_object(request)` carrying
  the stream set object id/size and all stream-object descriptors + stream ranges.
- After commit: trim WAL to the block's confirm offset; release the block.
- Ordering: multiple upload tasks may run concurrently, but **commits happen in seal
  order** (Java serializes via `inflightWALUploadTasks` queue semantics).
- Force upload (`maybeForceUpload`): at most one inflight force task; used on stream
  close, fencing, shutdown, and WAL over-capacity. Lazy upload with linger
  (`lazyUpload`) batches commit triggers (e.g. snapshot-read commit, upload interval).

## Read path

Source: `S3Storage#read0`.

1. Try LogCache for `[start, end)`; full hit => return (`DELTA_WAL_CACHE_HIT`).
2. Partial tail hit => clamp end to the first cached offset; read the head from the
   block cache (committed objects, with readahead); stitch; verify contiguity
   (`continuousCheck`) — any gap is a bug and must fail loudly.
3. `fastRead` option: fail fast instead of touching the block cache (used by tail
   consumers that fall back to a slow path).

## Recovery

Source: `S3Storage#recover0`, `WALRecovery`.

1. `StreamManager::get_opening_streams()` => per-stream committed `endOffset` map.
2. Iterate WAL records (see wal-protocol.md). For each stream, **discard records at
   offsets below the committed endOffset; keep only the continuous tail above it**.
   A discontinuity within the kept tail truncates recovery for that stream at the gap.
3. Upload the recovered records immediately as a normal upload task
   (prepare/upload/commit), bounded per batch (Java caps at 512 MiB per recovery upload).
4. `WriteAheadLog::reset()` — consume/trim everything recovered.
5. `StreamManager::close_stream(streamId, epoch)` for every opening stream so ownership
   can move cleanly.

Failover recovery is the same procedure run against a dead node's WAL handle
(`s3.failover.Failover`), in RECOVERY open mode.

## Invariants

1. An acked record is in the WAL, the LogCache, or a committed object — always in at
   least one durable or recoverable place; crash at any point loses nothing acked.
2. WAL trim offset <= last committed upload's confirm offset.
3. Commit order == seal order (metadata plane sees monotonic stream ranges).
4. After recovery completes, the WAL is empty (reset) and no stream remains open under
   the old epoch.
5. Stream ranges in a commit are exact: `[startOffset, endOffset)` per stream with no
   overlap against previously committed ranges.
