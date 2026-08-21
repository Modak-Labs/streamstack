# Object WAL Protocol

Byte-compatible with the upstream Java implementation. Conformance fixtures in
`../conformance/` pin the format.

The WAL is a **per-node, single-writer, epoch-fenced log of objects** in a shared bucket.
It is an ack accelerator, not long-term storage: records live in the WAL only until the
delta-WAL upload commits them into the shared object layout, then the WAL is trimmed.

## Key scheme

Source: `ObjectUtils`.

```
nodePrefix = md5hex(nodeId).toUpperCase() + "/" + namespace + clusterId + "/" + nodeId [ + "_" + type] + "/"
v0 object:  {nodePrefix}{epoch}/wal/{startOffset}
v1 object:  {nodePrefix}{epoch}/wal/{startOffset}-{endOffset}
```

- The md5 prefix spreads nodes across S3 key partitions.
- `epoch` is the **node epoch** granted by the metadata plane at open time; a new epoch
  fences all previous writers of this WAL.
- Offsets are logical byte offsets in the WAL's continuous address space.
- WAL data files align to `DATA_FILE_ALIGN_SIZE = 64 MiB` boundaries
  (`floorAlignOffset`/`ceilAlignOffset`).

## WAL object layout

Each WAL object = `WALObjectHeader` + concatenated records.

Header v1 (magic `0xEDCBA987`, 48 bytes) — v0 (magic `0x12345678`, 40 bytes) lacks trimOffset:

| size | field | notes |
| --- | --- | --- |
| 4 | magic | v0/v1 discriminator |
| 8 | startOffset | logical WAL offset of the first record body |
| 8 | bodyLength | bytes of record data in this object |
| 8 | stickyRecordLength | deprecated, 0 |
| 4 | nodeId | |
| 8 | nodeEpoch | |
| 8 | trimOffset | v1 only: WAL trim watermark piggybacked at write time |

v0 end-offset compatibility: `endOffset = startOffset + objectSize - HEADER_SIZE_V0`.

## WAL record framing

Source: `s3.wal.common.RecordHeader` (`RECORD_HEADER_SIZE = 24`):

| size | field |
| --- | --- |
| 4 | magic: `0x87654321` data, `0x76543210` padding/empty |
| 4 | recordBodyLength |
| 8 | recordBodyOffset (= header's logical offset + 24) |
| 4 | recordBodyCRC (crc32 of body) |
| 4 | recordHeaderCRC (crc32 of the preceding 20 bytes) |

Body = one encoded `StreamRecordBatch` (see record-format.md). CRC is CRC-32 (Java
`WALUtil.crc32`; verify polynomial — Netty/Java `CRC32` is ISO-HDLC, *not* Castagnoli —
and match it exactly).

## Write path

Source: `ObjectWALService` / `DefaultWriter`.

- Appends accumulate in a batch buffer; a batch is sealed and uploaded when
  `batchInterval` (default 250 ms) elapses or `maxBytesInBatch` (default 8 MiB) is
  reached, whichever first.
- **A record's place in the log is fixed when the append call is made**, not when
  it completes: Java's `append` is synchronous and returns a
  `CompletableFuture` (`submit` in Rust). Callers that need records adjacent in
  the log therefore only have to call in sequence — but they must not let an
  await, a spawned task, or a queue reorder those calls, since a caller-order
  inversion reaches the log cache as a base-offset discontinuity and the batch
  is rejected.
- Up to `maxInflightUploadCount` (default 50) object PUTs may be in flight; the
  **confirm offset advances only in order** — a batch is confirmed (and its appends
  acked) only when it and all prior batches are durably PUT.
- Backpressure: `maxUnflushedBytes` (default 1 GiB) caps unconfirmed bytes; appends
  beyond it fail with `OverCapacityException`.
- Trim: logical only — record the trim offset (piggybacked in v1 headers), then delete
  fully-trimmed WAL objects asynchronously.

## Fencing and recovery

- Open modes (`OpenMode`): `READ_WRITE` (owner), `RECOVERY` mode used by failover to
  read another node's WAL without writing.
- `ReservationService` verifies the caller's (nodeId, epoch) reservation before the WAL
  accepts writes; a stale writer gets `WALFencedException`.
- Recovery (`RecoverIterator` semantics):
  1. LIST all objects under the node prefix; parse `(epoch, startOffset, endOffset)`
     from keys (`ObjectUtils.parse`), sort by `(epoch, startOffset)`.
  2. `skipOverlapObjects`: when consecutive objects from *different epochs* overlap in
     offset range, drop the older-epoch object — it is a dirty write from a fenced
     zombie.
  3. Iterate records from the recovered trim offset onward, validating CRCs; stop at
     the first torn/invalid record (end of durable log).
  4. `reset()` after successful recovery+upload: mark everything recovered as consumed
     (trim to recover point).

## Invariants

1. Single logical writer per (nodeId): enforced by epoch fencing, not by locks.
2. An acked append is durable in a WAL object (PUT completed) — never ack from memory.
3. Confirm offset is contiguous: no holes below it, ever.
4. Trim never exceeds confirm offset; delete never touches objects at/beyond trim.
5. Recovery after any crash yields exactly the acked-but-not-uploaded suffix.
