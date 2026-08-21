# Record Format: StreamRecordBatch

Byte-compatible with the upstream Java implementation. Conformance fixtures in
`../conformance/` pin the format.

A `StreamRecordBatch` is the unit of append. It is stored **in its encoded form
everywhere** — in the WAL record body, in the LogCache, and inside object data blocks —
so encoding happens exactly once at append time.

All integers are **big-endian** (Netty `ByteBuf` default).

## Layout (v0, magic `0x22`)

| pos | size | field | notes |
| --- | --- | --- | --- |
| 0 | 1 | magic | `0x22` (`MAGIC_V0`) |
| 1 | 8 | streamId | i64 |
| 9 | 8 | epoch | i64, stream epoch at append time |
| 17 | 8 | baseOffset | i64, first logical offset in the batch |
| 25 | 4 | lastOffsetDelta (count) | i32; see link records below |
| 29 | 4 | payloadLength | i32, byte length of payload |
| 33 | payloadLength | payload | opaque bytes |

`HEADER_SIZE = 33`. Total encoded size = `33 + payloadLength`.

## Semantics

- `lastOffset = baseOffset + count` when `count > 0`.
- **Link records**: `count < 0` marks a link record and `lastOffset = baseOffset - count`
  (i.e. `baseOffset + |count|`). Link records reference data stored elsewhere
  (see `api.LinkRecordDecoder`); v1 of the Rust port only needs to preserve the encoding
  semantics, not the decoding pipeline.
- `size()` returns `payloadLength` (not total encoded size); `occupiedSize()` adds a fixed
  per-object overhead estimate used only for cache accounting (Java uses 144; the Rust
  port defines its own constant — it is not part of the wire format).
- Ordering: batches sort by `(streamId, baseOffset)`.

## Invariants

1. The encoded buffer is immutable after construction; all accessors read from it.
2. `parse` validates the magic byte and must reject unknown magics.
3. Within a stream, batches appended to storage are contiguous:
   `next.baseOffset == prev.lastOffset` (enforced at LogCache/object-writer level,
   not by the codec).

## Conformance

Golden vectors in `conformance/fixtures/record/`: encoded batches produced by the Java
implementation for a seeded set of (streamId, epoch, baseOffset, count, payload) tuples.
Round-trip tests must reproduce the exact bytes.
