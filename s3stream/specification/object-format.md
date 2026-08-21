# Object Format: data blocks, index block, footer

Byte-compatible with the upstream Java implementation. Conformance fixtures in
`../conformance/` pin the format.

An object (stream set object or stream object) is:

```
[data block 0][data block 1]...[data block N-1][index block][footer]
```

All integers big-endian.

## Data block

Source: `ObjectWriter.DataBlock`. `BLOCK_HEADER_SIZE = 10`.

| size | field | notes |
| --- | --- | --- |
| 1 | magic | `0x5A` (`DATA_BLOCK_MAGIC`) |
| 1 | flag | `0x02` (`DATA_BLOCK_DEFAULT_FLAG`); reserved bits for compression |
| 4 | recordCount | i32 |
| 4 | dataLength | i32, byte length of the records section |
| dataLength | records | concatenated encoded `StreamRecordBatch`es (see record-format.md) |

Rules:
- A data block contains records of **exactly one stream**, contiguous by offset.
- Blocks are cut when accumulated record payload reaches `blockSizeThreshold`
  (grouping rule in `DefaultObjectWriter#groupByBlock`).
- Writer-side ordering check (`DefaultObjectWriter#check`): stream ids must be
  non-decreasing across `write` calls; within a stream, start offset must be
  >= previous end offset (gaps allowed, overlap forbidden).

## Index block

Source: `ObjectWriter.DefaultObjectWriter.IndexBlock`, `DataBlockIndex`.

Concatenation of fixed-size entries, one per data block, in file order.
`BLOCK_INDEX_SIZE = 36`:

| size | field |
| --- | --- |
| 8 | streamId |
| 8 | startOffset |
| 4 | endOffsetDelta (endOffset = startOffset + delta) |
| 4 | recordCount |
| 8 | startPosition (byte position of the block in the object) |
| 4 | blockSize (bytes) |

Entries are sorted by (streamId, startOffset) because blocks are written in that order.
Lookup: binary search for (streamId, offset range) — Java `IndexBlockOrderedBytes`.

## Footer

Source: `ObjectWriter.Footer`. `FOOTER_SIZE = 48`, always the last 48 bytes.

| size | field |
| --- | --- |
| 8 | indexBlockStartPosition |
| 4 | indexBlockLength |
| 28 | reserved (zeros) |
| 8 | magic = `0x88E241B785F4CFF7` |

Read path: fetch last 48 bytes -> validate magic -> range-GET the index block ->
binary-search entries -> range-GET data blocks.

## Object keys

Source: `s3.metadata.ObjectUtils.genKey`: `md5hex-prefixed` namespaced keys of the form
`{md5hex(objectId)[0..4]}/{namespace}/{objectId}` (verify exact slicing against Java when
implementing `metadata.rs`; keep identical so mixed Java/Rust clusters address the same
objects).

## Composite objects

Source: `s3.CompositeObject{,Writer,Reader}`. A composite object links component objects
(their data block indexes) under one logical object to reduce metadata and enable
compaction without rewriting data. Layout: an object containing only an index of
(component objectId, block indexes) plus footer — no data blocks of its own. Port the
exact layout from `CompositeObjectWriter` when implementing `composite.rs`
(attributes bit `COMPOSITE` in `ObjectAttributes` marks them).

## Conformance

Golden vectors in `conformance/fixtures/object/`: whole objects written by the Java
`ObjectWriter` for seeded record sets, plus expected index entries as JSON. The Rust
writer must reproduce identical bytes; the Rust reader must read Java-written objects and
vice versa.
