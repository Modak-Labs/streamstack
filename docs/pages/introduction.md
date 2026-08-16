# Introduction

A durable stream engine on object storage, with storage, metadata, and protocol as three decoupled layers.

## S3

Before landing on StreamStack, one of the early names was `S3: Super Simple Streams`. The name captured two ideas that still define the project: simple streams and `S3-compatible` object storage as the primary durable storage layer.

In StreamStack, a stream is a `URL-addressable`, `append-only`, durable sequence of records. Applications are encouraged to use granular, independently addressable streams instead of combining multiple logical streams into a single shared log. Granular does not mean small. Each stream can scale from low to very high read and write throughput, support high fanout, be bottomless and replayable, and be read by `position` or `offset` like any other durable log.

StreamStack's data plane is built on the `S3Stream` storage engine and a write-ahead log, with metadata replicated through `JRaft`. Storage, metadata, and protocol are separate layers, so each can evolve without requiring the others to be rewritten.
