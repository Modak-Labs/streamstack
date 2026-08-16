# Protocol

Storage, metadata, and protocol are three decoupled layers. The engine writes to object-storage and replicates metadata with `JRaft`. The protocol layer sits on top and speaks one client API per deployment, on port `4437` by default.

The [StreamStack API](/protocol/native): sequenced records with timestamps and headers, batched appends in JSON or binary, idempotent producers, and live reads. The same engine can also serve a [Durable Streams](/protocol/durable-streams) compatible API instead.

Specs:

- [Stream Stack](/protocol/native) `Recommended`
- [Durable Streams](/protocol/durable-streams)
