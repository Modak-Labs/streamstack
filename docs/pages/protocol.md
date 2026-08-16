# Protocol

Storage, metadata, and protocol are three decoupled layers. The engine writes to object-storage and replicates metadata with `JRaft`. The protocol layer sits on top and speaks one client API per deployment, on port `4437` by default.

[Durable Streams](/protocol/durable-streams) is the API used across these docs. The same engine can also serve an [S2](/protocol/s2) compatible API instead, and an opinionated native API is coming.

Specs:

- [Durable Streams](/protocol/durable-streams) `Recommended`
- [S2](/protocol/s2)
