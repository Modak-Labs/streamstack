# Protocol

Storage, metadata, and protocol are three decoupled layers. The engine writes to object-storage and replicates metadata with `JRaft`. A protocol facade sits on top and speaks one client protocol.

[Durable Streams](https://github.com/durable-streams/durable-streams) and [S2](https://github.com/s2-streamstore/s2) are the supported facades today. Pick one per deployment. Both talk to the same streams, WAL, and raft group. The HTTP port is `4437` by default.

Specs:

- [Durable Streams](/protocol/durable-streams)
- [S2](/protocol/s2)
