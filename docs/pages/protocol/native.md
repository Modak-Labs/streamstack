---
aside: false
outline: false
---

# Stream Stack

The StreamStack API. Default HTTP port is `4437`. Endpoints are in the sidebar.

Streams are HTTP paths. Records carry a server-assigned sequence number, a monotonic timestamp, and optional headers. Appends can batch many records in one request, either as JSON or as a compact binary batch, and are atomic per request. Idempotent producers, optimistic concurrency with `SS-Match-Seq`, catch-up reads with large pages, long-poll, and SSE tailing are built in.
