# Ownership and routing

Each stream is served by exactly one node at a time. Single ownership is what makes the rest of the design simple: offsets are assigned in one place, the tail caches are coherent because only the owner writes, and appends never coordinate across nodes. The work is then split into two problems, deciding who owns a stream and getting requests to that node.

## Node identity

A node has a stable id and a fresh epoch for every process start, taken from the wall clock in milliseconds. On startup the node registers itself through the metadata log with its id, epoch, advertised HTTP address, and slot count. Registration at a lower epoch than the recorded one is rejected, so a replaced instance cannot re-register.

The epoch is also carried by every metadata command the node proposes. After a restart the new epoch invalidates everything the old instance had in flight, and the old instance, if it is still running somewhere, finds all of its proposals rejected. Two processes claiming the same node id resolve cleanly: the log orders their registrations and the later epoch wins.

## Placement

A stream needs an owner the first time something has to be done to it. Placement is a metadata command like everything else, and applying it is deterministic: the stream goes to the registered node with the lowest ratio of assigned streams to slots. Slots are a capacity weight, `1` by default, so a node with `4` slots takes four times the streams of a node with `1`. Slots can be changed at runtime through the admin API, which shifts where future placements land without moving anything already placed.

Because the choice is a pure function of the replicated state, every node computes the same answer. Two nodes racing to place the same stream propose two commands, the log orders them, and the second is a no-op.

## Routing

Any node accepts any request. The receiving node resolves the name to a stream id and checks its local view, in order.

<div class="pico-diagram">
<svg viewBox="0 0 660 340" width="660" role="img" aria-label="A request is checked against the view: unregistered names are served locally, pending transfers route to the target, owned streams route to the owner, closed streams are served locally.">
  <defs>
    <marker id="arro" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
      <path d="M0 0.5 L7.5 4 L0 7.5 Z" class="arrow"/>
    </marker>
  </defs>
  <rect x="20" y="132" width="170" height="56" class="box"/>
  <text x="105" y="156" text-anchor="middle" class="label">request</text>
  <text x="105" y="174" text-anchor="middle" class="sub">check the local view</text>
  <rect x="300" y="20" width="150" height="48" class="box"/>
  <text x="375" y="49" text-anchor="middle" class="sub">name not registered</text>
  <rect x="300" y="88" width="150" height="48" class="box"/>
  <text x="375" y="117" text-anchor="middle" class="sub">pending transfer</text>
  <rect x="300" y="156" width="150" height="48" class="box"/>
  <text x="375" y="185" text-anchor="middle" class="sub">opened or placed</text>
  <rect x="300" y="224" width="150" height="48" class="box"/>
  <text x="375" y="253" text-anchor="middle" class="sub">closed</text>
  <rect x="510" y="20" width="130" height="48" class="box-accent"/>
  <text x="575" y="49" text-anchor="middle" class="label">serve here</text>
  <rect x="510" y="88" width="130" height="48" class="box"/>
  <text x="575" y="112" text-anchor="middle" class="label">307</text>
  <text x="575" y="128" text-anchor="middle" class="sub">to target</text>
  <rect x="510" y="156" width="130" height="48" class="box"/>
  <text x="575" y="180" text-anchor="middle" class="label">307</text>
  <text x="575" y="196" text-anchor="middle" class="sub">to owner</text>
  <rect x="510" y="224" width="130" height="48" class="box-accent"/>
  <text x="575" y="253" text-anchor="middle" class="label">serve here</text>
  <path d="M190 150 L292 48" class="edge" marker-end="url(#arro)"/>
  <path d="M190 156 L292 114" class="edge" marker-end="url(#arro)"/>
  <path d="M190 164 L292 182" class="edge" marker-end="url(#arro)"/>
  <path d="M190 170 L292 246" class="edge" marker-end="url(#arro)"/>
  <path d="M450 44 L502 44" class="edge" marker-end="url(#arro)"/>
  <path d="M450 112 L502 112" class="edge" marker-end="url(#arro)"/>
  <path d="M450 180 L502 180" class="edge" marker-end="url(#arro)"/>
  <path d="M450 248 L502 248" class="edge" marker-end="url(#arro)"/>
  <text x="330" y="310" text-anchor="middle" class="sub">local requests always serve here</text>
</svg>
</div>

A name that is not registered yet is served locally, which is how creates land wherever the client happens to connect. A stream with a pending transfer routes to the transfer target, covered in [Transfers](/docs/design/transfers). A stream that is opened, or placed but never opened, routes to its owning node, as a `307` redirect to the owner's advertised address. A closed stream is served locally, so any node can revive one whose previous owner is gone. The next open re-places it and routing converges on the new owner.

## Why a stale view is safe

Routing reads the node's local view, which can lag the log by a moment. That is fine because routing is only a performance decision. Correctness comes from fencing: opening a stream bumps its epoch through the log, and a node that is not the current owner cannot commit anything to it. The worst a stale view produces is an extra redirect hop while the views catch up. It can misdirect a request, it cannot produce two writers.
