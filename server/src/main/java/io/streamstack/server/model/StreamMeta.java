package io.streamstack.server.model;

import java.time.Instant;

public record StreamMeta(
    String name,
    long streamId,
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    OffsetToken nextOffset,
    boolean closed,
    Integer ownerNodeId) {
}
