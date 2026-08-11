package io.streamstack.server.store;

import java.time.Instant;

public record StreamInfo(
    String path,
    long streamId,
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    OffsetToken nextOffset,
    boolean closed,
    Integer ownerNodeId) {
}
