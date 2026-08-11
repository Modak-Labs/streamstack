package io.streamstack.model.response;

import io.streamstack.model.Offset;

import java.time.Instant;

public record HeadResponse(
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    Offset nextOffset,
    boolean closed) {
}
