package io.streamstack.ds.model.response;

import io.streamstack.ds.model.Offset;

import java.time.Instant;

public record HeadResponse(
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    Offset nextOffset,
    boolean closed) {
}
