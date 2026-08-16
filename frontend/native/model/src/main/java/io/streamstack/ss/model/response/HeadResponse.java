package io.streamstack.ss.model.response;

import java.time.Instant;

public record HeadResponse(
    String name,
    String contentType,
    long startSeq,
    long nextSeq,
    boolean closed,
    Long ttlSeconds,
    Instant expiresAt) {
}
