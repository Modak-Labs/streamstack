package io.streamstack.model.request;

import java.time.Instant;

public record CreateRequest(
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    boolean closed,
    byte[] initialBody) {

    public CreateRequest {
        initialBody = initialBody == null ? new byte[0] : initialBody;
    }
}
