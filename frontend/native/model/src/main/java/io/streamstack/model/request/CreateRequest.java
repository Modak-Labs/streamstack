package io.streamstack.model.request;

import java.time.Instant;

public record CreateRequest(
    String contentType,
    Long ttlSeconds,
    Instant expiresAt) {

    public CreateRequest(String contentType) {
        this(contentType, null, null);
    }
}
