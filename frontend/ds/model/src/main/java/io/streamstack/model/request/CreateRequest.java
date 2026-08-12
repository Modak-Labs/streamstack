package io.streamstack.model.request;

import java.util.Objects;
import java.time.Instant;

public record CreateRequest(
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    boolean closed,
    byte[] initialBody) {

    public CreateRequest {
        initialBody = Objects.isNull(initialBody) ? new byte[0] : initialBody;
    }
}
