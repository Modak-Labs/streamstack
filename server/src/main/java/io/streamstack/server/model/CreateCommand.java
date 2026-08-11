package io.streamstack.server.model;

import java.time.Instant;
import java.util.Objects;

public record CreateCommand(
    String name,
    String contentType,
    Long ttlSeconds,
    Instant expiresAt,
    boolean closed,
    byte[] initialPayload) {

    public CreateCommand {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(contentType, "contentType");
        if (ttlSeconds != null && expiresAt != null) {
            throw new IllegalArgumentException("ttlSeconds and expiresAt are mutually exclusive");
        }
        if (ttlSeconds != null && ttlSeconds < 0) {
            throw new IllegalArgumentException("ttlSeconds must be >= 0");
        }
        initialPayload = initialPayload == null ? new byte[0] : initialPayload;
    }
}
