package io.streamstack.server.model;

import java.util.Objects;

public record StreamRecord(OffsetToken offset, byte[] payload) {

    public StreamRecord {
        Objects.requireNonNull(offset, "offset");
        payload = Objects.isNull(payload) ? new byte[0] : payload;
    }
}
