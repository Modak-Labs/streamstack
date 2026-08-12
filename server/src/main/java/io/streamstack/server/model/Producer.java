package io.streamstack.server.model;

import java.util.Objects;

public record Producer(String producerId, long epoch, long seq) {

    public Producer {
        Objects.requireNonNull(producerId, "producerId");

        if (producerId.isEmpty()) {
            throw new IllegalArgumentException("producerId must not be empty");
        }

        if (epoch < 0 || seq < 0) {
            throw new IllegalArgumentException("epoch and seq must be >= 0");
        }
    }
}
