package io.streamstack.model;

import java.util.Objects;

public record SequencedRecord(long seq, RecordEnvelope envelope) {

    public SequencedRecord {
        Objects.requireNonNull(envelope, "envelope");
    }
}
