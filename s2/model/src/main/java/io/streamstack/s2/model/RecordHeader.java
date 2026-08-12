package io.streamstack.s2.model;

import java.util.Objects;

public record RecordHeader(byte[] name, byte[] value) {

    public RecordHeader {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(value, "value");
    }
}
