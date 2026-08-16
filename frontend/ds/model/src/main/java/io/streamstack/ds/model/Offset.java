package io.streamstack.ds.model;

import java.util.Objects;

public final class Offset {

    public static final String BEGINNING = "-1";
    public static final String NOW = "now";
    private final String value;

    private Offset(String value) {
        this.value = Objects.requireNonNull(value, "value");
    }

    public static Offset of(String value) {
        if (Objects.isNull(value) || value.isEmpty()) {
            throw new IllegalArgumentException("empty offset");
        }

        return new Offset(value);
    }

    public static Offset beginning() {
        return new Offset(BEGINNING);
    }

    public String value() {
        return value;
    }

    public boolean isBeginning() {
        return BEGINNING.equals(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Offset that)) {
            return false;
        }

        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
