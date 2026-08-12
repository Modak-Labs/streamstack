package io.streamstack.server.model;

import java.util.Objects;

public final class OffsetToken {

    private static final int WIDTH = 20;
    private static final String BEGINNING = "-1";
    private final String value;
    private final long recordOffset;

    private OffsetToken(String value, long recordOffset) {
        this.value = value;
        this.recordOffset = recordOffset;
    }

    public static OffsetToken beginning() {
        return ofRecordOffset(0);
    }

    public static OffsetToken ofRecordOffset(long recordOffset) {
        if (recordOffset < 0) {
            throw new IllegalArgumentException("record offset must be >= 0");
        }

        return new OffsetToken(String.format("%0" + WIDTH + "d", recordOffset), recordOffset);
    }

    public static OffsetToken parse(String raw) {
        if (Objects.isNull(raw) || BEGINNING.equals(raw)) {
            return beginning();
        }

        if (raw.isEmpty()) {
            throw new IllegalArgumentException("empty offset");
        }

        try {
            long offset = Long.parseLong(raw);

            if (offset < 0) {
                return beginning();
            }

            return ofRecordOffset(offset);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid offset token: " + raw);
        }
    }

    public String value() {
        return value;
    }

    public long recordOffset() {
        return recordOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof OffsetToken that)) {
            return false;
        }

        return recordOffset == that.recordOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordOffset);
    }

    @Override
    public String toString() {
        return value;
    }
}
