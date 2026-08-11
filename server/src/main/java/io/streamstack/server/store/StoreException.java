package io.streamstack.server.store;

import io.streamstack.server.model.OffsetToken;

public final class StoreException extends Exception {
    public enum Kind { NOT_FOUND, CONFLICT, CLOSED, BAD_REQUEST, FENCED, SEQUENCE_GAP }

    private final Kind kind;
    private final OffsetToken nextOffset;
    private final boolean closed;
    private final Long producerEpoch;
    private final Long expectedSeq;
    private final Long receivedSeq;

    public StoreException(Kind kind) {
        this(kind, null, false, null, null, null, kind.name());
    }

    public StoreException(Kind kind, OffsetToken nextOffset, boolean closed) {
        this(kind, nextOffset, closed, null, null, null, kind.name());
    }

    public StoreException(Kind kind, OffsetToken nextOffset, boolean closed, String message) {
        this(kind, nextOffset, closed, null, null, null, message);
    }

    public StoreException(
        Kind kind,
        OffsetToken nextOffset,
        boolean closed,
        Long producerEpoch,
        Long expectedSeq,
        Long receivedSeq,
        String message) {
        super(message);
        this.kind = kind;
        this.nextOffset = nextOffset;
        this.closed = closed;
        this.producerEpoch = producerEpoch;
        this.expectedSeq = expectedSeq;
        this.receivedSeq = receivedSeq;
    }

    public static StoreException fenced(long currentEpoch) {
        return new StoreException(Kind.FENCED, null, false, currentEpoch, null, null, "Stale producer epoch");
    }

    public static StoreException sequenceGap(long expected, long received) {
        return new StoreException(Kind.SEQUENCE_GAP, null, false, null, expected, received, "Producer sequence gap");
    }

    public Kind kind() {
        return kind;
    }

    public OffsetToken nextOffset() {
        return nextOffset;
    }

    public boolean closed() {
        return closed;
    }

    public Long producerEpoch() {
        return producerEpoch;
    }

    public Long expectedSeq() {
        return expectedSeq;
    }

    public Long receivedSeq() {
        return receivedSeq;
    }
}
