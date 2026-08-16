package io.streamstack.server.model;

import java.util.Objects;

public final class StreamServiceException extends Exception {

    public enum Kind { NOT_FOUND, CONFLICT, CLOSED, BAD_REQUEST, FENCED, SEQUENCE_GAP, MATCH_FAILED, DURABILITY }
    private final Kind kind;
    private final OffsetToken nextOffset;
    private final boolean closed;
    private final Long producerEpoch;
    private final Long expectedSeq;
    private final Long receivedSeq;

    public StreamServiceException(Kind kind) {
        this(kind, null, false, null, null, null, kind.name());
    }

    public StreamServiceException(Kind kind, OffsetToken nextOffset, boolean closed) {
        this(kind, nextOffset, closed, null, null, null, kind.name());
    }

    public StreamServiceException(Kind kind, OffsetToken nextOffset, boolean closed, String message) {
        this(kind, nextOffset, closed, null, null, null, message);
    }

    public StreamServiceException(
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

    public static StreamServiceException fenced(long currentEpoch) {
        return new StreamServiceException(Kind.FENCED, null, false, currentEpoch, null, null, "Stale producer epoch");
    }

    public static StreamServiceException sequenceGap(long expected, long received) {
        return new StreamServiceException(
            Kind.SEQUENCE_GAP, null, false, null, expected, received, "Producer sequence gap");
    }

    public static StreamServiceException durability(Throwable cause) {
        if (cause instanceof StreamServiceException se) {
            return se;
        }

        String message = Objects.isNull(cause)
            ? "append not durable"
            : "append not durable: " + cause.getMessage();

        return new StreamServiceException(Kind.DURABILITY, null, false, message);
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
