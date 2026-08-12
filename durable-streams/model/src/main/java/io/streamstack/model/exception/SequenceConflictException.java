package io.streamstack.model.exception;

public final class SequenceConflictException extends DurableStreamException {

    private final Long expectedSeq;
    private final Long receivedSeq;

    public SequenceConflictException(Long expectedSeq, Long receivedSeq) {
        super(message(expectedSeq, receivedSeq), 409);
        this.expectedSeq = expectedSeq;
        this.receivedSeq = receivedSeq;
    }

    public SequenceConflictException(Long expectedSeq, Long receivedSeq, String message) {
        super(message, 409);
        this.expectedSeq = expectedSeq;
        this.receivedSeq = receivedSeq;
    }

    public Long expectedSeq() {
        return expectedSeq;
    }

    public Long receivedSeq() {
        return receivedSeq;
    }

    private static String message(Long expectedSeq, Long receivedSeq) {
        return "sequence conflict: expected=" + expectedSeq + ", received=" + receivedSeq;
    }
}
