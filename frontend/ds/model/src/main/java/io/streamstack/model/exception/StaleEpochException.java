package io.streamstack.model.exception;

public final class StaleEpochException extends DurableStreamException {

    private final Long currentEpoch;

    public StaleEpochException(Long currentEpoch) {
        super("stale producer epoch; current=" + currentEpoch, 403);
        this.currentEpoch = currentEpoch;
    }

    public StaleEpochException(Long currentEpoch, String message) {
        super(message, 403);
        this.currentEpoch = currentEpoch;
    }

    public Long currentEpoch() {
        return currentEpoch;
    }
}
