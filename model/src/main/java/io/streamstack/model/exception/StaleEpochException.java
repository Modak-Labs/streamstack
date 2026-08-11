package io.streamstack.model.exception;

/**
 * Producer fenced by a newer epoch (HTTP 403).
 */
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
