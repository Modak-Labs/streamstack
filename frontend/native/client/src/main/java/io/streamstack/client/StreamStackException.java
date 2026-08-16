package io.streamstack.client;

import java.util.Objects;

public final class StreamStackException extends RuntimeException {

    private final int status;
    private final String code;
    private final Long nextSeq;

    public StreamStackException(int status, String code, String message, Long nextSeq) {
        super(Objects.isNull(message) ? code : message);
        this.status = status;
        this.code = code;
        this.nextSeq = nextSeq;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public Long nextSeq() {
        return nextSeq;
    }

    public boolean isNotFound() {
        return status == 404;
    }

    public boolean isGone() {
        return status == 410;
    }

    public boolean isConflict() {
        return status == 409;
    }

    public boolean isTransport() {
        return status == 0;
    }
}
