package io.streamstack.ds.model.exception;

import java.util.Optional;

public class DurableStreamException extends RuntimeException {

    private final Integer statusCode;

    public DurableStreamException(String message) {
        this(message, null, null);
    }

    public DurableStreamException(String message, Throwable cause) {
        this(message, null, cause);
    }

    public DurableStreamException(String message, Integer statusCode) {
        this(message, statusCode, null);
    }

    public DurableStreamException(String message, Integer statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public Optional<Integer> statusCode() {
        return Optional.ofNullable(statusCode);
    }
}
