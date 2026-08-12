package io.streamstack.client;

import io.streamstack.model.exception.DurableStreamException;

public final class ParseErrorException extends DurableStreamException {

    public ParseErrorException(String message) {
        super(message);
    }

    public ParseErrorException(String message, Throwable cause) {
        super(message, cause);
    }
}
