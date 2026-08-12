package io.streamstack.s2.model.exception;

public final class InvalidException extends S2Exception {

    public InvalidException(String message) {
        super(422, "invalid", message);
    }
}
