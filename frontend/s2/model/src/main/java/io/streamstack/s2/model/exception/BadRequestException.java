package io.streamstack.s2.model.exception;

public final class BadRequestException extends S2Exception {

    public BadRequestException(String code, String message) {
        super(400, code, message);
    }
}
