package io.streamstack.s2.model.response;

public record ErrorResponse(String code, String message, String resource) {

    public ErrorResponse(String code, String message) {
        this(code, message, null);
    }
}
