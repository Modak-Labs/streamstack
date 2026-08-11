package io.streamstack.model.exception;

/**
 * Stream already exists with conflicting config (HTTP 409 on create).
 */
public final class StreamExistsException extends DurableStreamException {
    private final String url;

    public StreamExistsException(String url) {
        super("stream exists: " + url, 409);
        this.url = url;
    }

    public StreamExistsException(String url, String message) {
        super(message, 409);
        this.url = url;
    }

    public String url() {
        return url;
    }
}
