package io.streamstack.model.exception;

/**
 * Stream is closed and no longer accepts appends (HTTP 409 with Stream-Closed).
 */
public final class StreamClosedException extends DurableStreamException {
    private final String url;

    public StreamClosedException(String url) {
        super("stream closed: " + url, 409);
        this.url = url;
    }

    public StreamClosedException(String url, String message) {
        super(message, 409);
        this.url = url;
    }

    public String url() {
        return url;
    }
}
