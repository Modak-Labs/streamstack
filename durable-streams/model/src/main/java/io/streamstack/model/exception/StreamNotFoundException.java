package io.streamstack.model.exception;

/**
 * Stream does not exist (HTTP 404).
 */
public final class StreamNotFoundException extends DurableStreamException {
    private final String url;

    public StreamNotFoundException(String url) {
        super("stream not found: " + url, 404);
        this.url = url;
    }

    public StreamNotFoundException(String url, String message) {
        super(message, 404);
        this.url = url;
    }

    public String url() {
        return url;
    }
}
