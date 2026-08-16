package io.streamstack.ds.model.exception;

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
