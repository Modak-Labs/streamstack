package io.streamstack.model.exception;

/**
 * Requested offset was pruned / expired (HTTP 410).
 */
public final class OffsetGoneException extends DurableStreamException {
    private final String url;
    private final String offset;

    public OffsetGoneException(String url, String offset) {
        super("offset gone: url=" + url + ", offset=" + offset, 410);
        this.url = url;
        this.offset = offset;
    }

    public OffsetGoneException(String url, String offset, String message) {
        super(message, 410);
        this.url = url;
        this.offset = offset;
    }

    public String url() {
        return url;
    }

    public String offset() {
        return offset;
    }
}
