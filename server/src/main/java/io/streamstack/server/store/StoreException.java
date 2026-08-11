package io.streamstack.server.store;

public final class StoreException extends Exception {
    public enum Kind { NOT_FOUND, CONFLICT, CLOSED, BAD_REQUEST }

    private final Kind kind;
    private final OffsetToken nextOffset;
    private final boolean closed;

    public StoreException(Kind kind) {
        this(kind, null, false, kind.name());
    }

    public StoreException(Kind kind, OffsetToken nextOffset, boolean closed) {
        this(kind, nextOffset, closed, kind.name());
    }

    public StoreException(Kind kind, OffsetToken nextOffset, boolean closed, String message) {
        super(message);
        this.kind = kind;
        this.nextOffset = nextOffset;
        this.closed = closed;
    }

    public Kind kind() {
        return kind;
    }

    public OffsetToken nextOffset() {
        return nextOffset;
    }

    public boolean closed() {
        return closed;
    }
}
