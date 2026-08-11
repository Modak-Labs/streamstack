package io.streamstack.server.store;

public record ReadResult(
    byte[] body,
    String contentType,
    OffsetToken nextOffset,
    boolean upToDate,
    boolean closed) {
    public ReadResult {
        body = body == null ? new byte[0] : body;
    }
}
