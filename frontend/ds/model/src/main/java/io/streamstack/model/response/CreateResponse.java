package io.streamstack.model.response;

import io.streamstack.model.Offset;

public record CreateResponse(
    boolean created,
    String contentType,
    Offset nextOffset,
    boolean closed) {
}
