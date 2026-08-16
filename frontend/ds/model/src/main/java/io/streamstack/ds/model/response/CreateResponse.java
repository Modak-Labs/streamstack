package io.streamstack.ds.model.response;

import io.streamstack.ds.model.Offset;

public record CreateResponse(
    boolean created,
    String contentType,
    Offset nextOffset,
    boolean closed) {
}
