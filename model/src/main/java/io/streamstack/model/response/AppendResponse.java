package io.streamstack.model.response;

import io.streamstack.model.Offset;

public record AppendResponse(
    Offset nextOffset,
    boolean appended,
    boolean closed,
    Long producerEpoch,
    Long producerSeq) {
}
