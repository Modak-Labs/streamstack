package io.streamstack.ds.model.response;

import io.streamstack.ds.model.Offset;

public record AppendResponse(
    Offset nextOffset,
    boolean appended,
    boolean closed,
    Long producerEpoch,
    Long producerSeq) {
}
