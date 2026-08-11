package io.streamstack.server.model;

public record AppendResult(
    OffsetToken nextOffset,
    boolean appended,
    boolean closed,
    Long producerEpoch,
    Long producerSeq) {
}
