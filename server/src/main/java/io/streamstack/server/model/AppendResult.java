package io.streamstack.server.model;

public record AppendResult(
    OffsetToken nextOffset,
    boolean applied,
    boolean closed,
    Long producerEpoch,
    Long producerSeq) {
}
