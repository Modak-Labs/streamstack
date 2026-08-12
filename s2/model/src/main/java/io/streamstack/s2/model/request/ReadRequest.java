package io.streamstack.s2.model.request;

public record ReadRequest(
    Long seqNum,
    Long timestamp,
    Long tailOffset,
    boolean clamp,
    Long count,
    Long bytes,
    Long until,
    Long waitSeconds) {
}
