package io.streamstack.model.response;

public record AppendResponse(long startSeq, long nextSeq, Long timestamp, Long producerEpoch, Long producerSeq) {
}
