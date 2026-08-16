package io.streamstack.ss.model.response;

public record AppendResponse(long startSeq, long nextSeq, Long timestamp, Long producerEpoch, Long producerSeq) {
}
