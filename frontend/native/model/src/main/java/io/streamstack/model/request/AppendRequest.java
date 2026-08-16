package io.streamstack.model.request;

import io.streamstack.model.RecordEnvelope;

import java.util.List;
import java.util.Objects;

public record AppendRequest(
    List<RecordEnvelope> records,
    Long matchSeq,
    String producerId,
    Long producerEpoch,
    Long producerSeq) {

    public AppendRequest {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }

    public AppendRequest(List<RecordEnvelope> records) {
        this(records, null, null, null, null);
    }

    public AppendRequest(List<RecordEnvelope> records, Long matchSeq) {
        this(records, matchSeq, null, null, null);
    }
}
