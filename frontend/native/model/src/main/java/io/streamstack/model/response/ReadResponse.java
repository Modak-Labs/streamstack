package io.streamstack.model.response;

import io.streamstack.model.SequencedRecord;

import java.util.List;
import java.util.Objects;

public record ReadResponse(
    List<SequencedRecord> records,
    long nextSeq,
    boolean upToDate,
    boolean closed,
    String cursor) {

    public ReadResponse {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }

    public ReadResponse(List<SequencedRecord> records, long nextSeq, boolean upToDate, boolean closed) {
        this(records, nextSeq, upToDate, closed, null);
    }
}
