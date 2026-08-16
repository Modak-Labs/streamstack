package io.streamstack.ss.model.response;

import io.streamstack.ss.model.SequencedRecord;

import java.util.List;
import java.util.Objects;

public record ReadResponse(List<SequencedRecord> records, long nextSeq, boolean upToDate, boolean closed) {

    public ReadResponse {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }
}
