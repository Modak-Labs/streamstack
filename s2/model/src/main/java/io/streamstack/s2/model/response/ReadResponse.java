package io.streamstack.s2.model.response;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.streamstack.s2.model.StreamPosition;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReadResponse(List<SequencedRecord> records, StreamPosition tail) {

    public ReadResponse {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }
}
