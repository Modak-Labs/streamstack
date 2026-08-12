package io.streamstack.s2.model.request;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AppendRequest(
    List<AppendRecord> records,
    Long matchSeqNum,
    String fencingToken) {

    public AppendRequest {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }
}
