package io.streamstack.s2.model.response;

import java.util.Objects;
import java.util.List;

public record ListStreamsResponse(List<StreamResponse> streams, boolean hasMore) {

    public ListStreamsResponse {
        streams = Objects.isNull(streams) ? List.of() : List.copyOf(streams);
    }
}
