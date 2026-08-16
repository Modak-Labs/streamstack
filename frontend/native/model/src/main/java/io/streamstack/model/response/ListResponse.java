package io.streamstack.model.response;

import java.util.List;
import java.util.Objects;

public record ListResponse(List<HeadResponse> streams, boolean hasMore) {

    public ListResponse {
        streams = Objects.isNull(streams) ? List.of() : List.copyOf(streams);
    }
}
