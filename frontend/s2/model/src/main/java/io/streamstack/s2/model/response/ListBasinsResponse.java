package io.streamstack.s2.model.response;

import java.util.Objects;
import java.util.List;

public record ListBasinsResponse(List<BasinResponse> basins, boolean hasMore) {

    public ListBasinsResponse {
        basins = Objects.isNull(basins) ? List.of() : List.copyOf(basins);
    }
}
