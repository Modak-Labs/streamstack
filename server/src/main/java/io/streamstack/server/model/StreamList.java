package io.streamstack.server.model;

import java.util.List;
import java.util.Objects;

public record StreamList(List<StreamMeta> streams, boolean hasMore) {

    public StreamList {
        streams = Objects.isNull(streams) ? List.of() : List.copyOf(streams);
    }
}
