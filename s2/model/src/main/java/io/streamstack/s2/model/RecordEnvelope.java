package io.streamstack.s2.model;

import java.util.Objects;
import java.util.List;

public record RecordEnvelope(long timestamp, List<RecordHeader> headers, byte[] body) {

    public RecordEnvelope {
        headers = Objects.isNull(headers) ? List.of() : List.copyOf(headers);
        body = Objects.isNull(body) ? new byte[0] : body;
    }
}
