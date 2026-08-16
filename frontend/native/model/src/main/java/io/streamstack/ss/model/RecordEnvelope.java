package io.streamstack.ss.model;

import java.util.Map;
import java.util.Objects;

public record RecordEnvelope(long timestamp, Map<String, String> headers, byte[] body) {

    public RecordEnvelope {
        headers = Objects.isNull(headers) ? Map.of() : Map.copyOf(headers);
        body = Objects.isNull(body) ? new byte[0] : body;
    }
}
