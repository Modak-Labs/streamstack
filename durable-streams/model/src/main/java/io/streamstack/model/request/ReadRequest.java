package io.streamstack.model.request;

import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;

public record ReadRequest(
    Offset offset,
    LiveMode live,
    String cursor,
    Integer maxBytes) {
}
