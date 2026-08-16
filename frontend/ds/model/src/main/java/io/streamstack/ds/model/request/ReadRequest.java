package io.streamstack.ds.model.request;

import io.streamstack.ds.model.LiveMode;
import io.streamstack.ds.model.Offset;

public record ReadRequest(
    Offset offset,
    LiveMode live,
    String cursor) {
}
