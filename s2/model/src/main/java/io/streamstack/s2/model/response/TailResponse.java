package io.streamstack.s2.model.response;

import io.streamstack.s2.model.StreamPosition;

public record TailResponse(StreamPosition tail) {

}
