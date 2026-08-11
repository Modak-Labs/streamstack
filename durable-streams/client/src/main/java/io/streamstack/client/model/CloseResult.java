package io.streamstack.client.model;

import io.streamstack.model.Offset;

public record CloseResult(Offset finalOffset, boolean alreadyClosed) {
}
