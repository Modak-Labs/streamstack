package io.streamstack.ds.client.model;

import io.streamstack.ds.model.Offset;

public record CloseResult(Offset finalOffset, boolean alreadyClosed) {

}
