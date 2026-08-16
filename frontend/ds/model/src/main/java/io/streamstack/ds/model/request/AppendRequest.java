package io.streamstack.ds.model.request;

import java.util.Objects;

public record AppendRequest(
    String contentType,
    byte[] body,
    String streamSeq,
    String producerId,
    Long producerEpoch,
    Long producerSeq,
    boolean close) {

    public AppendRequest {
        body = Objects.isNull(body) ? new byte[0] : body;
    }
}
