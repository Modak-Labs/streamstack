package io.streamstack.model.request;

public record AppendRequest(
    String contentType,
    byte[] body,
    String streamSeq,
    String producerId,
    Long producerEpoch,
    Long producerSeq,
    boolean close) {

    public AppendRequest {
        body = body == null ? new byte[0] : body;
    }

    public boolean hasProducer() {
        return producerId != null;
    }
}
