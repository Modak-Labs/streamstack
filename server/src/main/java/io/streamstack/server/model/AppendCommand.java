package io.streamstack.server.model;

public record AppendCommand(
    String contentType,
    byte[] body,
    String streamSeq,
    String producerId,
    Long producerEpoch,
    Long producerSeq,
    boolean close) {

    public AppendCommand {
        body = body == null ? new byte[0] : body;
    }

    public boolean hasProducer() {
        return producerId != null;
    }
}
