package io.streamstack.server.model;

import java.util.List;
import java.util.Objects;

public record AppendCommand(
    String name,
    List<byte[]> payloads,
    String contentType,
    String streamSeq,
    Producer producer,
    boolean closeAfter) {

    public AppendCommand {
        Objects.requireNonNull(name, "name");
        payloads = payloads == null ? List.of() : List.copyOf(payloads);
        if (streamSeq != null && streamSeq.isEmpty()) {
            streamSeq = null;
        }
    }

    public boolean hasProducer() {
        return producer != null;
    }

    public byte[] concatenatedPayload() {
        if (payloads.isEmpty()) {
            return new byte[0];
        }
        if (payloads.size() == 1) {
            return payloads.get(0);
        }
        int total = 0;
        for (byte[] p : payloads) {
            total += p.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : payloads) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
