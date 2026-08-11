package io.streamstack.model.response;

import io.streamstack.model.Offset;

import java.util.List;

public record ReadResponse(
    List<byte[]> messages,
    String contentType,
    Offset nextOffset,
    boolean upToDate,
    boolean closed) {

    public ReadResponse {
        messages = messages == null ? List.of() : List.copyOf(messages);
    }

    public byte[] concatenated() {
        if (messages.isEmpty()) {
            return new byte[0];
        }
        if (messages.size() == 1) {
            return messages.get(0);
        }
        int total = 0;
        for (byte[] m : messages) {
            total += m.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] m : messages) {
            System.arraycopy(m, 0, out, pos, m.length);
            pos += m.length;
        }
        return out;
    }
}
