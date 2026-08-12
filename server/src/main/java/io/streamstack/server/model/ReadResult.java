package io.streamstack.server.model;

import java.util.Objects;
import java.util.ArrayList;
import java.util.List;

public record ReadResult(
    List<StreamRecord> records,
    String contentType,
    OffsetToken nextOffset,
    boolean upToDate,
    boolean closed) {

    public ReadResult {
        records = Objects.isNull(records) ? List.of() : List.copyOf(records);
    }

    public List<byte[]> payloads() {
        List<byte[]> out = new ArrayList<>(records.size());
        for (StreamRecord record : records) {
            out.add(record.payload());
        }
        return out;
    }

    public byte[] concatenated() {
        List<byte[]> messages = payloads();
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
