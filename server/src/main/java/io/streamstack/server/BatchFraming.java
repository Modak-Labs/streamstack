package io.streamstack.server;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class BatchFraming {

    private static final byte VERSION = 1;

    private BatchFraming() {
    }

    public static byte[] encode(List<byte[]> records) {
        int size = 1 + 4;
        for (byte[] record : records) {
            size += 4 + record.length;
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put(VERSION);
        buf.putInt(records.size());
        for (byte[] record : records) {
            buf.putInt(record.length);
            buf.put(record);
        }
        return buf.array();
    }

    public static List<byte[]> decode(byte[] payload, int expectedCount) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte version = buf.get();
        if (version != VERSION) {
            throw new IllegalStateException("unknown batch frame version " + version);
        }
        int count = buf.getInt();
        if (count != expectedCount) {
            throw new IllegalStateException(
                "batch frame count " + count + " does not match batch count " + expectedCount);
        }
        List<byte[]> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] record = new byte[buf.getInt()];
            buf.get(record);
            out.add(record);
        }
        if (buf.hasRemaining()) {
            throw new IllegalStateException("trailing bytes after batch frames");
        }
        return out;
    }
}
