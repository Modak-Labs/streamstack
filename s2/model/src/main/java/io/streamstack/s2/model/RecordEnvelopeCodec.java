package io.streamstack.s2.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class RecordEnvelopeCodec {

    private static final byte VERSION = 1;

    private RecordEnvelopeCodec() {
    }

    public static byte[] encode(RecordEnvelope envelope) {
        int size = 1 + 8 + 4 + envelope.body().length;

        for (RecordHeader header : envelope.headers()) {
            size += 8 + header.name().length + header.value().length;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);

        buf.put(VERSION);
        buf.putLong(envelope.timestamp());
        buf.putInt(envelope.headers().size());

        for (RecordHeader header : envelope.headers()) {
            buf.putInt(header.name().length);
            buf.put(header.name());
            buf.putInt(header.value().length);
            buf.put(header.value());
        }

        buf.put(envelope.body());

        return buf.array();
    }

    public static RecordEnvelope decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte version = buf.get();

        if (version != VERSION) {
            throw new IllegalStateException("unknown record envelope version " + version);
        }

        long timestamp = buf.getLong();
        int headerCount = buf.getInt();
        List<RecordHeader> headers = new ArrayList<>(headerCount);

        for (int i = 0; i < headerCount; i++) {
            byte[] name = new byte[buf.getInt()];

            buf.get(name);
            byte[] value = new byte[buf.getInt()];

            buf.get(value);
            headers.add(new RecordHeader(name, value));
        }

        byte[] body = new byte[buf.remaining()];

        buf.get(body);

        return new RecordEnvelope(timestamp, headers, body);
    }

    public static long decodeTimestamp(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        byte version = buf.get();

        if (version != VERSION) {
            throw new IllegalStateException("unknown record envelope version " + version);
        }

        return buf.getLong();
    }
}
