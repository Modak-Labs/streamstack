package io.streamstack.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RecordEnvelopeCodec {

    private static final byte VERSION = 1;

    private RecordEnvelopeCodec() {
    }

    public static byte[] encode(RecordEnvelope envelope) {
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + headersSize(envelope.headers()) + envelope.body().length);

        buf.put(VERSION);
        buf.putLong(envelope.timestamp());
        putHeaders(buf, envelope.headers());
        buf.put(envelope.body());

        return buf.array();
    }

    public static RecordEnvelope decode(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);

        checkVersion(buf.get());
        long timestamp = buf.getLong();
        Map<String, String> headers = getHeaders(buf);
        byte[] body = new byte[buf.remaining()];

        buf.get(body);

        return new RecordEnvelope(timestamp, headers, body);
    }

    public static long decodeTimestamp(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);

        checkVersion(buf.get());

        return buf.getLong();
    }

    static int headersSize(Map<String, String> headers) {
        int size = 4;

        for (Map.Entry<String, String> e : headers.entrySet()) {
            size += 8 + e.getKey().getBytes(StandardCharsets.UTF_8).length
                + e.getValue().getBytes(StandardCharsets.UTF_8).length;
        }

        return size;
    }

    static void putHeaders(ByteBuffer buf, Map<String, String> headers) {
        buf.putInt(headers.size());

        for (Map.Entry<String, String> e : headers.entrySet()) {
            byte[] name = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] value = e.getValue().getBytes(StandardCharsets.UTF_8);

            buf.putInt(name.length);
            buf.put(name);
            buf.putInt(value.length);
            buf.put(value);
        }
    }

    static Map<String, String> getHeaders(ByteBuffer buf) {
        int count = buf.getInt();
        Map<String, String> headers = new LinkedHashMap<>(Math.max(count, 1));

        for (int i = 0; i < count; i++) {
            byte[] name = new byte[buf.getInt()];

            buf.get(name);
            byte[] value = new byte[buf.getInt()];

            buf.get(value);
            headers.put(new String(name, StandardCharsets.UTF_8), new String(value, StandardCharsets.UTF_8));
        }

        return headers;
    }

    private static void checkVersion(byte version) {
        if (version != VERSION) {
            throw new IllegalStateException("unknown record envelope version " + version);
        }
    }
}
