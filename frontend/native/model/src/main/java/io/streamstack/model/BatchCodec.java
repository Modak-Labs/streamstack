package io.streamstack.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class BatchCodec {

    private static final byte VERSION = 1;

    private BatchCodec() {
    }

    public static byte[] encodeAppend(List<RecordEnvelope> records) {
        int size = 1 + 4;

        for (RecordEnvelope record : records) {
            size += 4 + RecordEnvelopeCodec.headersSize(record.headers()) + 4 + record.body().length;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);

        buf.put(VERSION);
        buf.putInt(records.size());

        for (RecordEnvelope record : records) {
            RecordEnvelopeCodec.putHeaders(buf, record.headers());
            buf.putInt(record.body().length);
            buf.put(record.body());
        }

        return buf.array();
    }

    public static List<RecordEnvelope> decodeAppend(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);

        checkVersion(buf.get());
        int count = buf.getInt();
        List<RecordEnvelope> records = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            Map<String, String> headers = RecordEnvelopeCodec.getHeaders(buf);
            byte[] body = new byte[buf.getInt()];

            buf.get(body);
            records.add(new RecordEnvelope(0, headers, body));
        }

        return records;
    }

    public static byte[] encodeRead(List<SequencedRecord> records) {
        int size = 1 + 4;

        for (SequencedRecord record : records) {
            size += 8 + 8 + RecordEnvelopeCodec.headersSize(record.envelope().headers())
                + 4 + record.envelope().body().length;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);

        buf.put(VERSION);
        buf.putInt(records.size());

        for (SequencedRecord record : records) {
            buf.putLong(record.seq());
            buf.putLong(record.envelope().timestamp());
            RecordEnvelopeCodec.putHeaders(buf, record.envelope().headers());
            buf.putInt(record.envelope().body().length);
            buf.put(record.envelope().body());
        }

        return buf.array();
    }

    public static List<SequencedRecord> decodeRead(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);

        checkVersion(buf.get());
        int count = buf.getInt();
        List<SequencedRecord> records = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            long seq = buf.getLong();
            long timestamp = buf.getLong();
            Map<String, String> headers = RecordEnvelopeCodec.getHeaders(buf);
            byte[] body = new byte[buf.getInt()];

            buf.get(body);
            records.add(new SequencedRecord(seq, new RecordEnvelope(timestamp, headers, body)));
        }

        return records;
    }

    private static void checkVersion(byte version) {
        if (version != VERSION) {
            throw new IllegalStateException("unknown batch version " + version);
        }
    }
}
