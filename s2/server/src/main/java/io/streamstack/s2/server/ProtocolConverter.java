package io.streamstack.s2.server;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.streamstack.s2.model.RecordEnvelope;
import io.streamstack.s2.model.RecordEnvelopeCodec;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.response.BasinResponse;
import io.streamstack.s2.model.response.SequencedRecord;
import io.streamstack.s2.model.response.StreamResponse;
import io.streamstack.server.model.StreamRecord;

import java.time.Instant;

public final class ProtocolConverter {

    private ProtocolConverter() {
    }

    public static byte[] toEnvelopeBytes(AppendRecord record, long timestamp) {
        return RecordEnvelopeCodec.encode(new RecordEnvelope(timestamp, record.headers(), record.body()));
    }

    public static SequencedRecord toSequencedRecord(StreamRecord record) {
        RecordEnvelope envelope = RecordEnvelopeCodec.decode(record.payload());
        return new SequencedRecord(
            record.offset().recordOffset(),
            envelope.timestamp(),
            envelope.headers(),
            envelope.body());
    }

    public static BasinResponse toBasinResponse(String basin, ObjectNode doc) {
        return new BasinResponse(basin, null, rfc3339(doc.path("created_at").asLong(0)), null, "active");
    }

    public static StreamResponse toStreamResponse(String stream, ObjectNode doc) {
        return new StreamResponse(stream, rfc3339(doc.path("created_at").asLong(0)), null, null);
    }

    private static String rfc3339(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).toString();
    }
}
