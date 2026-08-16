package io.streamstack.model;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CodecTest {

    @Test
    void envelopeRoundTrip() {
        RecordEnvelope envelope = new RecordEnvelope(
            1700000000123L, Map.of("k1", "v1", "k2", "v2"), "payload".getBytes(StandardCharsets.UTF_8));
        byte[] encoded = RecordEnvelopeCodec.encode(envelope);
        RecordEnvelope decoded = RecordEnvelopeCodec.decode(encoded);

        assertEquals(envelope.timestamp(), decoded.timestamp());
        assertEquals(envelope.headers(), decoded.headers());
        assertArrayEquals(envelope.body(), decoded.body());
        assertEquals(envelope.timestamp(), RecordEnvelopeCodec.decodeTimestamp(encoded));
    }

    @Test
    void binaryBatchRoundTrip() {
        List<RecordEnvelope> appended = List.of(
            new RecordEnvelope(0, Map.of("a", "1"), "one".getBytes(StandardCharsets.UTF_8)),
            new RecordEnvelope(0, Map.of(), new byte[] {0, 1, 2, (byte) 0xff}));
        List<RecordEnvelope> decoded = BatchCodec.decodeAppend(BatchCodec.encodeAppend(appended));

        assertEquals(2, decoded.size());
        assertEquals(Map.of("a", "1"), decoded.get(0).headers());
        assertArrayEquals(appended.get(1).body(), decoded.get(1).body());

        List<SequencedRecord> sequenced = List.of(
            new SequencedRecord(7, new RecordEnvelope(42L, Map.of("h", "x"), "r".getBytes(StandardCharsets.UTF_8))),
            new SequencedRecord(8, new RecordEnvelope(43L, Map.of(), new byte[0])));
        List<SequencedRecord> readBack = BatchCodec.decodeRead(BatchCodec.encodeRead(sequenced));

        assertEquals(7, readBack.get(0).seq());
        assertEquals(42L, readBack.get(0).envelope().timestamp());
        assertEquals(Map.of("h", "x"), readBack.get(0).envelope().headers());
        assertEquals(8, readBack.get(1).seq());
        assertEquals(0, readBack.get(1).envelope().body().length);
    }

    @Test
    void jsonRoundTrip() {
        List<SequencedRecord> records = List.of(
            new SequencedRecord(1, new RecordEnvelope(10L, Map.of("k", "v"), "text".getBytes(StandardCharsets.UTF_8))),
            new SequencedRecord(2, new RecordEnvelope(11L, Map.of(), new byte[] {(byte) 0xC3, (byte) 0x28})));
        List<SequencedRecord> decoded = JsonCodec.decodeRead(JsonCodec.encodeRead(records));

        assertEquals(1, decoded.get(0).seq());
        assertEquals("text", new String(decoded.get(0).envelope().body(), StandardCharsets.UTF_8));
        assertEquals(Map.of("k", "v"), decoded.get(0).envelope().headers());
        assertArrayEquals(records.get(1).envelope().body(), decoded.get(1).envelope().body());
        assertTrue(new String(JsonCodec.encodeRead(records), StandardCharsets.UTF_8).contains("body_b64"));

        List<RecordEnvelope> append = JsonCodec.decodeAppend(JsonCodec.encodeAppend(List.of(
            new RecordEnvelope(0, Map.of("h", "1"), "a".getBytes(StandardCharsets.UTF_8)))));

        assertEquals(1, append.size());
        assertEquals(Map.of("h", "1"), append.get(0).headers());
        assertThrows(IllegalArgumentException.class,
            () -> JsonCodec.decodeAppend("{\"records\":[]}".getBytes(StandardCharsets.UTF_8)));
    }
}
