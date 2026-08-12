package io.streamstack.s2.server;

import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.StreamPosition;
import io.streamstack.s2.model.response.ReadResponse;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class SseEncoder {

    private SseEncoder() {
    }

    public static byte[] batchEvent(
        ReadResponse response, Format format, long lastSeqNum, long count, long bytes) {
        return ("event: batch\nid: " + lastSeqNum + "," + count + "," + bytes
            + "\ndata: " + new String(S2Json.write(response, format), StandardCharsets.UTF_8)
            + "\n\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] pingEvent(StreamPosition tail) {
        return ("event: ping\ndata: " + new String(
            S2Json.write(Map.of("timestamp", System.currentTimeMillis(), "tail", tail), Format.RAW),
            StandardCharsets.UTF_8) + "\n\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] doneEvent() {
        return "data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8);
    }
}
