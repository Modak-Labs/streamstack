package io.streamstack.ss.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.streamstack.ss.model.JsonCodec;
import io.streamstack.ss.model.SequencedRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

final class SseEncoder {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SseEncoder() {
    }

    static byte[] dataEvent(List<SequencedRecord> records, long nextSeq) {
        StringBuilder sb = new StringBuilder("event: data\nid: ").append(nextSeq).append('\n');
        String json = new String(JsonCodec.encodeRead(records), StandardCharsets.UTF_8);

        for (String line : json.split("\r\n|\r|\n", -1)) {
            sb.append("data:").append(line).append('\n');
        }

        sb.append('\n');

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    static byte[] controlEvent(long nextSeq, boolean upToDate, boolean closed) {
        ObjectNode node = MAPPER.createObjectNode();

        node.put("next_seq", nextSeq);
        node.put("up_to_date", upToDate);

        if (closed) {
            node.put("closed", true);
        }

        try {
            return ("event: control\nid: " + nextSeq + "\ndata:" + MAPPER.writeValueAsString(node) + "\n\n")
                .getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("failed to encode control event", e);
        }
    }
}
