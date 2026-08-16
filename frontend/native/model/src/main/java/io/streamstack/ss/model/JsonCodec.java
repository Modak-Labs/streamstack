package io.streamstack.ss.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class JsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonCodec() {
    }

    public static byte[] encodeRead(List<SequencedRecord> records) {
        ArrayNode array = MAPPER.createArrayNode();

        for (SequencedRecord record : records) {
            ObjectNode node = array.addObject();

            node.put("seq", record.seq());
            node.put("timestamp", record.envelope().timestamp());

            if (!record.envelope().headers().isEmpty()) {
                ObjectNode headers = node.putObject("headers");

                record.envelope().headers().forEach(headers::put);
            }

            putBody(node, record.envelope().body());
        }

        return write(array);
    }

    public static List<SequencedRecord> decodeRead(byte[] payload) {
        JsonNode array = read(payload);
        List<SequencedRecord> records = new ArrayList<>(array.size());

        for (JsonNode node : array) {
            records.add(new SequencedRecord(
                node.path("seq").asLong(),
                new RecordEnvelope(node.path("timestamp").asLong(), headersOf(node), bodyOf(node))));
        }

        return records;
    }

    public static byte[] encodeAppend(List<RecordEnvelope> records) {
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode array = root.putArray("records");

        for (RecordEnvelope record : records) {
            ObjectNode node = array.addObject();

            if (!record.headers().isEmpty()) {
                ObjectNode headers = node.putObject("headers");

                record.headers().forEach(headers::put);
            }

            putBody(node, record.body());
        }

        return write(root);
    }

    public static List<RecordEnvelope> decodeAppend(byte[] payload) {
        JsonNode root = read(payload);
        JsonNode array = root.path("records");

        if (!array.isArray() || array.isEmpty()) {
            throw new IllegalArgumentException("records must be a non-empty array");
        }

        List<RecordEnvelope> records = new ArrayList<>(array.size());

        for (JsonNode node : array) {
            records.add(new RecordEnvelope(0, headersOf(node), bodyOf(node)));
        }

        return records;
    }

    private static void putBody(ObjectNode node, byte[] body) {
        String text = utf8(body);

        if (Objects.nonNull(text)) {
            node.put("body", text);
        } else {
            node.put("body_b64", Base64.getEncoder().encodeToString(body));
        }
    }

    private static byte[] bodyOf(JsonNode node) {
        JsonNode b64 = node.get("body_b64");

        if (Objects.nonNull(b64)) {
            return Base64.getDecoder().decode(b64.asText());
        }

        JsonNode body = node.get("body");

        return Objects.isNull(body) ? new byte[0] : body.asText().getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, String> headersOf(JsonNode node) {
        JsonNode headers = node.get("headers");

        if (Objects.isNull(headers) || !headers.isObject()) {
            return Map.of();
        }

        Map<String, String> out = new LinkedHashMap<>();

        headers.properties().forEach(e -> out.put(e.getKey(), e.getValue().asText()));

        return out;
    }

    private static String utf8(byte[] body) {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static byte[] write(JsonNode node) {
        try {
            return MAPPER.writeValueAsBytes(node);
        } catch (Exception e) {
            throw new IllegalStateException("failed to encode json", e);
        }
    }

    private static JsonNode read(byte[] payload) {
        try {
            return MAPPER.readTree(payload);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid JSON");
        }
    }
}
