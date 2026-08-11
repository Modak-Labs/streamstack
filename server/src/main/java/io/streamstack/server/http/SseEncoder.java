package io.streamstack.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

public final class SseEncoder {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final boolean json;
    private final boolean base64;

    public SseEncoder(String contentType) {
        String mime = mimeOf(contentType);
        this.json = isJson(mime);
        this.base64 = !json && !mime.startsWith("text/");
    }

    public boolean base64() {
        return base64;
    }

    public byte[] dataEvent(List<byte[]> messages) {
        // No space after "data:" — clients strip at most one leading space; conformance
        // and the reference server omit it so payloads match exactly.
        StringBuilder sb = new StringBuilder("event: data\n");
        if (base64) {
            byte[] payload = concat(messages);
            sb.append("data:").append(Base64.getEncoder().encodeToString(payload)).append('\n');
        } else if (json) {
            String array = new String(jsonArrayBody(messages), StandardCharsets.UTF_8);
            for (String line : array.split("\r\n|\r|\n", -1)) {
                sb.append("data:").append(line).append('\n');
            }
        } else {
            String text = new String(concat(messages), StandardCharsets.UTF_8);
            for (String line : text.split("\r\n|\r|\n", -1)) {
                sb.append("data:").append(line).append('\n');
            }
        }
        sb.append('\n');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    public byte[] controlEvent(String nextOffset, Long cursor, boolean upToDate, boolean closed) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("streamNextOffset", nextOffset);
        if (!closed && cursor != null) {
            node.put("streamCursor", Long.toString(cursor));
        }
        node.put("upToDate", upToDate);
        if (closed) {
            node.put("streamClosed", true);
        }
        try {
            return ("event: control\ndata:" + MAPPER.writeValueAsString(node) + "\n\n")
                .getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to encode control event", e);
        }
    }

    public static boolean isJson(String mime) {
        return "application/json".equals(mime) || mime.endsWith("+json");
    }

    public static String mimeOf(String contentType) {
        if (contentType == null) {
            return "";
        }
        int semi = contentType.indexOf(';');
        return (semi >= 0 ? contentType.substring(0, semi) : contentType).trim().toLowerCase(Locale.ROOT);
    }

    public static byte[] jsonArrayBody(List<byte[]> messages) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < messages.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(new String(messages.get(i), StandardCharsets.UTF_8));
        }
        sb.append(']');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(List<byte[]> messages) {
        if (messages == null || messages.isEmpty()) {
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
