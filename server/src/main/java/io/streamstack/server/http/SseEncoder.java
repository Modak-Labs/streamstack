package io.streamstack.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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

    public byte[] dataEvent(byte[] payload) {
        StringBuilder sb = new StringBuilder("event: data\n");
        if (base64) {
            sb.append("data: ").append(Base64.getEncoder().encodeToString(payload)).append('\n');
        } else {
            String text = new String(payload, StandardCharsets.UTF_8);
            if (json) {
                text = '[' + text + ']';
            }
            for (String line : text.split("\r\n|\r|\n", -1)) {
                sb.append("data: ").append(line).append('\n');
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
            return ("event: control\ndata: " + MAPPER.writeValueAsString(node) + "\n\n")
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
}
