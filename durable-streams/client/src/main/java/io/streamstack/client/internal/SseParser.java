package io.streamstack.client.internal;

import java.util.Objects;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class SseParser {

    private final BufferedReader reader;
    private boolean closed;

    public SseParser(InputStream input) {
        this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        this.closed = false;
    }

    public SseEvent nextEvent() throws IOException {
        if (closed) {
            return null;
        }
        StringBuilder data = new StringBuilder();
        String eventType = "message";
        String id = null;
        Integer retry = null;
        String line;
        while (Objects.nonNull((line = reader.readLine()))) {
            if (line.isEmpty()) {
                if (data.length() > 0) {
                    if (data.charAt(data.length() - 1) == '\n') {
                        data.setLength(data.length() - 1);
                    }
                    return new SseEvent(eventType, data.toString(), id, retry);
                }
                continue;
            }
            if (line.startsWith(":")) {
                continue;
            }
            int colonIndex = line.indexOf(':');
            String field;
            String value;
            if (colonIndex == -1) {
                field = line;
                value = "";
            } else {
                field = line.substring(0, colonIndex);
                value = line.substring(colonIndex + 1);
                if (value.startsWith(" ")) {
                    value = value.substring(1);
                }
            }
            switch (field) {
                case "event" -> eventType = value;
                case "data" -> data.append(value).append('\n');
                case "id" -> id = value;
                case "retry" -> {
                    try {
                        retry = Integer.parseInt(value);
                    } catch (NumberFormatException ignored) {
                    }
                }
                default -> {
                }
            }
        }
        closed = true;
        if (data.length() > 0) {
            if (data.charAt(data.length() - 1) == '\n') {
                data.setLength(data.length() - 1);
            }
            return new SseEvent(eventType, data.toString(), id, retry);
        }
        return null;
    }

    public boolean closed() {
        return closed;
    }

    public void close() throws IOException {
        closed = true;
        reader.close();
    }

    public static final class SseEvent {
        private final String event;
        private final String data;
        private final String id;
        private final Integer retry;
        public SseEvent(String event, String data, String id, Integer retry) {
            this.event = event;
            this.data = data;
            this.id = id;
            this.retry = retry;
        }
        public String event() {
            return event;
        }
        public String data() {
            return data;
        }
        public Optional<String> id() {
            return Optional.ofNullable(id);
        }
        public Optional<Integer> retry() {
            return Optional.ofNullable(retry);
        }
    }
}
