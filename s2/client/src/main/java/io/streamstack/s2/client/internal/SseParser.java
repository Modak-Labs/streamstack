package io.streamstack.s2.client.internal;

import java.util.Objects;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class SseParser implements Closeable {

    private final BufferedReader reader;

    public SseParser(InputStream in) {
        this.reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    public Optional<Event> next() throws IOException {
        String event = null;
        String id = null;
        StringBuilder data = new StringBuilder();
        String line;

        while (Objects.nonNull((line = reader.readLine()))) {
            if (line.isEmpty()) {
                if (Objects.isNull(event) && Objects.isNull(id) && data.isEmpty()) {
                    continue;
                }

                return Optional.of(new Event(event, id, data.toString()));
            }

            if (line.startsWith("event:")) {
                event = line.substring(6).trim();
            } else if (line.startsWith("id:")) {
                id = line.substring(3).trim();
            } else if (line.startsWith("data:")) {
                if (!data.isEmpty()) {
                    data.append('\n');
                }

                String payload = line.length() > 5 && line.charAt(5) == ' '
                    ? line.substring(6)
                    : line.substring(5).trim();
                data.append(payload);
            }
        }

        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public record Event(String event, String id, String data) {
        public boolean done() {
            return "[DONE]".equals(data);
        }
    }
}
