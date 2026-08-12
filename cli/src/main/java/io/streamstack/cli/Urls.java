package io.streamstack.cli;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class Urls {

    private Urls() {
    }

    public static String ds(String endpoint, String stream) {
        String path = stream.startsWith("/") ? stream : "/" + stream;

        return endpoint + path;
    }

    public static Resource s2(String uri) {
        String value = uri;

        if (value.startsWith("s2://")) {
            value = value.substring(5);
        }

        int slash = value.indexOf('/');

        if (slash <= 0 || slash == value.length() - 1) {
            throw new IllegalArgumentException("expected basin/stream, got: " + uri);
        }

        return new Resource(value.substring(0, slash), value.substring(slash + 1));
    }

    public static String basin(String uri) {
        String value = uri;

        if (value.startsWith("s2://")) {
            value = value.substring(5);
        }

        int slash = value.indexOf('/');

        if (slash >= 0) {
            value = value.substring(0, slash);
        }

        if (value.isBlank()) {
            throw new IllegalArgumentException("expected basin name, got: " + uri);
        }

        return value;
    }

    public record Resource(String basin, String stream) {
        public Resource {
            Objects.requireNonNull(basin, "basin");
            Objects.requireNonNull(stream, "stream");
        }
    }

    public static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
