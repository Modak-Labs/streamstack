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

    public static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
