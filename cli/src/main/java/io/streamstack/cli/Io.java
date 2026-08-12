package io.streamstack.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public final class Io {

    private Io() {
    }

    public static void eachStdinLine(Consumer<String> consumer) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;

        while ((line = reader.readLine()) != null) {
            consumer.accept(line);
        }
    }

    public static void out(String line) {
        System.out.println(line);
    }

    public static void err(String line) {
        System.err.println(line);
    }
}
