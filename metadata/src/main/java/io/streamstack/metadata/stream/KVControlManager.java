package io.streamstack.metadata.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public final class KVControlManager {

    private final Map<String, byte[]> store = new HashMap<>();

    public byte[] put(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] copy = Arrays.copyOf(value, value.length);

        store.put(key, copy);

        return Arrays.copyOf(copy, copy.length);
    }

    public byte[] putIfAbsent(String key, byte[] value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        byte[] existing = store.get(key);

        if (Objects.nonNull(existing)) {
            return Arrays.copyOf(existing, existing.length);
        }

        byte[] copy = Arrays.copyOf(value, value.length);

        store.put(key, copy);

        return Arrays.copyOf(copy, copy.length);
    }

    public byte[] get(String key) {
        Objects.requireNonNull(key, "key");
        byte[] existing = store.get(key);

        return Objects.isNull(existing) ? null : Arrays.copyOf(existing, existing.length);
    }

    public byte[] delete(String key) {
        Objects.requireNonNull(key, "key");
        byte[] removed = store.remove(key);

        return Objects.isNull(removed) ? null : Arrays.copyOf(removed, removed.length);
    }

    public Map<String, byte[]> entries() {
        return store;
    }

    public Map<String, byte[]> list(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        Map<String, byte[]> out = new TreeMap<>();

        for (Map.Entry<String, byte[]> entry : store.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                out.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
            }
        }

        return out;
    }

    public Map<String, byte[]> snapshot() {
        Map<String, byte[]> copy = new TreeMap<>();

        for (Map.Entry<String, byte[]> entry : store.entrySet()) {
            copy.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }

        return copy;
    }

    public void replaceAll(Map<String, byte[]> entries) {
        store.clear();

        if (Objects.isNull(entries)) {
            return;
        }

        for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
            store.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
    }
}
