package io.streamstack.metadata.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Replicated key-value store for protocol-level stream registry data.
 * Mutations must only happen on the Raft apply path.
 */
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
        if (existing != null) {
            return Arrays.copyOf(existing, existing.length);
        }
        byte[] copy = Arrays.copyOf(value, value.length);
        store.put(key, copy);
        return Arrays.copyOf(copy, copy.length);
    }

    public byte[] get(String key) {
        Objects.requireNonNull(key, "key");
        byte[] existing = store.get(key);
        return existing == null ? null : Arrays.copyOf(existing, existing.length);
    }

    public byte[] delete(String key) {
        Objects.requireNonNull(key, "key");
        byte[] removed = store.remove(key);
        return removed == null ? null : Arrays.copyOf(removed, removed.length);
    }

    public Map<String, byte[]> entries() {
        return store;
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
        if (entries == null) {
            return;
        }
        for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
            store.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
    }
}
