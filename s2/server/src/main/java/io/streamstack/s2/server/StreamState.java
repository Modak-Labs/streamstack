package io.streamstack.s2.server;

import java.util.concurrent.ConcurrentHashMap;

final class StreamState {

    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> lastTimestamps = new ConcurrentHashMap<>();
    Object lock(String coreName) {
        return locks.computeIfAbsent(coreName, n -> new Object());
    }

    Long cachedTimestamp(String coreName) {
        return lastTimestamps.get(coreName);
    }

    void cacheTimestamp(String coreName, long timestamp) {
        lastTimestamps.put(coreName, timestamp);
    }

    void invalidate(String coreName) {
        lastTimestamps.remove(coreName);
    }
}
