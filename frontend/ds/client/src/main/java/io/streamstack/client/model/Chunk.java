package io.streamstack.client.model;

import java.util.Objects;

import io.streamstack.model.Offset;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

public final class Chunk {

    private final byte[] data;
    private final Offset nextOffset;
    private final boolean upToDate;
    private final boolean closed;
    private final String cursor;
    private final int statusCode;
    private final Map<String, String> headers;

    public Chunk(
        byte[] data,
        Offset nextOffset,
        boolean upToDate,
        boolean closed,
        String cursor,
        int statusCode,
        Map<String, String> headers) {
        this.data = Objects.isNull(data) ? new byte[0] : data;
        this.nextOffset = nextOffset;
        this.upToDate = upToDate;
        this.closed = closed;
        this.cursor = cursor;
        this.statusCode = statusCode;
        this.headers = Objects.isNull(headers) ? Map.of() : Map.copyOf(headers);
    }

    public byte[] data() {
        return data;
    }

    public String dataAsString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    public Offset nextOffset() {
        return nextOffset;
    }

    public boolean upToDate() {
        return upToDate;
    }

    public boolean closed() {
        return closed;
    }

    public Optional<String> cursor() {
        return Optional.ofNullable(cursor);
    }

    public int statusCode() {
        return statusCode;
    }

    public Map<String, String> headers() {
        return headers;
    }
}
