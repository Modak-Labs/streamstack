package io.streamstack.client.model;

import java.util.Objects;

import io.streamstack.model.Offset;

import java.util.Iterator;
import java.util.List;

public final class JsonBatch<T> implements Iterable<T> {

    private final List<T> items;
    private final Offset nextOffset;
    private final boolean upToDate;
    private final String cursor;

    public JsonBatch(List<T> items, Offset nextOffset, boolean upToDate, String cursor) {
        this.items = Objects.isNull(items) ? List.of() : List.copyOf(items);
        this.nextOffset = nextOffset;
        this.upToDate = upToDate;
        this.cursor = cursor;
    }

    public List<T> items() {
        return items;
    }

    public int size() {
        return items.size();
    }

    public Offset nextOffset() {
        return nextOffset;
    }

    public boolean upToDate() {
        return upToDate;
    }

    public String cursor() {
        return cursor;
    }

    @Override
    public Iterator<T> iterator() {
        return items.iterator();
    }
}
