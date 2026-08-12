package io.streamstack.client;

import java.util.Objects;

import io.streamstack.client.model.Chunk;
import io.streamstack.client.model.JsonBatch;
import io.streamstack.model.Offset;
import io.streamstack.model.exception.DurableStreamException;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class JsonIterator<T> implements Iterator<JsonBatch<T>>, Iterable<JsonBatch<T>>, AutoCloseable {

    private final ChunkIterator chunkIterator;
    private final Function<String, List<T>> parser;
    private JsonBatch<T> nextBatch;
    private boolean hasNextComputed;

    public JsonIterator(ChunkIterator chunkIterator, Function<String, List<T>> parser) {
        this.chunkIterator = chunkIterator;
        this.parser = parser;
    }

    @Override
    public Iterator<JsonBatch<T>> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (hasNextComputed) {
            return Objects.nonNull(nextBatch);
        }
        hasNextComputed = true;
        if (!chunkIterator.hasNext()) {
            nextBatch = null;
            return false;
        }
        nextBatch = parseChunk(chunkIterator.next());
        return true;
    }

    @Override
    public JsonBatch<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextComputed = false;
        JsonBatch<T> result = nextBatch;
        nextBatch = null;
        return result;
    }

    public JsonBatch<T> poll(Duration timeout) {
        Chunk chunk = chunkIterator.poll(timeout);
        if (Objects.isNull(chunk)) {
            return null;
        }
        return parseChunk(chunk);
    }

    public Iterable<T> items() {
        return () -> new FlattenedIterator<>(this);
    }

    public Stream<T> itemStream() {
        return StreamSupport.stream(items().spliterator(), false);
    }

    public Stream<JsonBatch<T>> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public Offset currentOffset() {
        return chunkIterator.currentOffset();
    }

    public boolean upToDate() {
        return chunkIterator.upToDate();
    }

    public boolean streamClosed() {
        return chunkIterator.streamClosed();
    }

    @Override
    public void close() {
        chunkIterator.close();
    }

    private JsonBatch<T> parseChunk(Chunk chunk) {
        String json = chunk.dataAsString();
        List<T> items;
        if (Objects.isNull(json) || json.isEmpty()) {
            items = List.of();
        } else {
            try {
                items = parser.apply(json);
            } catch (Exception e) {
                throw new ParseErrorException("Failed to parse JSON: " + e.getMessage(), e);
            }
        }
        return new JsonBatch<>(items, chunk.nextOffset(), chunk.upToDate(), chunk.cursor().orElse(null));
    }

    private static final class FlattenedIterator<T> implements Iterator<T> {
        private final JsonIterator<T> jsonIterator;
        private Iterator<T> currentBatch;
        FlattenedIterator(JsonIterator<T> jsonIterator) {
            this.jsonIterator = jsonIterator;
        }
        @Override
        public boolean hasNext() {
            while (Objects.isNull(currentBatch) || !currentBatch.hasNext()) {
                if (!jsonIterator.hasNext()) {
                    return false;
                }
                currentBatch = jsonIterator.next().iterator();
            }
            return true;
        }
        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentBatch.next();
        }
    }
}
