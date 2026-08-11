package io.streamstack.client;

import io.streamstack.client.internal.SseStreamingReader;
import io.streamstack.client.model.Chunk;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.request.ReadRequest;

import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

public final class ChunkIterator implements Iterator<Chunk>, Iterable<Chunk>, AutoCloseable {
    private final DurableStream client;
    private final String url;
    private final LiveMode liveMode;
    private final Duration timeout;

    private Offset currentOffset;
    private String cursor;
    private boolean upToDate;
    private boolean closed;
    private Chunk nextChunk;
    private boolean hasNextComputed;

    private SseStreamingReader sseReader;
    private boolean sseStarted;

    ChunkIterator(DurableStream client, String url, ReadRequest request, Duration timeout) {
        this.client = client;
        this.url = url;
        this.currentOffset = request.offset() != null ? request.offset() : Offset.beginning();
        this.liveMode = request.live();
        this.timeout = timeout;
        this.cursor = request.cursor();
    }

    @Override
    public Iterator<Chunk> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        if (hasNextComputed) {
            return nextChunk != null;
        }
        if (liveMode == null && upToDate) {
            return false;
        }
        nextChunk = fetchNext();
        hasNextComputed = true;
        return nextChunk != null;
    }

    @Override
    public Chunk next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextComputed = false;
        Chunk chunk = nextChunk;
        nextChunk = null;
        updateState(chunk);
        return chunk;
    }

    public Chunk poll(Duration pollTimeout) {
        if (closed) {
            return null;
        }
        if (liveMode == null && upToDate) {
            return null;
        }
        if (liveMode == LiveMode.SSE) {
            return pollSse(pollTimeout);
        }
        Chunk chunk;
        try {
            chunk = client.readOnce(url, new ReadRequest(currentOffset, liveMode, cursor, null), pollTimeout);
        } catch (DurableStreamException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpTimeoutException || cause instanceof TimeoutException) {
                upToDate = true;
                return null;
            }
            throw e;
        }
        if (chunk.statusCode() == 204) {
            if (chunk.nextOffset() != null) {
                currentOffset = chunk.nextOffset();
            }
            upToDate = true;
            return null;
        }
        updateState(chunk);
        return chunk;
    }

    public Offset currentOffset() {
        if (sseReader != null && sseReader.currentOffset() != null) {
            return sseReader.currentOffset();
        }
        return currentOffset;
    }

    public boolean upToDate() {
        if (sseReader != null) {
            return sseReader.upToDate();
        }
        return upToDate;
    }

    @Override
    public void close() {
        closed = true;
        if (sseReader != null) {
            sseReader.close();
        }
    }

    private void updateState(Chunk chunk) {
        if (chunk.nextOffset() != null) {
            currentOffset = chunk.nextOffset();
        }
        cursor = chunk.cursor().orElse(null);
        upToDate = chunk.upToDate();
    }

    private Chunk pollSse(Duration pollTimeout) {
        ensureSse();
        if (sseReader.closed()) {
            return null;
        }
        long timeoutMs = pollTimeout != null ? pollTimeout.toMillis() : 30_000L;
        Chunk chunk = sseReader.poll(timeoutMs);
        if (chunk != null) {
            updateState(chunk);
        }
        return chunk;
    }

    private Chunk fetchNext() {
        if (liveMode == LiveMode.SSE) {
            return fetchNextSse();
        }
        Chunk chunk = client.readOnce(url, new ReadRequest(currentOffset, liveMode, cursor, null), timeout);
        if (chunk.statusCode() == 204) {
            if (liveMode == null) {
                upToDate = true;
                return null;
            }
            return chunk;
        }
        if (liveMode == null && chunk.data().length == 0 && chunk.upToDate()) {
            if (chunk.nextOffset() != null) {
                currentOffset = chunk.nextOffset();
            }
            upToDate = true;
            return null;
        }
        return chunk;
    }

    private Chunk fetchNextSse() {
        ensureSse();
        if (sseReader.closed()) {
            return null;
        }
        long timeoutMs = timeout != null ? timeout.toMillis() : 60_000L;
        return sseReader.poll(timeoutMs);
    }

    private void ensureSse() {
        if (sseStarted) {
            return;
        }
        sseReader = client.openSseStream(url, currentOffset, cursor);
        sseReader.start();
        sseStarted = true;
    }
}
