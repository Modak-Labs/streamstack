package io.streamstack.client;

import java.util.Objects;

import io.streamstack.client.helper.SseStreamingReader;
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
    private boolean streamClosed;
    private boolean closed;
    private Chunk nextChunk;
    private boolean hasNextComputed;
    private SseStreamingReader sseReader;
    private boolean sseStarted;

    ChunkIterator(DurableStream client, String url, ReadRequest request, Duration timeout) {
        this.client = client;
        this.url = url;
        this.currentOffset = Objects.nonNull(request.offset()) ? request.offset() : Offset.beginning();
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
            return Objects.nonNull(nextChunk);
        }

        if (Objects.isNull(liveMode) && upToDate) {
            return false;
        }

        if (streamClosed) {
            return false;
        }

        nextChunk = fetchNext();
        hasNextComputed = true;

        return Objects.nonNull(nextChunk);
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
        if (closed || streamClosed) {
            return null;
        }

        if (Objects.isNull(liveMode) && upToDate) {
            return null;
        }

        if (liveMode == LiveMode.SSE) {
            return pollSse(pollTimeout);
        }

        Chunk chunk;

        try {
            chunk = client.readOnce(url, new ReadRequest(currentOffset, liveMode, cursor), pollTimeout);
        } catch (DurableStreamException e) {
            Throwable cause = e.getCause();

            if (cause instanceof HttpTimeoutException || cause instanceof TimeoutException) {
                upToDate = true;
                return null;
            }

            throw e;
        }

        if (chunk.statusCode() == 204) {
            if (Objects.nonNull(chunk.nextOffset())) {
                currentOffset = chunk.nextOffset();
            }

            upToDate = true;
            streamClosed = chunk.closed() && chunk.upToDate();

            return null;
        }

        updateState(chunk);

        return chunk;
    }

    public Offset currentOffset() {
        if (Objects.nonNull(sseReader) && Objects.nonNull(sseReader.currentOffset())) {
            return sseReader.currentOffset();
        }

        return currentOffset;
    }

    public boolean upToDate() {
        if (Objects.nonNull(sseReader)) {
            return sseReader.upToDate();
        }

        return upToDate;
    }

    public boolean streamClosed() {
        if (Objects.nonNull(sseReader)) {
            return sseReader.streamClosed() || streamClosed;
        }

        return streamClosed;
    }

    @Override
    public void close() {
        closed = true;

        if (Objects.nonNull(sseReader)) {
            sseReader.close();
        }
    }

    private void updateState(Chunk chunk) {
        if (Objects.nonNull(chunk.nextOffset())) {
            currentOffset = chunk.nextOffset();
        }

        cursor = chunk.cursor().orElse(null);
        upToDate = chunk.upToDate();

        if (chunk.closed() && chunk.upToDate()) {
            streamClosed = true;
        }
    }

    private Chunk pollSse(Duration pollTimeout) {
        ensureSse();

        if (sseReader.closed()) {
            return null;
        }

        long timeoutMs = Objects.nonNull(pollTimeout) ? pollTimeout.toMillis() : 30_000L;
        Chunk chunk = sseReader.poll(timeoutMs);

        if (Objects.nonNull(chunk)) {
            updateState(chunk);
        }

        return chunk;
    }

    private Chunk fetchNext() {
        if (liveMode == LiveMode.SSE) {
            return fetchNextSse();
        }

        Chunk chunk = client.readOnce(url, new ReadRequest(currentOffset, liveMode, cursor), timeout);

        if (chunk.statusCode() == 204) {
            if (Objects.isNull(liveMode)) {
                upToDate = true;
                return null;
            }

            return chunk;
        }

        if (Objects.isNull(liveMode) && chunk.data().length == 0 && chunk.upToDate()) {
            if (Objects.nonNull(chunk.nextOffset())) {
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

        long timeoutMs = Objects.nonNull(timeout) ? timeout.toMillis() : 60_000L;

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
