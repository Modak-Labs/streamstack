package io.streamstack.ss.client;

import io.streamstack.ss.model.RecordEnvelope;
import io.streamstack.ss.model.request.AppendRequest;
import io.streamstack.ss.model.response.AppendResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Not thread-safe: submit/flush/close must be called from a single producer thread.
 * Producer sequence numbers require ordered delivery, so at most one batch is in
 * flight; the next batch fills while the previous one awaits its ack.
 */
public final class Producer implements AutoCloseable {

    private final StreamStack client;
    private final String stream;
    private final String producerId;
    private final long epoch;

    private final List<RecordEnvelope> buffer = new ArrayList<>();
    private long bufferedBytes;
    private int maxRecords = 1000;
    private long maxBytes = 1024 * 1024;
    private long nextSeq;

    private CompletableFuture<AppendResponse> inFlight;
    private AppendResponse lastAck;

    Producer(StreamStack client, String stream, String producerId) {
        this.client = Objects.requireNonNull(client, "client");
        this.stream = Objects.requireNonNull(stream, "stream");
        this.producerId = Objects.requireNonNull(producerId, "producerId");
        this.epoch = System.currentTimeMillis();
    }

    public Producer maxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public Producer maxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public void submit(RecordEnvelope record) {
        long size = 16 + record.body().length;

        if (!buffer.isEmpty() && (buffer.size() >= maxRecords || bufferedBytes + size > maxBytes)) {
            dispatch();
        }

        buffer.add(record);
        bufferedBytes += size;

        if (buffer.size() >= maxRecords || bufferedBytes >= maxBytes) {
            dispatch();
        }
    }

    public AppendResponse flush() {
        if (!buffer.isEmpty()) {
            dispatch();
        }

        awaitInFlight();

        return lastAck;
    }

    public AppendResponse lastAck() {
        return lastAck;
    }

    @Override
    public void close() {
        flush();
    }

    private void dispatch() {
        awaitInFlight();

        AppendRequest request = new AppendRequest(List.copyOf(buffer), null, producerId, epoch, nextSeq++);

        buffer.clear();
        bufferedBytes = 0;
        inFlight = client.appendAsync(stream, request);
    }

    private void awaitInFlight() {
        if (Objects.isNull(inFlight)) {
            return;
        }

        CompletableFuture<AppendResponse> pending = inFlight;

        inFlight = null;

        try {
            lastAck = pending.join();
        } catch (Exception e) {
            Throwable cause = Objects.isNull(e.getCause()) ? e : e.getCause();

            if (cause instanceof StreamStackException se) {
                throw se;
            }

            throw new IllegalStateException("producer failed", cause);
        }
    }
}
