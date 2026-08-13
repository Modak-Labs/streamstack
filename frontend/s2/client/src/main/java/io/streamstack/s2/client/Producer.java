package io.streamstack.s2.client;

import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.response.AppendResponse;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Not thread-safe: submit/flush/close must be called from a single producer thread.
 */
public final class Producer implements AutoCloseable {

    private final AppendSession session;

    private final List<AppendRecord> buffer = new ArrayList<>();
    private long bufferedBytes;
    private int maxRecords = Protocol.RECORD_BATCH_MAX_COUNT;
    private long maxBytes = Protocol.RECORD_BATCH_MAX_BYTES;

    private long maxInFlightBytes;
    private final Deque<InFlight> inFlight = new ArrayDeque<>();
    private long inFlightBytes;
    private volatile Throwable poison;

    Producer(AppendSession session) {
        this.session = session;
    }

    public Producer maxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public Producer maxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public Producer maxInFlightBytes(long bytes) {
        this.maxInFlightBytes = Math.max(bytes, 0);
        return this;
    }

    public Producer fencingToken(String fencingToken) {
        session.fencingToken(fencingToken);
        return this;
    }

    public Producer matchSeqNum(Long matchSeqNum) {
        session.matchSeqNum(matchSeqNum);
        return this;
    }

    public void submit(AppendRecord record) {
        checkPoison();
        long size = metered(record);

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
        checkPoison();

        if (maxInFlightBytes > 0) {
            drainInFlight();
            checkPoison();
        }

        if (buffer.isEmpty()) {
            return session.lastAck();
        }

        handOff();

        if (maxInFlightBytes == 0) {
            return session.flush();
        }

        joinFuture(session.flushAsync());
        checkPoison();

        return session.lastAck();
    }

    public AppendResponse lastAck() {
        return session.lastAck();
    }

    @Override
    public void close() {
        try {
            flush();
        } finally {
            session.close();
        }
    }

    private void dispatch() {
        if (maxInFlightBytes == 0) {
            handOff();
            session.flush();
            return;
        }

        long batchBytes = bufferedBytes;

        while (!inFlight.isEmpty() && inFlightBytes + batchBytes > maxInFlightBytes) {
            joinHead();
            checkPoison();
        }

        handOff();
        CompletableFuture<AppendResponse> future = session.flushAsync();

        future.whenComplete((ack, failure) -> {
            if (Objects.nonNull(failure)) {
                poison(failure);
            }
        });
        inFlight.addLast(new InFlight(future, batchBytes));
        inFlightBytes += batchBytes;
    }

    private void handOff() {
        session.submit(buffer);
        buffer.clear();
        bufferedBytes = 0;
    }

    private void drainInFlight() {
        while (!inFlight.isEmpty()) {
            joinHead();
        }
    }

    private void joinHead() {
        InFlight head = inFlight.removeFirst();

        inFlightBytes -= head.bytes();
        joinFuture(head.future());
    }

    private void joinFuture(CompletableFuture<AppendResponse> future) {
        try {
            future.join();
        } catch (CompletionException | CancellationException e) {
            poison(e);
        }
    }

    private synchronized void poison(Throwable failure) {
        if (Objects.isNull(poison)) {
            poison = unwrap(failure);
        }
    }

    private void checkPoison() {
        Throwable failure = poison;

        if (Objects.nonNull(failure)) {
            throw new IllegalStateException("producer failed", failure);
        }
    }

    private static Throwable unwrap(Throwable failure) {
        return failure instanceof CompletionException && Objects.nonNull(failure.getCause())
            ? failure.getCause()
            : failure;
    }

    private static long metered(AppendRecord record) {
        return Protocol.meteredBytes(record.headers(), record.body());
    }

    private record InFlight(CompletableFuture<AppendResponse> future, long bytes) {
    }
}
