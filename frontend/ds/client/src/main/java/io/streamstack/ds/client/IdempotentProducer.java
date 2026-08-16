package io.streamstack.ds.client;

import java.util.Objects;

import io.streamstack.ds.client.model.ProducerConfig;
import io.streamstack.ds.model.Protocol;
import io.streamstack.ds.model.exception.DurableStreamException;
import io.streamstack.ds.model.exception.SequenceConflictException;
import io.streamstack.ds.model.exception.StaleEpochException;
import io.streamstack.ds.model.exception.StreamClosedException;
import io.streamstack.ds.model.exception.StreamNotFoundException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class IdempotentProducer implements AutoCloseable {

    private final DurableStream client;
    private final String url;
    private final String producerId;
    private final ProducerConfig config;
    private final AtomicLong epoch;
    private final AtomicLong nextSeq;

    private final AtomicInteger inFlight = new AtomicInteger();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean streamClosed = new AtomicBoolean();

    private final Object batchLock = new Object();

    private final Object epochLock = new Object();

    private List<byte[]> pendingBatch = new ArrayList<>(1024);

    private int batchBytes;
    private ScheduledFuture<?> lingerTimer;
    private final ScheduledExecutorService scheduler;

    private final ConcurrentLinkedQueue<CompletableFuture<Void>> inFlightFutures = new ConcurrentLinkedQueue<>();

    private final LinkedBlockingQueue<DurableStreamException> errors = new LinkedBlockingQueue<>();

    private final ConcurrentHashMap<Long, EpochSeqState> seqState = new ConcurrentHashMap<>();

    public IdempotentProducer(DurableStream client, String url, String producerId, ProducerConfig config) {
        this.client = client;
        this.url = url;
        this.producerId = producerId;
        this.config = config;
        this.epoch = new AtomicLong(config.epoch());
        this.nextSeq = new AtomicLong(config.startingSeq());
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "durable-streams-producer-scheduler");

            t.setDaemon(true);

            return t;
        });
    }

    public void append(Object data) {
        if (closed.get()) {
            throw new DurableStreamException("Producer is closed");
        }

        byte[] bytes = serialize(data);

        synchronized (batchLock) {
            pendingBatch.add(bytes);
            batchBytes += bytes.length;

            if (batchBytes >= config.maxBatchBytes() || config.lingerMs() == 0) {
                sendBatch();
            } else if (Objects.isNull(lingerTimer)) {
                lingerTimer = scheduler.schedule(this::onLingerTimeout, config.lingerMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    public void flush() {
        while (true) {
            boolean hasPending;
            boolean hasInFlight;

            synchronized (batchLock) {
                if (Objects.nonNull(lingerTimer)) {
                    lingerTimer.cancel(false);
                    lingerTimer = null;
                }

                if (!pendingBatch.isEmpty()) {
                    dispatchBatch();
                }

                hasPending = !pendingBatch.isEmpty();
                hasInFlight = inFlight.get() > 0;
            }

            if (!hasPending && !hasInFlight) {
                break;
            }

            if (hasInFlight) {
                CompletableFuture<Void> any = inFlightFutures.peek();

                if (Objects.nonNull(any)) {
                    try {
                        any.get(100, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException | CancellationException ignored) {
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new DurableStreamException("Flush interrupted", e);
                    } catch (ExecutionException ignored) {
                    }
                }
            }
        }

        DurableStreamException first = null;
        DurableStreamException error;

        while (Objects.nonNull((error = errors.poll()))) {
            if (Objects.isNull(first)) {
                first = error;
            } else {
                first.addSuppressed(error);
            }
        }

        if (Objects.nonNull(first)) {
            throw first;
        }
    }

    @Override
    public void close() {
        if (closed.getAndSet(true)) {
            return;
        }

        try {
            flush();
        } finally {
            scheduler.shutdown();

            try {
                scheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void closeStream() {
        closeStream(null);
    }

    public void closeStream(byte[] data) {
        if (closed.get()) {
            throw new DurableStreamException("Producer is closed");
        }

        if (streamClosed.get()) {
            return;
        }

        flush();
        long seq = nextSeq.getAndIncrement();
        long epochVal = epoch.get();

        getSeqFuture(epochVal, seq);

        try {
            sendCloseWithRetry(data, epochVal, seq, false);
            signalSeqComplete(epochVal, seq, null);
            streamClosed.set(true);
        } catch (DurableStreamException err) {
            signalSeqComplete(epochVal, seq, err);
            errors.offer(err);

            if (Objects.nonNull(config.onError())) {
                config.onError().accept(err);
            }

            throw err;
        }
    }

    public void restart() {
        synchronized (epochLock) {
            epoch.incrementAndGet();
            nextSeq.set(0);
        }
    }

    public String producerId() {
        return producerId;
    }

    public long currentEpoch() {
        return epoch.get();
    }

    public long currentSeq() {
        return nextSeq.get();
    }

    private void onLingerTimeout() {
        synchronized (batchLock) {
            lingerTimer = null;
            sendBatch();
        }
    }

    private void sendBatch() {
        if (pendingBatch.isEmpty()) {
            return;
        }

        cancelLingerTimer();

        if (inFlight.get() >= config.maxInFlight()) {
            if (Objects.isNull(lingerTimer)) {
                lingerTimer = scheduler.schedule(this::onLingerTimeout, 1, TimeUnit.MILLISECONDS);
            }

            return;
        }

        dispatchBatch();
    }

    private void dispatchBatch() {
        List<byte[]> batch = pendingBatch;

        pendingBatch = new ArrayList<>(1024);
        batchBytes = 0;
        long seq = nextSeq.getAndIncrement();
        long currentEpoch = epoch.get();

        inFlight.incrementAndGet();
        CompletableFuture<Void> tracked = new CompletableFuture<>();

        inFlightFutures.add(tracked);
        sendBatchWithRetry(batch, currentEpoch, seq, false).whenComplete((v, ex) -> {
            inFlight.decrementAndGet();
            inFlightFutures.remove(tracked);

            if (Objects.nonNull(ex)) {
                tracked.completeExceptionally(ex);
            } else {
                tracked.complete(null);
            }
        });
    }

    private void cancelLingerTimer() {
        if (Objects.nonNull(lingerTimer)) {
            lingerTimer.cancel(false);
            lingerTimer = null;
        }
    }

    private EpochSeqState epochState(long epochVal) {
        return seqState.computeIfAbsent(epochVal, k -> new EpochSeqState());
    }

    private CompletableFuture<Void> getSeqFuture(long epochVal, long seq) {
        return epochState(epochVal).futures.computeIfAbsent(seq, k -> new CompletableFuture<>());
    }

    private void signalSeqComplete(long epochVal, long seq, Throwable error) {
        EpochSeqState state = epochState(epochVal);
        CompletableFuture<Void> future = state.futures.get(seq);

        if (Objects.nonNull(future)) {
            if (Objects.nonNull(error)) {
                future.completeExceptionally(error);
            } else {
                future.complete(null);
            }
        }

        state.highestCompleted.accumulateAndGet(seq, Math::max);
        long cleanupThreshold = seq - config.maxInFlight() * 3L;

        if (cleanupThreshold > 0) {
            state.futures.keySet().removeIf(oldSeq -> oldSeq < cleanupThreshold);
        }
    }

    private CompletableFuture<Void> waitForSeq(long epochVal, long seq) {
        EpochSeqState state = epochState(epochVal);
        CompletableFuture<Void> existing = state.futures.get(seq);

        if (Objects.nonNull(existing)) {
            return existing;
        }

        if (seq <= state.highestCompleted.get()) {
            return CompletableFuture.completedFuture(null);
        }

        return state.futures.computeIfAbsent(seq, k -> new CompletableFuture<>());
    }

    private Claim claimEpoch(long serverEpoch) {
        synchronized (epochLock) {
            if (epoch.get() <= serverEpoch) {
                epoch.set(serverEpoch + 1);
                nextSeq.set(0);
            }

            return new Claim(epoch.get(), nextSeq.getAndIncrement());
        }
    }

    private CompletableFuture<Void> awaitPredecessors(long epochVal, long fromSeq, long toSeq) {
        List<CompletableFuture<Void>> wait = new ArrayList<>();

        for (long s = fromSeq; s < toSeq; s++) {
            wait.add(waitForSeq(epochVal, s));
        }

        return CompletableFuture.allOf(wait.toArray(CompletableFuture[]::new));
    }

    private void reportFailure(long epochVal, long seq, DurableStreamException err) {
        signalSeqComplete(epochVal, seq, err);
        errors.offer(err);

        if (Objects.nonNull(config.onError())) {
            config.onError().accept(err);
        }
    }

    private CompletableFuture<Void> failBatch(long epochVal, long seq, DurableStreamException err) {
        reportFailure(epochVal, seq, err);
        return CompletableFuture.failedFuture(err);
    }

    private CompletableFuture<Void> sendBatchWithRetry(List<byte[]> batch, long batchEpoch, long seq, boolean isRetry) {
        getSeqFuture(batchEpoch, seq);
        String contentType = Objects.nonNull(config.contentType()) ? config.contentType() : client.cachedContentType(url);
        byte[] data = serializeBatch(batch, contentType);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofByteArray(data))
            .timeout(Duration.ofSeconds(30))
            .header(Protocol.H_PRODUCER_ID, producerId)
            .header(Protocol.H_PRODUCER_EPOCH, Long.toString(batchEpoch))
            .header(Protocol.H_PRODUCER_SEQ, Long.toString(seq));
        if (Objects.nonNull(contentType)) {
            builder.header(Protocol.H_CONTENT_TYPE, contentType);
        }

        client.resolveHeaders().forEach(builder::header);

        return client.sendAsync(builder.build()).thenCompose(response -> {
            int status = response.statusCode();

            if (status == 200 || status == 201 || status == 204) {
                signalSeqComplete(batchEpoch, seq, null);
                return CompletableFuture.completedFuture(null);
            }

            if (status == 403) {
                long serverEpoch = parseEpoch(response);

                if (config.autoClaim() && !isRetry) {
                    Claim claim = claimEpoch(serverEpoch);
                    return sendBatchWithRetry(batch, claim.epoch(), claim.seq(), true);
                }

                return failBatch(batchEpoch, seq, new StaleEpochException(serverEpoch));
            }

            if (status == 409) {
                long expectedSeq = expectedSeq(response);

                if (expectedSeq >= 0 && expectedSeq < seq) {
                    return awaitPredecessors(batchEpoch, expectedSeq, seq)
                        .thenCompose(v -> sendBatchWithRetry(batch, batchEpoch, seq, false));
                }

                return failBatch(batchEpoch, seq,
                    new SequenceConflictException(expectedSeq >= 0 ? expectedSeq : null, seq));
            }

            return failBatch(batchEpoch, seq,
                new DurableStreamException("Batch failed with status: " + status, status));
        }).exceptionally(ex -> {
            Throwable cause = Objects.nonNull(ex.getCause()) ? ex.getCause() : ex;

            if (!(cause instanceof DurableStreamException)) {
                reportFailure(batchEpoch, seq,
                    new DurableStreamException("Batch send failed: " + cause.getMessage(), cause));
            }

            return null;
        });
    }

    private void sendCloseWithRetry(byte[] data, long batchEpoch, long seq, boolean isRetry) {
        byte[] body = null;
        String contentType = Objects.nonNull(config.contentType()) ? config.contentType() : client.cachedContentType(url);

        if (Objects.nonNull(data) && data.length > 0) {
            boolean isJson = Objects.nonNull(contentType) && contentType.contains("json");

            body = isJson ? DurableStream.wrapInJsonArray(data) : data;
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .header(Protocol.H_PRODUCER_ID, producerId)
            .header(Protocol.H_PRODUCER_EPOCH, Long.toString(batchEpoch))
            .header(Protocol.H_PRODUCER_SEQ, Long.toString(seq))
            .header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        if (Objects.nonNull(contentType) && Objects.nonNull(body)) {
            builder.header(Protocol.H_CONTENT_TYPE, contentType);
        }

        client.resolveHeaders().forEach(builder::header);

        if (Objects.nonNull(body)) {
            builder.POST(HttpRequest.BodyPublishers.ofByteArray(body));
        } else {
            builder.POST(HttpRequest.BodyPublishers.noBody());
        }

        HttpResponse<byte[]> response;

        try {
            response = client.httpClient().send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new DurableStreamException("Close failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DurableStreamException("Close failed: " + e.getMessage(), e);
        }

        int status = response.statusCode();

        if (status == 200 || status == 201 || status == 204) {
            return;
        }

        if (status == 403) {
            long serverEpoch = parseEpoch(response);

            if (config.autoClaim() && !isRetry) {
                Claim claim = claimEpoch(serverEpoch);

                sendCloseWithRetry(data, claim.epoch(), claim.seq(), true);

                return;
            }

            throw new StaleEpochException(serverEpoch);
        }

        if (status == 409) {
            if (Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null))) {
                throw new StreamClosedException(url);
            }

            long expectedSeq = expectedSeq(response);

            if (expectedSeq >= 0 && expectedSeq < seq) {
                awaitPredecessors(batchEpoch, expectedSeq, seq).join();
                sendCloseWithRetry(data, batchEpoch, seq, false);

                return;
            }

            throw new SequenceConflictException(expectedSeq >= 0 ? expectedSeq : null, seq);
        }

        if (status == 404) {
            throw new StreamNotFoundException(url);
        }

        throw new DurableStreamException("Close failed with status: " + status, status);
    }

    private static long parseEpoch(HttpResponse<byte[]> response) {
        return response.headers().firstValue(Protocol.H_PRODUCER_EPOCH).map(Long::parseLong).orElse(0L);
    }

    private static long expectedSeq(HttpResponse<byte[]> response) {
        return response.headers()
            .firstValue(Protocol.H_PRODUCER_EXPECTED_SEQ)
            .map(Long::parseLong)
            .orElse(-1L);
    }

    private static byte[] serialize(Object data) {
        if (data instanceof byte[] bytes) {
            return bytes;
        }

        if (data instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }

        throw new IllegalArgumentException("Unsupported data type: " + data.getClass().getName());
    }

    private static byte[] serializeBatch(List<byte[]> batch, String contentType) {
        boolean json = Objects.nonNull(contentType) && contentType.toLowerCase().contains("json");

        if (json) {
            if (batch.size() == 1 && DurableStream.looksLikeJsonArray(batch.get(0))) {
                return batch.get(0);
            }

            return joinAsJsonArray(batch);
        }

        if (batch.size() == 1) {
            return batch.get(0);
        }

        int total = 0;

        for (byte[] bytes : batch) {
            total += bytes.length;
        }

        byte[] result = new byte[total];
        int pos = 0;

        for (byte[] bytes : batch) {
            System.arraycopy(bytes, 0, result, pos, bytes.length);
            pos += bytes.length;
        }

        return result;
    }

    private static byte[] joinAsJsonArray(List<byte[]> batch) {
        int total = batch.size() + 1;

        for (byte[] bytes : batch) {
            total += bytes.length;
        }

        byte[] result = new byte[total];
        int pos = 0;

        result[pos++] = '[';

        for (int i = 0; i < batch.size(); i++) {
            if (i > 0) {
                result[pos++] = ',';
            }

            byte[] bytes = batch.get(i);

            System.arraycopy(bytes, 0, result, pos, bytes.length);
            pos += bytes.length;
        }

        result[pos] = ']';

        return result;
    }

    private record Claim(long epoch, long seq) {
    }

    private static final class EpochSeqState {
        final ConcurrentHashMap<Long, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();
        final AtomicLong highestCompleted = new AtomicLong(-1);
    }
}
