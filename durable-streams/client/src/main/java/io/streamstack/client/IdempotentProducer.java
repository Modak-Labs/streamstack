package io.streamstack.client;

import io.streamstack.client.model.ProducerConfig;
import io.streamstack.model.Protocol;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.exception.SequenceConflictException;
import io.streamstack.model.exception.StaleEpochException;
import io.streamstack.model.exception.StreamClosedException;
import io.streamstack.model.exception.StreamNotFoundException;

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
    private final ConcurrentHashMap<Long, ConcurrentHashMap<Long, CompletableFuture<Void>>> seqState =
        new ConcurrentHashMap<>();

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
            if (batchBytes >= config.maxBatchBytes()) {
                sendBatch();
            } else if (lingerTimer == null && config.lingerMs() > 0) {
                lingerTimer = scheduler.schedule(this::onLingerTimeout, config.lingerMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    public void flush() {
        while (true) {
            boolean hasPending;
            boolean hasInFlight;
            synchronized (batchLock) {
                if (lingerTimer != null) {
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
                if (any != null) {
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
        while ((error = errors.poll()) != null) {
            if (first == null) {
                first = error;
            } else {
                first.addSuppressed(error);
            }
        }
        if (first != null) {
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
            if (config.onError() != null) {
                config.onError().accept(err);
            }
            throw err;
        }
    }

    public void restart() {
        epoch.incrementAndGet();
        nextSeq.set(0);
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
            if (lingerTimer == null) {
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
            if (ex != null) {
                tracked.completeExceptionally(ex);
            } else {
                tracked.complete(null);
            }
        });
    }

    private void cancelLingerTimer() {
        if (lingerTimer != null) {
            lingerTimer.cancel(false);
            lingerTimer = null;
        }
    }

    private CompletableFuture<Void> getSeqFuture(long epochVal, long seq) {
        return seqState.computeIfAbsent(epochVal, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(seq, k -> new CompletableFuture<>());
    }

    private void signalSeqComplete(long epochVal, long seq, Throwable error) {
        ConcurrentHashMap<Long, CompletableFuture<Void>> epochMap = seqState.get(epochVal);
        if (epochMap == null) {
            return;
        }
        CompletableFuture<Void> future = epochMap.get(seq);
        if (future != null) {
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                future.complete(null);
            }
        }
        long cleanupThreshold = seq - config.maxInFlight() * 3L;
        if (cleanupThreshold > 0) {
            epochMap.keySet().removeIf(oldSeq -> oldSeq < cleanupThreshold);
        }
    }

    private CompletableFuture<Void> waitForSeq(long epochVal, long seq) {
        return getSeqFuture(epochVal, seq);
    }

    private CompletableFuture<Void> sendBatchWithRetry(List<byte[]> batch, long batchEpoch, long seq, boolean isRetry) {
        getSeqFuture(batchEpoch, seq);
        byte[] data = serializeBatch(batch);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .POST(HttpRequest.BodyPublishers.ofByteArray(data))
            .timeout(Duration.ofSeconds(30))
            .header(Protocol.H_PRODUCER_ID, producerId)
            .header(Protocol.H_PRODUCER_EPOCH, Long.toString(batchEpoch))
            .header(Protocol.H_PRODUCER_SEQ, Long.toString(seq));
        String contentType = config.contentType() != null ? config.contentType() : client.cachedContentType(url);
        if (contentType != null) {
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
                    long retrySeq;
                    synchronized (epochLock) {
                        if (epoch.get() <= serverEpoch) {
                            epoch.set(serverEpoch + 1);
                            nextSeq.set(0);
                        }
                        retrySeq = nextSeq.getAndIncrement();
                    }
                    return sendBatchWithRetry(batch, epoch.get(), retrySeq, true);
                }
                StaleEpochException err = new StaleEpochException(serverEpoch);
                signalSeqComplete(batchEpoch, seq, err);
                errors.offer(err);
                if (config.onError() != null) {
                    config.onError().accept(err);
                }
                return CompletableFuture.failedFuture(err);
            }
            if (status == 409) {
                long expectedSeq = response.headers()
                    .firstValue(Protocol.H_PRODUCER_EXPECTED_SEQ)
                    .map(Long::parseLong)
                    .orElse(-1L);
                if (expectedSeq >= 0 && expectedSeq < seq) {
                    List<CompletableFuture<Void>> wait = new ArrayList<>();
                    for (long s = expectedSeq; s < seq; s++) {
                        wait.add(waitForSeq(batchEpoch, s));
                    }
                    return CompletableFuture.allOf(wait.toArray(CompletableFuture[]::new))
                        .thenCompose(v -> sendBatchWithRetry(batch, batchEpoch, seq, false));
                }
                SequenceConflictException err = new SequenceConflictException(
                    expectedSeq >= 0 ? expectedSeq : null,
                    seq);
                signalSeqComplete(batchEpoch, seq, err);
                errors.offer(err);
                if (config.onError() != null) {
                    config.onError().accept(err);
                }
                return CompletableFuture.failedFuture(err);
            }
            DurableStreamException err = new DurableStreamException("Batch failed with status: " + status, status);
            signalSeqComplete(batchEpoch, seq, err);
            errors.offer(err);
            if (config.onError() != null) {
                config.onError().accept(err);
            }
            return CompletableFuture.failedFuture(err);
        }).exceptionally(ex -> {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            DurableStreamException err = cause instanceof DurableStreamException dse
                ? dse
                : new DurableStreamException("Batch send failed: " + cause.getMessage(), cause);
            signalSeqComplete(batchEpoch, seq, err);
            errors.offer(err);
            if (config.onError() != null) {
                config.onError().accept(err);
            }
            return null;
        });
    }

    private void sendCloseWithRetry(byte[] data, long batchEpoch, long seq, boolean isRetry) {
        byte[] body = null;
        String contentType = config.contentType() != null ? config.contentType() : client.cachedContentType(url);
        if (data != null && data.length > 0) {
            boolean isJson = contentType != null && contentType.contains("json");
            body = isJson ? DurableStream.wrapInJsonArray(data) : data;
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(30))
            .header(Protocol.H_PRODUCER_ID, producerId)
            .header(Protocol.H_PRODUCER_EPOCH, Long.toString(batchEpoch))
            .header(Protocol.H_PRODUCER_SEQ, Long.toString(seq))
            .header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        if (contentType != null && body != null) {
            builder.header(Protocol.H_CONTENT_TYPE, contentType);
        }
        client.resolveHeaders().forEach(builder::header);
        if (body != null) {
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
                long retrySeq;
                synchronized (epochLock) {
                    if (epoch.get() <= serverEpoch) {
                        epoch.set(serverEpoch + 1);
                        nextSeq.set(0);
                    }
                    retrySeq = nextSeq.getAndIncrement();
                }
                sendCloseWithRetry(data, epoch.get(), retrySeq, true);
                return;
            }
            throw new StaleEpochException(serverEpoch);
        }
        if (status == 409) {
            if (Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null))) {
                throw new StreamClosedException(url);
            }
            long expectedSeq = response.headers()
                .firstValue(Protocol.H_PRODUCER_EXPECTED_SEQ)
                .map(Long::parseLong)
                .orElse(-1L);
            if (expectedSeq >= 0 && expectedSeq < seq) {
                List<CompletableFuture<Void>> wait = new ArrayList<>();
                for (long s = expectedSeq; s < seq; s++) {
                    wait.add(waitForSeq(batchEpoch, s));
                }
                CompletableFuture.allOf(wait.toArray(CompletableFuture[]::new)).join();
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

    private static byte[] serialize(Object data) {
        if (data instanceof byte[] bytes) {
            return bytes;
        }
        if (data instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalArgumentException("Unsupported data type: " + data.getClass().getName());
    }

    private static byte[] serializeBatch(List<byte[]> batch) {
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
}
