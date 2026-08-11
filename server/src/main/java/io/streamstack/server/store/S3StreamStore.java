package io.streamstack.server.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamstack.DefaultRecordBatch;
import io.streamstack.api.CreateStreamOptions;
import io.streamstack.api.FetchResult;
import io.streamstack.api.KVClient;
import io.streamstack.api.KeyValue;
import io.streamstack.api.KeyValue.Key;
import io.streamstack.api.KeyValue.Value;
import io.streamstack.api.OpenStreamOptions;
import io.streamstack.api.RecordBatchWithContext;
import io.streamstack.api.Stream;
import io.streamstack.api.StreamClient;
import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.server.http.SseEncoder;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamInfo;

import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class S3StreamStore implements StreamStore {
    private static final long OP_TIMEOUT_SEC = 30;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final StreamClient streamClient;
    private final KVClient kvClient;
    private final MetadataNode metadataNode;
    private final StreamWaiterRegistry waiters;
    private final ConcurrentHashMap<Long, Stream> openStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicLong> localEpochs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> pathLocks = new ConcurrentHashMap<>();

    public S3StreamStore(
        StreamClient streamClient,
        KVClient kvClient,
        MetadataNode metadataNode,
        StreamWaiterRegistry waiters) {
        this.streamClient = Objects.requireNonNull(streamClient, "streamClient");
        this.kvClient = Objects.requireNonNull(kvClient, "kvClient");
        this.metadataNode = Objects.requireNonNull(metadataNode, "metadataNode");
        this.waiters = Objects.requireNonNull(waiters, "waiters");
    }

    @Override
    public CreateResult create(
        URI url,
        String contentType,
        Long ttlSeconds,
        Instant expiresAt,
        boolean closed,
        InputStream initialBody) throws StoreException {
        String path = pathOf(url);
        Object lock = pathLocks.computeIfAbsent(path, p -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry existing = getEntry(path, false);
                if (existing != null) {
                    if (!configMatches(existing, contentType, ttlSeconds, expiresAt, closed)) {
                        throw new StoreException(StoreException.Kind.CONFLICT);
                    }
                    return new CreateResult(false, toInfo(path, existing));
                }

                Stream stream = streamClient.createAndOpenStream(
                    CreateStreamOptions.builder().epoch(1).replicaCount(1).tag("path", path).build())
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                openStreams.put(stream.streamId(), stream);
                localEpochs.put(stream.streamId(), new AtomicLong(stream.streamEpoch()));

                long deadline = deadlineOf(ttlSeconds, expiresAt);
                RegistryEntry candidate = new RegistryEntry(
                    stream.streamId(), contentType, ttlSeconds, expiresAt, closed, deadline, null,
                    Map.of(), null);
                Value stored = kvClient.putKVIfAbsent(KeyValue.of(path, ByteBuffer.wrap(candidate.encode())))
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                RegistryEntry current = RegistryEntry.decode(toBytes(stored));

                if (current.streamId() != candidate.streamId()) {
                    try {
                        stream.destroy().get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                    }
                    openStreams.remove(stream.streamId());
                    localEpochs.remove(stream.streamId());
                    if (!configMatches(current, contentType, ttlSeconds, expiresAt, closed)) {
                        throw new StoreException(StoreException.Kind.CONFLICT);
                    }
                    return new CreateResult(false, toInfo(path, current));
                }

                if (initialBody != null) {
                    byte[] body = initialBody.readAllBytes();
                    if (body.length > 0) {
                        List<byte[]> messages = splitMessages(contentType, body, true);
                        for (byte[] message : messages) {
                            appendBytes(path, stream, message);
                        }
                        if (!messages.isEmpty()) {
                            current = requireEntry(path, false);
                        }
                    }
                }
                if (closed && !current.closed()) {
                    putEntry(path, current.close(null));
                    current = requireEntry(path, false);
                }
                return new CreateResult(true, toInfo(path, current, stream));
            } catch (StoreException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public AppendResult append(URI url, AppendCommand request) throws StoreException {
        String path = pathOf(url);
        Object lock = pathLocks.computeIfAbsent(path, p -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(path, false);
                if (entry == null) {
                    throw new StoreException(StoreException.Kind.NOT_FOUND);
                }
                Stream stream = ensureOpen(entry.streamId());
                OffsetToken next = OffsetToken.ofRecordOffset(stream.nextOffset());

                ProducerDecision producer = validateProducer(entry, request);
                if (producer != null && producer.status() != ProducerDecision.Status.ACCEPTED) {
                    return handleProducerReject(entry, next, producer, request);
                }

                if (entry.closed()) {
                    // Close-only without body is idempotent (protocol close-idempotent).
                    if (request.close() && request.body().length == 0) {
                        if (producer != null && !matchesClosedBy(entry, request) && entry.closedBy() != null) {
                            throw new StoreException(StoreException.Kind.CLOSED, next, true);
                        }
                        Long echoedSeq = request.hasProducer()
                            ? (entry.closedBy() != null ? entry.closedBy().seq() : request.producerSeq())
                            : null;
                        Long echoedEpoch = request.hasProducer() ? request.producerEpoch() : null;
                        return new AppendResult(next, false, true, echoedEpoch, echoedSeq);
                    }
                    if (producer != null && matchesClosedBy(entry, request)) {
                        return new AppendResult(next, false, true, request.producerEpoch(), entry.closedBy().seq());
                    }
                    throw new StoreException(StoreException.Kind.CLOSED, next, true);
                }

                boolean closeOnly = request.body().length == 0 && request.close();
                if (!closeOnly) {
                    if (request.contentType() == null || request.contentType().isEmpty()) {
                        throw new StoreException(StoreException.Kind.BAD_REQUEST, next, false, "missing Content-Type");
                    }
                    if (!mimeEquals(entry.contentType(), request.contentType())) {
                        throw new StoreException(StoreException.Kind.CONFLICT, next, false);
                    }
                    if (request.body().length == 0) {
                        throw new StoreException(StoreException.Kind.BAD_REQUEST, next, false, "Empty body");
                    }
                }

                if (request.streamSeq() != null
                    && (producer == null || producer.status() == ProducerDecision.Status.ACCEPTED)) {
                    if (entry.lastSeq() != null && request.streamSeq().compareTo(entry.lastSeq()) <= 0) {
                        throw new StoreException(StoreException.Kind.CONFLICT, next, false, "Sequence conflict");
                    }
                }

                if (!closeOnly) {
                    List<byte[]> messages = splitMessages(entry.contentType(), request.body(), false);
                    for (byte[] message : messages) {
                        next = appendBytes(path, stream, message);
                    }
                }

                RegistryEntry updated = entry;
                if (request.streamSeq() != null) {
                    updated = updated.withLastSeq(request.streamSeq());
                }
                if (producer != null && producer.status() == ProducerDecision.Status.ACCEPTED) {
                    updated = updated.withProducer(request.producerId(), request.producerEpoch(), request.producerSeq());
                }
                ClosedBy closedBy = null;
                if (request.close()) {
                    if (request.hasProducer()) {
                        closedBy = new ClosedBy(request.producerId(), request.producerEpoch(), request.producerSeq());
                    }
                    updated = updated.close(closedBy);
                }
                updated = touchDeadline(updated);
                putEntry(path, updated);
                if (request.close()) {
                    waiters.notifyClosed(path);
                }
                Long echoedSeq = request.hasProducer() ? request.producerSeq() : null;
                Long echoedEpoch = request.hasProducer() ? request.producerEpoch() : null;
                return new AppendResult(next, !closeOnly, request.close(), echoedEpoch, echoedSeq);
            } catch (StoreException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public OffsetToken close(URI url) throws StoreException {
        return append(url, new AppendCommand(null, new byte[0], null, null, null, null, true)).nextOffset();
    }

    @Override
    public boolean delete(URI url) throws StoreException {
        String path = pathOf(url);
        Object lock = pathLocks.computeIfAbsent(path, p -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(path, false);
                if (entry == null) {
                    return false;
                }
                Value deleted = kvClient.delKV(Key.of(path)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (deleted == null) {
                    return false;
                }
                destroyStream(entry.streamId());
                waiters.notifyClosed(path);
                return true;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public Optional<StreamInfo> head(URI url) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path, false);
            return entry == null ? Optional.empty() : Optional.of(toInfo(path, entry));
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public ReadResult read(URI url, OffsetToken startOffset, int maxBytes) throws StoreException {
        String path = pathOf(url);
        Object lock = pathLocks.computeIfAbsent(path, p -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(path, true);
                if (entry == null) {
                    throw new StoreException(StoreException.Kind.NOT_FOUND);
                }
                Stream stream = ensureOpen(entry.streamId());
                long start = startOffset.recordOffset();
                long end = stream.confirmOffset();
                if (start > end) {
                    throw new StoreException(StoreException.Kind.BAD_REQUEST);
                }
                if (start == end) {
                    return new ReadResult(List.of(), entry.contentType(), OffsetToken.ofRecordOffset(end), true,
                        entry.closed());
                }
                FetchResult fetch = stream.fetch(start, end, maxBytes).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                try {
                    List<byte[]> messages = new ArrayList<>();
                    int total = 0;
                    long next = start;
                    for (RecordBatchWithContext batch : fetch.recordBatchList()) {
                        ByteBuffer payload = batch.rawPayload();
                        byte[] bytes = new byte[payload.remaining()];
                        payload.get(bytes);
                        if (total + bytes.length > maxBytes && total > 0) {
                            break;
                        }
                        messages.add(bytes);
                        total += bytes.length;
                        next = batch.lastOffset();
                        if (total >= maxBytes) {
                            break;
                        }
                    }
                    return new ReadResult(messages, entry.contentType(), OffsetToken.ofRecordOffset(next),
                        next >= end, entry.closed());
                } finally {
                    fetch.free();
                }
            } catch (StoreException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public boolean await(URI url, OffsetToken startOffset, Duration timeout) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path, false);
            if (entry == null) {
                return false;
            }
            if (entry.closed()) {
                return true;
            }
            Stream stream = ensureOpen(entry.streamId());
            if (stream.confirmOffset() > startOffset.recordOffset()) {
                return true;
            }
            return waiters.await(path, startOffset, timeout);
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    public OptionalLong lookupStreamId(String path) throws Exception {
        RegistryEntry entry = getEntry(path, false);
        return entry == null ? OptionalLong.empty() : OptionalLong.of(entry.streamId());
    }

    public Optional<Integer> ownerNodeId(long streamId) throws Exception {
        List<StreamMetadata> streams = metadataNode.client().readIndex(() ->
            metadataNode.stateMachine().streamControlManager().getStreams(List.of(streamId)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (streams.isEmpty() || streams.get(0).state() != StreamState.OPENED) {
            return Optional.empty();
        }
        return Optional.of(streams.get(0).nodeId());
    }

    public Collection<Stream> openStreamSnapshot() {
        return List.copyOf(openStreams.values());
    }

    public void shutdown() {
        waiters.clear();
        for (Stream stream : openStreams.values()) {
            try {
                stream.close().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        openStreams.clear();
    }

    private AppendResult handleProducerReject(
        RegistryEntry entry,
        OffsetToken next,
        ProducerDecision producer,
        AppendCommand request) throws StoreException {
        return switch (producer.status()) {
            case DUPLICATE -> new AppendResult(next, false, entry.closed(),
                request.producerEpoch(), producer.lastSeq());
            case STALE_EPOCH -> throw StoreException.fenced(producer.currentEpoch());
            case INVALID_EPOCH_SEQ -> throw new StoreException(
                StoreException.Kind.BAD_REQUEST, next, false, "New epoch must start with sequence 0");
            case SEQUENCE_GAP -> throw StoreException.sequenceGap(producer.expectedSeq(), producer.receivedSeq());
            default -> throw new StoreException(StoreException.Kind.BAD_REQUEST);
        };
    }

    private static ProducerDecision validateProducer(RegistryEntry entry, AppendCommand request) {
        if (!request.hasProducer()) {
            return null;
        }
        ProducerState state = entry.producers().get(request.producerId());
        long epoch = request.producerEpoch();
        long seq = request.producerSeq();
        if (state == null) {
            if (seq != 0L) {
                return ProducerDecision.invalidEpochSeq();
            }
            return ProducerDecision.accepted(epoch, seq);
        }
        if (epoch < state.epoch()) {
            return ProducerDecision.stale(state.epoch());
        }
        if (epoch > state.epoch()) {
            if (seq != 0L) {
                return ProducerDecision.invalidEpochSeq();
            }
            return ProducerDecision.accepted(epoch, seq);
        }
        if (seq <= state.lastSeq()) {
            return ProducerDecision.duplicate(state.lastSeq());
        }
        if (seq == state.lastSeq() + 1) {
            return ProducerDecision.accepted(epoch, seq);
        }
        return ProducerDecision.gap(state.lastSeq() + 1, seq);
    }

    private static boolean matchesClosedBy(RegistryEntry entry, AppendCommand request) {
        ClosedBy closedBy = entry.closedBy();
        return closedBy != null
            && request.hasProducer()
            && closedBy.producerId().equals(request.producerId())
            && closedBy.epoch() == request.producerEpoch()
            && closedBy.seq() == request.producerSeq();
    }

    private OffsetToken appendBytes(String path, Stream stream, byte[] bytes) throws Exception {
        io.streamstack.api.AppendResult result = stream.append(
            new DefaultRecordBatch(1, System.currentTimeMillis(), Map.of(), ByteBuffer.wrap(bytes)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        long next = Math.max(result.baseOffset() + 1, stream.nextOffset());
        waiters.notifyAppend(path, next);
        return OffsetToken.ofRecordOffset(next);
    }

    private static List<byte[]> splitMessages(String contentType, byte[] body, boolean create) throws StoreException {
        if (!SseEncoder.isJson(SseEncoder.mimeOf(contentType))) {
            return List.of(body);
        }
        try {
            JsonNode node = JSON.readTree(body);
            if (node.isArray()) {
                if (node.isEmpty()) {
                    if (create) {
                        return List.of();
                    }
                    throw new StoreException(StoreException.Kind.BAD_REQUEST, null, false, "empty JSON array not allowed");
                }
                List<byte[]> out = new ArrayList<>(node.size());
                for (JsonNode child : node) {
                    out.add(JSON.writeValueAsBytes(child));
                }
                return out;
            }
            return List.of(JSON.writeValueAsBytes(node));
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw new StoreException(StoreException.Kind.BAD_REQUEST, null, false, "invalid JSON");
        }
    }

    private Stream ensureOpen(long streamId) throws Exception {
        Stream existing = openStreams.get(streamId);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = openStreams.get(streamId);
            if (existing != null) {
                return existing;
            }
            long epoch = nextEpoch(streamId);
            Stream opened = streamClient.openStream(streamId, OpenStreamOptions.builder().epoch(epoch).build())
                .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            openStreams.put(streamId, opened);
            localEpochs.put(streamId, new AtomicLong(opened.streamEpoch()));
            return opened;
        }
    }

    private long nextEpoch(long streamId) throws Exception {
        AtomicLong local = localEpochs.get(streamId);
        if (local != null) {
            return local.incrementAndGet();
        }
        List<StreamMetadata> streams = metadataNode.client().readIndex(() ->
            metadataNode.stateMachine().streamControlManager().getStreams(List.of(streamId)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        long current = streams.isEmpty() ? 0L : streams.get(0).epoch();
        return localEpochs.computeIfAbsent(streamId, id -> new AtomicLong(current)).incrementAndGet();
    }

    private RegistryEntry getEntry(String path, boolean touch) throws Exception {
        Value value = kvClient.getKV(Key.of(path)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (value == null || value.isNull()) {
            return null;
        }
        RegistryEntry entry = RegistryEntry.decode(toBytes(value));
        if (entry.deadlineMs() > 0 && System.currentTimeMillis() > entry.deadlineMs()) {
            expire(path, entry);
            return null;
        }
        if (touch) {
            RegistryEntry refreshed = touchDeadline(entry);
            if (refreshed != entry) {
                putEntry(path, refreshed);
                return refreshed;
            }
        }
        return entry;
    }

    private void expire(String path, RegistryEntry entry) {
        try {
            kvClient.delKV(Key.of(path)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        destroyStream(entry.streamId());
        waiters.notifyClosed(path);
    }

    private void destroyStream(long streamId) {
        Stream stream = openStreams.remove(streamId);
        localEpochs.remove(streamId);
        try {
            if (stream == null) {
                stream = streamClient.openStream(streamId,
                    OpenStreamOptions.builder().epoch(nextEpoch(streamId)).build())
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            }
            stream.destroy().get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
    }

    private RegistryEntry requireEntry(String path, boolean touch) throws Exception {
        RegistryEntry entry = getEntry(path, touch);
        if (entry == null) {
            throw new IllegalStateException("missing registry entry for " + path);
        }
        return entry;
    }

    private void putEntry(String path, RegistryEntry entry) throws Exception {
        kvClient.putKV(KeyValue.of(path, ByteBuffer.wrap(entry.encode())))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    private static RegistryEntry touchDeadline(RegistryEntry entry) {
        if (entry.ttlSeconds() == null || entry.expiresAt() != null) {
            return entry;
        }
        long now = System.currentTimeMillis();
        long next = now + entry.ttlSeconds() * 1000L;
        long coarsen = Math.max(entry.ttlSeconds() * 100L, 1000L);
        if (entry.deadlineMs() > 0 && next - entry.deadlineMs() < coarsen) {
            return entry;
        }
        return entry.withDeadline(next);
    }

    private static long deadlineOf(Long ttlSeconds, Instant expiresAt) {
        if (expiresAt != null) {
            return expiresAt.toEpochMilli();
        }
        if (ttlSeconds != null) {
            return System.currentTimeMillis() + ttlSeconds * 1000L;
        }
        return 0L;
    }

    private StreamInfo toInfo(String path, RegistryEntry entry) throws Exception {
        return toInfo(path, entry, ensureOpen(entry.streamId()));
    }

    private StreamInfo toInfo(String path, RegistryEntry entry, Stream stream) throws Exception {
        return new StreamInfo(
            path,
            entry.streamId(),
            entry.contentType(),
            entry.ttlSeconds(),
            entry.expiresAt(),
            OffsetToken.ofRecordOffset(stream.nextOffset()),
            entry.closed(),
            ownerNodeId(entry.streamId()).orElse(null));
    }

    private static boolean configMatches(
        RegistryEntry entry,
        String contentType,
        Long ttl,
        Instant expiresAt,
        boolean closed) {
        return mimeEquals(entry.contentType(), contentType)
            && Objects.equals(entry.ttlSeconds(), ttl)
            && Objects.equals(entry.expiresAt(), expiresAt)
            && entry.closed() == closed;
    }

    private static boolean mimeEquals(String a, String b) {
        return SseEncoder.mimeOf(a).equals(SseEncoder.mimeOf(b));
    }

    static String pathOf(URI url) {
        String path = url.getPath();
        return path == null || path.isEmpty() ? "/" : path;
    }

    private static byte[] toBytes(Value value) {
        ByteBuffer buffer = value.get().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static StoreException wrap(Exception e) {
        if (e instanceof StoreException se) {
            return se;
        }
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throw new RuntimeException(e);
    }

    record ProducerState(long epoch, long lastSeq) {
    }

    record ClosedBy(String producerId, long epoch, long seq) {
    }

    record ProducerDecision(
        Status status,
        long lastSeq,
        long currentEpoch,
        long expectedSeq,
        long receivedSeq) {
        enum Status { ACCEPTED, DUPLICATE, STALE_EPOCH, INVALID_EPOCH_SEQ, SEQUENCE_GAP }

        static ProducerDecision accepted(long epoch, long seq) {
            return new ProducerDecision(Status.ACCEPTED, seq, epoch, 0, 0);
        }

        static ProducerDecision duplicate(long lastSeq) {
            return new ProducerDecision(Status.DUPLICATE, lastSeq, 0, 0, 0);
        }

        static ProducerDecision stale(long currentEpoch) {
            return new ProducerDecision(Status.STALE_EPOCH, 0, currentEpoch, 0, 0);
        }

        static ProducerDecision invalidEpochSeq() {
            return new ProducerDecision(Status.INVALID_EPOCH_SEQ, 0, 0, 0, 0);
        }

        static ProducerDecision gap(long expected, long received) {
            return new ProducerDecision(Status.SEQUENCE_GAP, 0, 0, expected, received);
        }
    }

    record RegistryEntry(
        long streamId,
        String contentType,
        Long ttlSeconds,
        Instant expiresAt,
        boolean closed,
        long deadlineMs,
        String lastSeq,
        Map<String, ProducerState> producers,
        ClosedBy closedBy) {

        RegistryEntry {
            producers = producers == null ? Map.of() : Map.copyOf(producers);
        }

        RegistryEntry close(ClosedBy by) {
            return new RegistryEntry(streamId, contentType, ttlSeconds, expiresAt, true, deadlineMs, lastSeq,
                producers, by != null ? by : closedBy);
        }

        RegistryEntry withLastSeq(String seq) {
            return new RegistryEntry(streamId, contentType, ttlSeconds, expiresAt, closed, deadlineMs, seq,
                producers, closedBy);
        }

        RegistryEntry withDeadline(long deadline) {
            return new RegistryEntry(streamId, contentType, ttlSeconds, expiresAt, closed, deadline, lastSeq,
                producers, closedBy);
        }

        RegistryEntry withProducer(String id, long epoch, long seq) {
            Map<String, ProducerState> next = new LinkedHashMap<>(producers);
            next.put(id, new ProducerState(epoch, seq));
            return new RegistryEntry(streamId, contentType, ttlSeconds, expiresAt, closed, deadlineMs, lastSeq,
                next, closedBy);
        }

        byte[] encode() {
            byte[] ct = contentType.getBytes(StandardCharsets.UTF_8);
            byte[] seqBytes = lastSeq == null ? new byte[0] : lastSeq.getBytes(StandardCharsets.UTF_8);
            byte[] closedById = closedBy == null ? new byte[0]
                : closedBy.producerId().getBytes(StandardCharsets.UTF_8);
            int producersSize = 4;
            for (Map.Entry<String, ProducerState> e : producers.entrySet()) {
                producersSize += 4 + e.getKey().getBytes(StandardCharsets.UTF_8).length + 8 + 8;
            }
            ByteBuffer buf = ByteBuffer.allocate(
                1 + 8 + 4 + ct.length + 1 + 8 + 1 + 8 + 1 + 8 + 4 + seqBytes.length + 1 + 4 + closedById.length
                    + (closedBy == null ? 0 : 16) + producersSize);
            buf.put((byte) 1);
            buf.putLong(streamId);
            buf.putInt(ct.length);
            buf.put(ct);
            buf.put((byte) (ttlSeconds != null ? 1 : 0));
            buf.putLong(ttlSeconds != null ? ttlSeconds : 0L);
            buf.put((byte) (expiresAt != null ? 1 : 0));
            buf.putLong(expiresAt != null ? expiresAt.toEpochMilli() : 0L);
            buf.put((byte) (closed ? 1 : 0));
            buf.putLong(deadlineMs);
            buf.putInt(seqBytes.length);
            buf.put(seqBytes);
            buf.put((byte) (closedBy != null ? 1 : 0));
            if (closedBy != null) {
                buf.putInt(closedById.length);
                buf.put(closedById);
                buf.putLong(closedBy.epoch());
                buf.putLong(closedBy.seq());
            }
            buf.putInt(producers.size());
            for (Map.Entry<String, ProducerState> e : producers.entrySet()) {
                byte[] id = e.getKey().getBytes(StandardCharsets.UTF_8);
                buf.putInt(id.length);
                buf.put(id);
                buf.putLong(e.getValue().epoch());
                buf.putLong(e.getValue().lastSeq());
            }
            return trim(buf);
        }

        static RegistryEntry decode(byte[] bytes) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            byte version = buf.get();
            if (version != 1) {
                throw new IllegalStateException("unknown registry entry version " + version);
            }
            long streamId = buf.getLong();
            byte[] ct = new byte[buf.getInt()];
            buf.get(ct);
            Long ttl = null;
            if (buf.get() == 1) {
                ttl = buf.getLong();
            } else {
                buf.getLong();
            }
            Instant expiresAt = null;
            if (buf.get() == 1) {
                expiresAt = Instant.ofEpochMilli(buf.getLong());
            } else {
                buf.getLong();
            }
            boolean closed = buf.get() == 1;
            long deadlineMs = buf.getLong();
            byte[] seqBytes = new byte[buf.getInt()];
            buf.get(seqBytes);
            String lastSeq = seqBytes.length == 0 ? null : new String(seqBytes, StandardCharsets.UTF_8);
            ClosedBy closedBy = null;
            if (buf.get() == 1) {
                byte[] id = new byte[buf.getInt()];
                buf.get(id);
                closedBy = new ClosedBy(new String(id, StandardCharsets.UTF_8), buf.getLong(), buf.getLong());
            }
            int count = buf.getInt();
            Map<String, ProducerState> producers = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) {
                byte[] id = new byte[buf.getInt()];
                buf.get(id);
                producers.put(new String(id, StandardCharsets.UTF_8), new ProducerState(buf.getLong(), buf.getLong()));
            }
            return new RegistryEntry(streamId, new String(ct, StandardCharsets.UTF_8), ttl, expiresAt, closed,
                deadlineMs, lastSeq, producers, closedBy);
        }

        private static byte[] trim(ByteBuffer buf) {
            byte[] out = new byte[buf.position()];
            buf.flip();
            buf.get(out);
            return out;
        }
    }
}
