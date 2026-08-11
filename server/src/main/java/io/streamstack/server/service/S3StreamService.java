package io.streamstack.server.service;

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
import io.streamstack.server.ContentTypes;
import io.streamstack.server.StreamWaiterRegistry;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.Producer;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.CloseResult;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamRecord;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class S3StreamService implements StreamLifecycleService, AppendService, ReadService {
    private static final long OP_TIMEOUT_SEC = 30;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final StreamClient streamClient;
    private final KVClient kvClient;
    private final MetadataNode metadataNode;
    private final StreamWaiterRegistry waiters;
    private final ConcurrentHashMap<Long, Stream> openStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicLong> localEpochs = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> nameLocks = new ConcurrentHashMap<>();

    public S3StreamService(
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
    public CreateResult create(CreateCommand command) throws StreamServiceException {
        String name = normalize(command.name());
        Object lock = nameLocks.computeIfAbsent(name, n -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry existing = getEntry(name, false);
                if (existing != null) {
                    if (!configMatches(existing, command.contentType(), command.ttlSeconds(), command.expiresAt(),
                        command.closed())) {
                        throw new StreamServiceException(StreamServiceException.Kind.CONFLICT);
                    }
                    return new CreateResult(false, toMeta(name, existing));
                }

                Stream stream = streamClient.createAndOpenStream(
                    CreateStreamOptions.builder().epoch(1).replicaCount(1).tag("path", name).build())
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                openStreams.put(stream.streamId(), stream);
                localEpochs.put(stream.streamId(), new AtomicLong(stream.streamEpoch()));

                long deadline = deadlineOf(command.ttlSeconds(), command.expiresAt());
                RegistryEntry candidate = new RegistryEntry(
                    stream.streamId(), command.contentType(), command.ttlSeconds(), command.expiresAt(),
                    command.closed(), deadline, null, Map.of(), null);
                Value stored = kvClient.putKVIfAbsent(KeyValue.of(name, ByteBuffer.wrap(candidate.encode())))
                    .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                RegistryEntry current = RegistryEntry.decode(toBytes(stored));

                if (current.streamId() != candidate.streamId()) {
                    try {
                        stream.destroy().get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                    }
                    openStreams.remove(stream.streamId());
                    localEpochs.remove(stream.streamId());
                    if (!configMatches(current, command.contentType(), command.ttlSeconds(), command.expiresAt(),
                        command.closed())) {
                        throw new StreamServiceException(StreamServiceException.Kind.CONFLICT);
                    }
                    return new CreateResult(false, toMeta(name, current));
                }

                if (command.initialPayload().length > 0) {
                    List<byte[]> messages = splitMessages(command.contentType(), command.initialPayload(), true);
                    for (byte[] message : messages) {
                        appendBytes(name, stream, message);
                    }
                    if (!messages.isEmpty()) {
                        current = requireEntry(name, false);
                    }
                }
                if (command.closed() && !current.closed()) {
                    putEntry(name, current.close(null));
                    current = requireEntry(name, false);
                }
                return new CreateResult(true, toMeta(name, current, stream));
            } catch (StreamServiceException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public Optional<StreamMeta> head(String name) throws StreamServiceException {
        try {
            String normalized = normalize(name);
            RegistryEntry entry = getEntry(normalized, false);
            return entry == null ? Optional.empty() : Optional.of(toMeta(normalized, entry));
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public CloseResult close(String name) throws StreamServiceException {
        AppendResult result = append(new AppendCommand(name, List.of(), null, null, null, true));
        return new CloseResult(result.nextOffset());
    }

    @Override
    public boolean delete(String name) throws StreamServiceException {
        String normalized = normalize(name);
        Object lock = nameLocks.computeIfAbsent(normalized, n -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(normalized, false);
                if (entry == null) {
                    return false;
                }
                Value deleted = kvClient.delKV(Key.of(normalized)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                if (deleted == null) {
                    return false;
                }
                destroyStream(entry.streamId());
                waiters.notifyClosed(normalized);
                return true;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public AppendResult append(AppendCommand command) throws StreamServiceException {
        String name = normalize(command.name());
        Object lock = nameLocks.computeIfAbsent(name, n -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(name, false);
                if (entry == null) {
                    throw new StreamServiceException(StreamServiceException.Kind.NOT_FOUND);
                }
                Stream stream = ensureOpen(entry.streamId());
                OffsetToken next = OffsetToken.ofRecordOffset(stream.nextOffset());

                ProducerDecision producer = validateProducer(entry, command);
                if (producer != null && producer.status() != ProducerDecision.Status.ACCEPTED) {
                    return handleProducerReject(entry, next, producer, command);
                }

                byte[] body = command.concatenatedPayload();
                if (entry.closed()) {
                    if (command.closeAfter() && body.length == 0) {
                        if (producer != null && !matchesClosedBy(entry, command) && entry.closedBy() != null) {
                            throw new StreamServiceException(StreamServiceException.Kind.CLOSED, next, true);
                        }
                        Long echoedSeq = command.hasProducer()
                            ? (entry.closedBy() != null ? entry.closedBy().seq() : command.producer().seq())
                            : null;
                        Long echoedEpoch = command.hasProducer() ? command.producer().epoch() : null;
                        return new AppendResult(next, false, true, echoedEpoch, echoedSeq);
                    }
                    if (producer != null && matchesClosedBy(entry, command)) {
                        return new AppendResult(next, false, true, command.producer().epoch(), entry.closedBy().seq());
                    }
                    throw new StreamServiceException(StreamServiceException.Kind.CLOSED, next, true);
                }

                boolean closeOnly = body.length == 0 && command.closeAfter();
                if (!closeOnly) {
                    if (command.contentType() == null || command.contentType().isEmpty()) {
                        throw new StreamServiceException(
                            StreamServiceException.Kind.BAD_REQUEST, next, false, "missing Content-Type");
                    }
                    if (!ContentTypes.mimeEquals(entry.contentType(), command.contentType())) {
                        throw new StreamServiceException(StreamServiceException.Kind.CONFLICT, next, false);
                    }
                    if (body.length == 0) {
                        throw new StreamServiceException(
                            StreamServiceException.Kind.BAD_REQUEST, next, false, "Empty body");
                    }
                }

                if (command.streamSeq() != null
                    && (producer == null || producer.status() == ProducerDecision.Status.ACCEPTED)) {
                    checkStreamSeq(entry, command.streamSeq(), next);
                }

                if (!closeOnly) {
                    List<byte[]> messages = expandPayloads(entry.contentType(), command.payloads());
                    for (byte[] message : messages) {
                        next = appendBytes(name, stream, message);
                    }
                }

                RegistryEntry updated = entry;
                if (command.streamSeq() != null) {
                    updated = updated.withLastSeq(OffsetToken.ofRecordOffset(command.streamSeq()).value());
                }
                if (producer != null && producer.status() == ProducerDecision.Status.ACCEPTED) {
                    Producer ctx = command.producer();
                    updated = updated.withProducer(ctx.producerId(), ctx.epoch(), ctx.seq());
                }
                ClosedBy closedBy = null;
                if (command.closeAfter()) {
                    if (command.hasProducer()) {
                        Producer ctx = command.producer();
                        closedBy = new ClosedBy(ctx.producerId(), ctx.epoch(), ctx.seq());
                    }
                    updated = updated.close(closedBy);
                }
                updated = touchDeadline(updated);
                putEntry(name, updated);
                if (command.closeAfter()) {
                    waiters.notifyClosed(name);
                }
                Long echoedSeq = command.hasProducer() ? command.producer().seq() : null;
                Long echoedEpoch = command.hasProducer() ? command.producer().epoch() : null;
                return new AppendResult(next, !closeOnly, command.closeAfter(), echoedEpoch, echoedSeq);
            } catch (StreamServiceException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public ReadResult read(String name, OffsetToken from, int maxBytes, int maxRecords) throws StreamServiceException {
        name = normalize(name);
        Object lock = nameLocks.computeIfAbsent(name, n -> new Object());
        synchronized (lock) {
            try {
                RegistryEntry entry = getEntry(name, true);
                if (entry == null) {
                    throw new StreamServiceException(StreamServiceException.Kind.NOT_FOUND);
                }
                Stream stream = ensureOpen(entry.streamId());
                long start = from.recordOffset();
                long end = stream.confirmOffset();
                if (start > end) {
                    throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST);
                }
                if (start == end) {
                    return new ReadResult(List.of(), entry.contentType(), OffsetToken.ofRecordOffset(end), true,
                        entry.closed());
                }
                maxBytes = maxBytes > 0 ? maxBytes : Integer.MAX_VALUE;
                maxRecords = maxRecords > 0 ? maxRecords : Integer.MAX_VALUE;
                FetchResult fetch = stream.fetch(start, end, maxBytes).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                try {
                    List<StreamRecord> records = new ArrayList<>();
                    int total = 0;
                    long next = start;
                    for (RecordBatchWithContext batch : fetch.recordBatchList()) {
                        ByteBuffer payload = batch.rawPayload();
                        byte[] bytes = new byte[payload.remaining()];
                        payload.get(bytes);
                        if (total + bytes.length > maxBytes && total > 0) {
                            break;
                        }
                        records.add(new StreamRecord(OffsetToken.ofRecordOffset(batch.baseOffset()), bytes));
                        total += bytes.length;
                        next = batch.lastOffset();
                        if (total >= maxBytes || records.size() >= maxRecords) {
                            break;
                        }
                    }
                    return new ReadResult(records, entry.contentType(), OffsetToken.ofRecordOffset(next),
                        next >= end, entry.closed());
                } finally {
                    fetch.free();
                }
            } catch (StreamServiceException e) {
                throw e;
            } catch (Exception e) {
                throw wrap(e);
            }
        }
    }

    @Override
    public boolean await(String name, OffsetToken from, Duration timeout) throws StreamServiceException {
        try {
            String normalized = normalize(name);
            RegistryEntry entry = getEntry(normalized, false);
            if (entry == null) {
                return false;
            }
            if (entry.closed()) {
                return true;
            }
            Stream stream = ensureOpen(entry.streamId());
            if (stream.confirmOffset() > from.recordOffset()) {
                return true;
            }
            return waiters.await(normalized, from, timeout);
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> whenAppended(String name, OffsetToken from, Duration timeout) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return await(name, from, timeout);
            } catch (StreamServiceException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public OptionalLong lookupStreamId(String name) throws Exception {
        RegistryEntry entry = getEntry(normalize(name), false);
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
        AppendCommand command) throws StreamServiceException {
        return switch (producer.status()) {
            case DUPLICATE -> new AppendResult(next, false, entry.closed(),
                command.producer().epoch(), producer.lastSeq());
            case STALE_EPOCH -> throw StreamServiceException.fenced(producer.currentEpoch());
            case INVALID_EPOCH_SEQ -> throw new StreamServiceException(
                StreamServiceException.Kind.BAD_REQUEST, next, false, "New epoch must start with sequence 0");
            case SEQUENCE_GAP -> throw StreamServiceException.sequenceGap(producer.expectedSeq(), producer.receivedSeq());
            default -> throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST);
        };
    }

    private static ProducerDecision validateProducer(RegistryEntry entry, AppendCommand command) {
        if (!command.hasProducer()) {
            return null;
        }
        Producer ctx = command.producer();
        ProducerState state = entry.producers().get(ctx.producerId());
        long epoch = ctx.epoch();
        long seq = ctx.seq();
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

    private static boolean matchesClosedBy(RegistryEntry entry, AppendCommand command) {
        ClosedBy closedBy = entry.closedBy();
        return closedBy != null
            && command.hasProducer()
            && closedBy.producerId().equals(command.producer().producerId())
            && closedBy.epoch() == command.producer().epoch()
            && closedBy.seq() == command.producer().seq();
    }

    private static void checkStreamSeq(
        RegistryEntry entry,
        long expectedRecordOffset,
        OffsetToken next) throws StreamServiceException {
        if (entry.lastSeq() == null) {
            return;
        }
        long last;
        try {
            last = OffsetToken.parse(entry.lastSeq()).recordOffset();
        } catch (IllegalArgumentException e) {
            // Legacy opaque string seq: fall back to lexicographic compare on padded value.
            String expected = OffsetToken.ofRecordOffset(expectedRecordOffset).value();
            if (expected.compareTo(entry.lastSeq()) <= 0) {
                throw new StreamServiceException(
                    StreamServiceException.Kind.CONFLICT, next, false, "Sequence conflict");
            }
            return;
        }
        if (expectedRecordOffset <= last) {
            throw new StreamServiceException(
                StreamServiceException.Kind.CONFLICT, next, false, "Sequence conflict");
        }
    }

    private OffsetToken appendBytes(String name, Stream stream, byte[] bytes) throws Exception {
        io.streamstack.api.AppendResult result = stream.append(
            new DefaultRecordBatch(1, System.currentTimeMillis(), Map.of(), ByteBuffer.wrap(bytes)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        long next = Math.max(result.baseOffset() + 1, stream.nextOffset());
        waiters.notifyAppend(name, next);
        return OffsetToken.ofRecordOffset(next);
    }

    private static List<byte[]> expandPayloads(String contentType, List<byte[]> payloads)
        throws StreamServiceException {
        if (payloads.isEmpty()) {
            return List.of();
        }
        if (payloads.size() == 1) {
            return splitMessages(contentType, payloads.get(0), false);
        }
        List<byte[]> out = new ArrayList<>();
        for (byte[] payload : payloads) {
            out.addAll(splitMessages(contentType, payload, false));
        }
        return out;
    }

    private static List<byte[]> splitMessages(String contentType, byte[] body, boolean create)
        throws StreamServiceException {
        if (!ContentTypes.isJson(ContentTypes.mimeOf(contentType))) {
            return List.of(body);
        }
        try {
            JsonNode node = JSON.readTree(body);
            if (node.isArray()) {
                if (node.isEmpty()) {
                    if (create) {
                        return List.of();
                    }
                    throw new StreamServiceException(
                        StreamServiceException.Kind.BAD_REQUEST, null, false, "empty JSON array not allowed");
                }
                List<byte[]> out = new ArrayList<>(node.size());
                for (JsonNode child : node) {
                    out.add(JSON.writeValueAsBytes(child));
                }
                return out;
            }
            return List.of(JSON.writeValueAsBytes(node));
        } catch (StreamServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST, null, false, "invalid JSON");
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

    private RegistryEntry getEntry(String name, boolean touch) throws Exception {
        Value value = kvClient.getKV(Key.of(name)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (value == null || value.isNull()) {
            return null;
        }
        RegistryEntry entry = RegistryEntry.decode(toBytes(value));
        if (entry.deadlineMs() > 0 && System.currentTimeMillis() > entry.deadlineMs()) {
            expire(name, entry);
            return null;
        }
        if (touch) {
            RegistryEntry refreshed = touchDeadline(entry);
            if (refreshed != entry) {
                putEntry(name, refreshed);
                return refreshed;
            }
        }
        return entry;
    }

    private void expire(String name, RegistryEntry entry) {
        try {
            kvClient.delKV(Key.of(name)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        destroyStream(entry.streamId());
        waiters.notifyClosed(name);
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

    private RegistryEntry requireEntry(String name, boolean touch) throws Exception {
        RegistryEntry entry = getEntry(name, touch);
        if (entry == null) {
            throw new IllegalStateException("missing registry entry for " + name);
        }
        return entry;
    }

    private void putEntry(String name, RegistryEntry entry) throws Exception {
        kvClient.putKV(KeyValue.of(name, ByteBuffer.wrap(entry.encode())))
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

    private StreamMeta toMeta(String name, RegistryEntry entry) throws Exception {
        return toMeta(name, entry, ensureOpen(entry.streamId()));
    }

    private StreamMeta toMeta(String name, RegistryEntry entry, Stream stream) throws Exception {
        return new StreamMeta(
            name,
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
        Long ttlSeconds,
        Instant expiresAt,
        boolean closed) {
        return ContentTypes.mimeEquals(entry.contentType(), contentType)
            && Objects.equals(entry.ttlSeconds(), ttlSeconds)
            && Objects.equals(entry.expiresAt(), expiresAt)
            && entry.closed() == closed;
    }

    static String normalize(String name) {
        if (name == null || name.isEmpty()) {
            return "/";
        }
        return name;
    }

    private static byte[] toBytes(Value value) {
        ByteBuffer buffer = value.get().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static StreamServiceException wrap(Exception e) {
        if (e instanceof StreamServiceException se) {
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
