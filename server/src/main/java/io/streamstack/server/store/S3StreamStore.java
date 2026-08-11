package io.streamstack.server.store;

import io.streamstack.DefaultRecordBatch;
import io.streamstack.api.AppendResult;
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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
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

    private final StreamClient streamClient;
    private final KVClient kvClient;
    private final MetadataNode metadataNode;
    private final StreamWaiterRegistry waiters;
    private final ConcurrentHashMap<Long, Stream> openStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, AtomicLong> localEpochs = new ConcurrentHashMap<>();

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
    public CreateResult create(URI url, String contentType, Long ttlSeconds, Instant expiresAt, InputStream initialBody)
        throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry existing = getEntry(path);
            if (existing != null) {
                if (!configMatches(existing, contentType, ttlSeconds, expiresAt)) {
                    throw new StoreException(StoreException.Kind.CONFLICT);
                }
                return new CreateResult(false, toInfo(path, existing));
            }

            Stream stream = streamClient.createAndOpenStream(
                CreateStreamOptions.builder().epoch(1).replicaCount(1).tag("path", path).build())
                .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            openStreams.put(stream.streamId(), stream);
            localEpochs.put(stream.streamId(), new AtomicLong(stream.streamEpoch()));

            RegistryEntry candidate = new RegistryEntry(stream.streamId(), contentType, ttlSeconds, expiresAt, false);
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
                if (!configMatches(current, contentType, ttlSeconds, expiresAt)) {
                    throw new StoreException(StoreException.Kind.CONFLICT);
                }
                return new CreateResult(false, toInfo(path, current));
            }

            if (initialBody != null) {
                byte[] body = initialBody.readAllBytes();
                if (body.length > 0) {
                    appendInternal(path, stream, body);
                    current = requireEntry(path);
                }
            }
            return new CreateResult(true, toInfo(path, current, stream));
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public OffsetToken append(URI url, String contentType, InputStream body) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
            if (entry == null) {
                throw new StoreException(StoreException.Kind.NOT_FOUND);
            }
            Stream stream = ensureOpen(entry.streamId());
            OffsetToken next = OffsetToken.ofRecordOffset(stream.nextOffset());
            if (entry.closed()) {
                throw new StoreException(StoreException.Kind.CLOSED, next, true);
            }
            if (!entry.contentType().equals(contentType)) {
                throw new StoreException(StoreException.Kind.CONFLICT, next, false);
            }
            byte[] bytes = body == null ? new byte[0] : body.readAllBytes();
            if (bytes.length == 0) {
                throw new StoreException(StoreException.Kind.BAD_REQUEST);
            }
            return appendInternal(path, stream, bytes);
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public OffsetToken close(URI url) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
            if (entry == null) {
                throw new StoreException(StoreException.Kind.NOT_FOUND);
            }
            Stream stream = ensureOpen(entry.streamId());
            OffsetToken next = OffsetToken.ofRecordOffset(stream.nextOffset());
            if (entry.closed()) {
                throw new StoreException(StoreException.Kind.CLOSED, next, true);
            }
            kvClient.putKV(KeyValue.of(path, ByteBuffer.wrap(entry.close().encode())))
                .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            waiters.notifyClosed(path);
            return next;
        } catch (StoreException e) {
            throw e;
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public boolean delete(URI url) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
            if (entry == null) {
                return false;
            }
            Value deleted = kvClient.delKV(Key.of(path)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (deleted == null) {
                return false;
            }
            Stream stream = openStreams.remove(entry.streamId());
            localEpochs.remove(entry.streamId());
            if (stream == null) {
                try {
                    stream = streamClient.openStream(entry.streamId(),
                        OpenStreamOptions.builder().epoch(nextEpoch(entry.streamId())).build())
                        .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
                } catch (Exception e) {
                    waiters.notifyClosed(path);
                    return true;
                }
            }
            try {
                stream.destroy().get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
            waiters.notifyClosed(path);
            return true;
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public Optional<StreamInfo> head(URI url) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
            return entry == null ? Optional.empty() : Optional.of(toInfo(path, entry));
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    @Override
    public ReadResult read(URI url, OffsetToken startOffset, int maxBytes) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
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
                return new ReadResult(new byte[0], entry.contentType(), OffsetToken.ofRecordOffset(end), true,
                    entry.closed());
            }
            FetchResult fetch = stream.fetch(start, end, maxBytes).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                long next = start;
                for (RecordBatchWithContext batch : fetch.recordBatchList()) {
                    ByteBuffer payload = batch.rawPayload();
                    byte[] bytes = new byte[payload.remaining()];
                    payload.get(bytes);
                    if (out.size() + bytes.length > maxBytes && out.size() > 0) {
                        break;
                    }
                    out.write(bytes);
                    next = batch.lastOffset();
                    if (out.size() >= maxBytes) {
                        break;
                    }
                }
                return new ReadResult(out.toByteArray(), entry.contentType(), OffsetToken.ofRecordOffset(next),
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

    @Override
    public boolean await(URI url, OffsetToken startOffset, Duration timeout) throws StoreException {
        try {
            String path = pathOf(url);
            RegistryEntry entry = getEntry(path);
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
        RegistryEntry entry = getEntry(path);
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

    private OffsetToken appendInternal(String path, Stream stream, byte[] bytes) throws Exception {
        AppendResult result = stream.append(
            new DefaultRecordBatch(1, System.currentTimeMillis(), Map.of(), ByteBuffer.wrap(bytes)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        long next = Math.max(result.baseOffset() + 1, stream.nextOffset());
        waiters.notifyAppend(path, next);
        return OffsetToken.ofRecordOffset(next);
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

    private RegistryEntry getEntry(String path) throws Exception {
        Value value = kvClient.getKV(Key.of(path)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (value == null || value.isNull()) {
            return null;
        }
        return RegistryEntry.decode(toBytes(value));
    }

    private RegistryEntry requireEntry(String path) throws Exception {
        RegistryEntry entry = getEntry(path);
        if (entry == null) {
            throw new IllegalStateException("missing registry entry for " + path);
        }
        return entry;
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

    private static boolean configMatches(RegistryEntry entry, String contentType, Long ttl, Instant expiresAt) {
        return entry.contentType().equals(contentType)
            && Objects.equals(entry.ttlSeconds(), ttl)
            && Objects.equals(entry.expiresAt(), expiresAt);
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

    record RegistryEntry(long streamId, String contentType, Long ttlSeconds, Instant expiresAt, boolean closed) {
        RegistryEntry close() {
            return new RegistryEntry(streamId, contentType, ttlSeconds, expiresAt, true);
        }

        byte[] encode() {
            byte[] ct = contentType.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 4 + ct.length + 1 + 8 + 1 + 8 + 1);
            buf.put((byte) 1);
            buf.putLong(streamId);
            buf.putInt(ct.length);
            buf.put(ct);
            buf.put((byte) (ttlSeconds != null ? 1 : 0));
            buf.putLong(ttlSeconds != null ? ttlSeconds : 0L);
            buf.put((byte) (expiresAt != null ? 1 : 0));
            buf.putLong(expiresAt != null ? expiresAt.toEpochMilli() : 0L);
            buf.put((byte) (closed ? 1 : 0));
            return buf.array();
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
            return new RegistryEntry(streamId, new String(ct, StandardCharsets.UTF_8), ttl, expiresAt, closed);
        }
    }
}
