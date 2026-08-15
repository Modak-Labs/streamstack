package io.streamstack.metadata.raft;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamstack.s3.network.ThrottleStrategy;
import io.streamstack.s3.operator.ObjectStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class ObjectStorageSnapshotArchive implements SnapshotArchive {

    public static final int DEFAULT_RETAIN_COUNT = 5;

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageSnapshotArchive.class);

    private static final String SUFFIX = ".bin";
    private static final long OPERATION_TIMEOUT_SEC = 60;

    private final ObjectStorage storage;
    private final String prefix;
    private final int retainCount;

    private final AtomicReference<PendingSnapshot> pending = new AtomicReference<>();
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final AtomicLong lastArchivedIndex = new AtomicLong(-1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ExecutorService uploader = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "metadata-snapshot-archive");

        t.setDaemon(true);

        return t;
    });

    public ObjectStorageSnapshotArchive(ObjectStorage storage, String prefix) {
        this(storage, prefix, DEFAULT_RETAIN_COUNT);
    }

    public ObjectStorageSnapshotArchive(ObjectStorage storage, String prefix, int retainCount) {
        this.storage = Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(prefix, "prefix");

        if (prefix.isBlank()) {
            throw new IllegalArgumentException("prefix must not be blank");
        }

        if (retainCount < 1) {
            throw new IllegalArgumentException("retainCount must be >= 1");
        }

        this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        this.retainCount = retainCount;
    }

    public String prefix() {
        return prefix;
    }

    @Override
    public void submit(long appliedIndex, byte[] snapshotBytes) {
        Objects.requireNonNull(snapshotBytes, "snapshotBytes");

        if (closed.get()) {
            return;
        }

        pending.set(new PendingSnapshot(appliedIndex, snapshotBytes));
        uploader.execute(this::drain);
    }

    private void drain() {
        PendingSnapshot next = pending.getAndSet(null);

        if (Objects.isNull(next) || closed.get()) {
            return;
        }

        String key = key(next.appliedIndex(), System.currentTimeMillis());

        try {
            ByteBuf buf = Unpooled.wrappedBuffer(next.bytes());

            storage.write(writeOptions(), key, buf).get(OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
            successCount.incrementAndGet();
            lastArchivedIndex.set(next.appliedIndex());
            LOGGER.info("archived metadata snapshot appliedIndex={} key={} size={}",
                next.appliedIndex(), key, next.bytes().length);
            prune();
        } catch (Exception e) {
            failureCount.incrementAndGet();
            LOGGER.warn("failed to archive metadata snapshot appliedIndex={} key={}: {}",
                next.appliedIndex(), key, e.getMessage());
        }
    }

    private void prune() {
        try {
            List<ArchivedSnapshot> snapshots = list();

            if (snapshots.size() <= retainCount) {
                return;
            }

            List<ObjectStorage.ObjectPath> stale = new ArrayList<>();

            for (ArchivedSnapshot snapshot : snapshots.subList(0, snapshots.size() - retainCount)) {
                stale.add(new ObjectStorage.ObjectPath(storage.bucketId(), snapshot.key()));
            }

            storage.delete(stale).get(OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.warn("failed to prune archived metadata snapshots: {}", e.getMessage());
        }
    }

    @Override
    public List<ArchivedSnapshot> list() {
        try {
            List<ObjectStorage.ObjectInfo> objects =
                storage.list(prefix).get(OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);
            List<ArchivedSnapshot> snapshots = new ArrayList<>();

            for (ObjectStorage.ObjectInfo info : objects) {
                ArchivedSnapshot snapshot = parse(info);

                if (Objects.nonNull(snapshot)) {
                    snapshots.add(snapshot);
                }
            }

            snapshots.sort(Comparator.comparing(ArchivedSnapshot::key));

            return snapshots;
        } catch (Exception e) {
            throw new IllegalStateException("failed to list archived metadata snapshots", e);
        }
    }

    @Override
    public Optional<ArchivedSnapshot> latest() {
        List<ArchivedSnapshot> snapshots = list();

        return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(snapshots.size() - 1));
    }

    @Override
    public byte[] read(ArchivedSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");

        try {
            ByteBuf buf = storage.rangeRead(readOptions(), snapshot.key(), 0, ObjectStorage.RANGE_READ_TO_END)
                .get(OPERATION_TIMEOUT_SEC, TimeUnit.SECONDS);

            try {
                byte[] bytes = new byte[buf.readableBytes()];

                buf.readBytes(bytes);

                return bytes;
            } finally {
                buf.release();
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to read archived metadata snapshot " + snapshot.key(), e);
        }
    }

    @Override
    public long successCount() {
        return successCount.get();
    }

    @Override
    public long failureCount() {
        return failureCount.get();
    }

    @Override
    public long lastArchivedIndex() {
        return lastArchivedIndex.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        uploader.shutdown();

        try {
            if (!uploader.awaitTermination(10, TimeUnit.SECONDS)) {
                uploader.shutdownNow();
            }
        } catch (InterruptedException e) {
            uploader.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private String key(long appliedIndex, long timestampMs) {
        return prefix + String.format("%020d-%d%s", appliedIndex, timestampMs, SUFFIX);
    }

    private ArchivedSnapshot parse(ObjectStorage.ObjectInfo info) {
        String key = info.key();

        if (!key.startsWith(prefix) || !key.endsWith(SUFFIX)) {
            return null;
        }

        String name = key.substring(prefix.length(), key.length() - SUFFIX.length());
        int separator = name.indexOf('-');

        if (separator <= 0) {
            return null;
        }

        try {
            long appliedIndex = Long.parseLong(name.substring(0, separator));
            long timestampMs = Long.parseLong(name.substring(separator + 1));

            return new ArchivedSnapshot(key, appliedIndex, timestampMs, info.size());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static ObjectStorage.WriteOptions writeOptions() {
        return new ObjectStorage.WriteOptions().throttleStrategy(ThrottleStrategy.BYPASS);
    }

    private ObjectStorage.ReadOptions readOptions() {
        return new ObjectStorage.ReadOptions().bucket(storage.bucketId()).throttleStrategy(ThrottleStrategy.BYPASS);
    }

    private record PendingSnapshot(long appliedIndex, byte[] bytes) {
    }
}
