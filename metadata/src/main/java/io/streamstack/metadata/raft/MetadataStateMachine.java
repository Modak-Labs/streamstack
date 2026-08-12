package io.streamstack.metadata.raft;

import java.util.Objects;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

import io.streamstack.metadata.MetadataException;
import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.metadata.codec.MetadataCommandCodec;
import io.streamstack.metadata.stream.KVControlManager;
import io.streamstack.metadata.stream.S3ObjectControlManager;
import io.streamstack.metadata.stream.StreamControlManager;
import io.streamstack.s3.objects.CommitStreamSetObjectResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public final class MetadataStateMachine extends StateMachineAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStateMachine.class);

    private static final String SNAPSHOT_FILE = "metadata_snapshot.bin";
    private final StreamControlManager streamControlManager;
    private final S3ObjectControlManager objectControlManager;
    private final KVControlManager kvControlManager;

    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    private final AtomicLong leaderTerm = new AtomicLong(-1);

    private final AtomicLong appliedIndex = new AtomicLong(0);

    private final AtomicLong applySuccessCount = new AtomicLong(0);

    private final AtomicLong applyFailCount = new AtomicLong(0);
    private final ConcurrentSkipListMap<Long, CopyOnWriteArrayList<CompletableFuture<Void>>> applyWaiters =
        new ConcurrentSkipListMap<>();

    private final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "metadata-listener-dispatch");

        t.setDaemon(true);

        return t;
    });

    private volatile MetadataLifecycle lifecycle;

    public MetadataStateMachine() {
        this.streamControlManager = new StreamControlManager();
        this.objectControlManager = new S3ObjectControlManager(streamControlManager);
        this.kvControlManager = new KVControlManager();
    }

    public void setLifecycle(MetadataLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    public StreamControlManager streamControlManager() {
        return streamControlManager;
    }

    public S3ObjectControlManager objectControlManager() {
        return objectControlManager;
    }

    public KVControlManager kvControlManager() {
        return kvControlManager;
    }

    public boolean isLeader() {
        return leaderTerm.get() > 0;
    }

    public long appliedIndex() {
        return appliedIndex.get();
    }

    public long applySuccessCount() {
        return applySuccessCount.get();
    }

    public long applyFailCount() {
        return applyFailCount.get();
    }

    public <T> T read(Supplier<T> reader) {
        stateLock.readLock().lock();

        try {
            return reader.get();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    public CompletableFuture<Void> awaitApplied(long index) {
        if (appliedIndex.get() >= index) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();

        applyWaiters.computeIfAbsent(index, i -> new CopyOnWriteArrayList<>()).add(future);

        if (appliedIndex.get() >= index) {
            completeWaiters(appliedIndex.get());
        }

        return future;
    }

    private void advanceAppliedIndex(long index) {
        long previous = appliedIndex.get();

        while (index > previous && !appliedIndex.compareAndSet(previous, index)) {
            previous = appliedIndex.get();
        }

        completeWaiters(appliedIndex.get());
    }

    private void completeWaiters(long upToIndex) {
        Map<Long, CopyOnWriteArrayList<CompletableFuture<Void>>> ready =
            applyWaiters.headMap(upToIndex, true);
        for (Map.Entry<Long, CopyOnWriteArrayList<CompletableFuture<Void>>> entry : ready.entrySet()) {
            for (CompletableFuture<Void> future : entry.getValue()) {
                future.complete(null);
            }
        }

        ready.clear();
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            MetadataClosure closure = null;
            MetadataCommand command;

            try {
                if (Objects.nonNull(iter.done())) {
                    closure = (MetadataClosure) iter.done();
                    command = closure.command();
                } else {
                    ByteBuffer data = iter.getData();
                    byte[] bytes = new byte[data.remaining()];

                    data.get(bytes);
                    command = MetadataCommandCodec.decode(bytes);
                }
            } catch (Throwable t) {
                LOGGER.error("failed to decode metadata command at index {}", iter.getIndex(), t);
                iter.setErrorAndRollback(1, new Status(RaftError.ESTATEMACHINE, t.getMessage()));

                return;
            }

            Runnable notification = null;

            try {
                Object result;

                stateLock.writeLock().lock();

                try {
                    result = applyCommand(command);
                    notification = pendingNotification(command);
                } finally {
                    stateLock.writeLock().unlock();
                }

                applySuccessCount.incrementAndGet();

                if (Objects.nonNull(closure)) {
                    closure.success(result);
                }
            } catch (MetadataException e) {
                applyFailCount.incrementAndGet();

                if (Objects.nonNull(closure)) {
                    closure.failure(e);
                } else {
                    LOGGER.debug("metadata command rejected deterministically: {}", e.getMessage());
                }
            } catch (Throwable t) {
                applyFailCount.incrementAndGet();
                LOGGER.error("unexpected error applying metadata command", t);
                iter.setErrorAndRollback(1, new Status(RaftError.ESTATEMACHINE, t.getMessage()));

                return;
            }

            advanceAppliedIndex(iter.getIndex());

            if (Objects.nonNull(notification)) {
                listenerExecutor.execute(notification);
            }

            iter.next();
        }
    }

    Object applyCommand(MetadataCommand command) {
        return switch (command.type()) {
            case MetadataCommand.REGISTER_NODE -> {
                MetadataCommand.RegisterNode c = (MetadataCommand.RegisterNode) command;

                streamControlManager.registerNode(c.nodeId(), c.nodeEpoch(), c.httpAddress());
                yield null;
            }
            case MetadataCommand.CREATE_STREAM -> {
                MetadataCommand.CreateStream c = (MetadataCommand.CreateStream) command;

                yield streamControlManager.createStream(c.nodeId(), c.nodeEpoch());
            }
            case MetadataCommand.OPEN_STREAM -> {
                MetadataCommand.OpenStream c = (MetadataCommand.OpenStream) command;

                yield streamControlManager.openStream(c.nodeId(), c.nodeEpoch(), c.streamId(), c.epoch());
            }
            case MetadataCommand.TRIM_STREAM -> {
                MetadataCommand.TrimStream c = (MetadataCommand.TrimStream) command;

                streamControlManager.trimStream(c.nodeId(), c.nodeEpoch(), c.streamId(), c.epoch(), c.newStartOffset());
                yield null;
            }
            case MetadataCommand.CLOSE_STREAM -> {
                MetadataCommand.CloseStream c = (MetadataCommand.CloseStream) command;

                streamControlManager.closeStream(c.nodeId(), c.nodeEpoch(), c.streamId(), c.epoch());
                yield null;
            }
            case MetadataCommand.DELETE_STREAM -> {
                MetadataCommand.DeleteStream c = (MetadataCommand.DeleteStream) command;

                streamControlManager.deleteStream(c.nodeId(), c.nodeEpoch(), c.streamId(), c.epoch());
                objectControlManager.onStreamDeleted(c.streamId());
                yield null;
            }
            case MetadataCommand.PREPARE_OBJECT -> {
                MetadataCommand.PrepareObject c = (MetadataCommand.PrepareObject) command;

                yield objectControlManager.prepareObject(c.nodeId(), c.nodeEpoch(), c.count(), c.ttlMs(), c.nowMs());
            }
            case MetadataCommand.COMMIT_STREAM_SET_OBJECT -> {
                MetadataCommand.CommitStreamSetObject c = (MetadataCommand.CommitStreamSetObject) command;

                try {
                    objectControlManager.commitStreamSetObject(c.nodeId(), c.nodeEpoch(), c.request(), c.nowMs());
                } catch (MetadataException e) {
                    if (!e.isRedundant()) {
                        throw e;
                    }
                }

                yield new CommitStreamSetObjectResponse();
            }
            case MetadataCommand.COMPACT_STREAM_OBJECT -> {
                MetadataCommand.CompactStreamObject c = (MetadataCommand.CompactStreamObject) command;

                try {
                    objectControlManager.compactStreamObject(c.nodeId(), c.nodeEpoch(), c.request(), c.nowMs());
                } catch (MetadataException e) {
                    if (!e.isRedundant()) {
                        throw e;
                    }
                }

                yield null;
            }
            case MetadataCommand.EXPIRE_PREPARED_OBJECTS -> {
                MetadataCommand.ExpirePreparedObjects c = (MetadataCommand.ExpirePreparedObjects) command;

                yield objectControlManager.expirePreparedObjects(c.nowMs());
            }
            case MetadataCommand.CLEAN_DESTROYED_OBJECTS -> {
                MetadataCommand.CleanDestroyedObjects c = (MetadataCommand.CleanDestroyedObjects) command;

                objectControlManager.cleanDestroyedObjects(c.objectIds());
                yield null;
            }
            case MetadataCommand.PUT_KV -> {
                MetadataCommand.PutKV c = (MetadataCommand.PutKV) command;

                yield kvControlManager.put(c.key(), c.value());
            }
            case MetadataCommand.PUT_KV_IF_ABSENT -> {
                MetadataCommand.PutKVIfAbsent c = (MetadataCommand.PutKVIfAbsent) command;

                yield kvControlManager.putIfAbsent(c.key(), c.value());
            }
            case MetadataCommand.DELETE_KV -> {
                MetadataCommand.DeleteKV c = (MetadataCommand.DeleteKV) command;

                yield kvControlManager.delete(c.key());
            }
            default -> throw new IllegalArgumentException("unknown command type " + command.type());
        };
    }

    private Runnable pendingNotification(MetadataCommand command) {
        return switch (command.type()) {
            case MetadataCommand.CREATE_STREAM, MetadataCommand.OPEN_STREAM,
                 MetadataCommand.TRIM_STREAM, MetadataCommand.CLOSE_STREAM -> {
                long streamId = switch (command.type()) {
                    case MetadataCommand.CREATE_STREAM -> streamControlManager.nextAssignedStreamId() - 1;
                    case MetadataCommand.OPEN_STREAM -> ((MetadataCommand.OpenStream) command).streamId();
                    case MetadataCommand.TRIM_STREAM -> ((MetadataCommand.TrimStream) command).streamId();
                    default -> ((MetadataCommand.CloseStream) command).streamId();
                };

                yield streamControlManager.notification(streamId);
            }
            default -> null;
        };
    }

    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        try {
            byte[] bytes;

            stateLock.readLock().lock();

            try {
                bytes = MetadataSnapshotCodec.encode(streamControlManager, objectControlManager, kvControlManager);
            } finally {
                stateLock.readLock().unlock();
            }

            File file = new File(writer.getPath(), SNAPSHOT_FILE);

            Files.write(file.toPath(), bytes);

            if (!writer.addFile(SNAPSHOT_FILE)) {
                done.run(new Status(RaftError.EIO, "failed to add snapshot file"));
                return;
            }

            done.run(Status.OK());
        } catch (IOException e) {
            LOGGER.error("failed to save metadata snapshot", e);
            done.run(new Status(RaftError.EIO, e.getMessage()));
        }
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        try {
            File file = new File(reader.getPath(), SNAPSHOT_FILE);

            if (!file.exists()) {
                return false;
            }

            byte[] bytes = Files.readAllBytes(file.toPath());

            stateLock.writeLock().lock();

            try {
                MetadataSnapshotCodec.decode(bytes, streamControlManager, objectControlManager, kvControlManager);
            } finally {
                stateLock.writeLock().unlock();
            }

            RaftOutter.SnapshotMeta meta = reader.load();

            if (Objects.nonNull(meta)) {
                advanceAppliedIndex(meta.getLastIncludedIndex());
            }

            return true;
        } catch (IOException e) {
            LOGGER.error("failed to load metadata snapshot", e);
            return false;
        }
    }

    @Override
    public void onLeaderStart(long term) {
        leaderTerm.set(term);
        MetadataLifecycle current = lifecycle;

        if (Objects.nonNull(current)) {
            current.onLeaderStart();
        }
    }

    @Override
    public void onLeaderStop(Status status) {
        leaderTerm.set(-1);
        MetadataLifecycle current = lifecycle;

        if (Objects.nonNull(current)) {
            current.onLeaderStop();
        }
    }

    @Override
    public void onShutdown() {
        listenerExecutor.shutdownNow();
        super.onShutdown();
    }

    public byte[] stateDigestSource() {
        stateLock.readLock().lock();

        try {
            return MetadataSnapshotCodec.encode(streamControlManager, objectControlManager, kvControlManager);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    List<CompletableFuture<Void>> pendingWaiters() {
        return applyWaiters.values().stream().flatMap(List::stream).collect(java.util.stream.Collectors.toList());
    }
}
