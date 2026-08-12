package io.streamstack.metadata.raft;

import java.util.Objects;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.operator.ObjectStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetadataLifecycle implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataLifecycle.class);

    private final MetadataClient client;
    private final ObjectCleaner objectCleaner;

    private final AtomicBoolean leader = new AtomicBoolean(false);

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> expireFuture;
    private ScheduledFuture<?> cleanFuture;

    public MetadataLifecycle(MetadataClient client, ObjectStorage objectStorage) {
        this.client = client;
        this.objectCleaner = new ObjectCleaner(client, objectStorage);
    }

    public ObjectCleaner objectCleaner() {
        return objectCleaner;
    }

    public synchronized void onLeaderStart() {
        if (!leader.compareAndSet(false, true)) {
            return;
        }

        scheduler = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r, "metadata-lifecycle-" + client.node().nodeId());

            t.setDaemon(true);

            return t;
        });

        expireFuture = scheduler.scheduleWithFixedDelay(this::expirePrepared, 1, 1, TimeUnit.SECONDS);
        cleanFuture = scheduler.scheduleWithFixedDelay(this::cleanObjects, 1, 1, TimeUnit.SECONDS);
    }

    public synchronized void onLeaderStop() {
        if (!leader.compareAndSet(true, false)) {
            return;
        }

        cancel(expireFuture);
        cancel(cleanFuture);
        expireFuture = null;
        cleanFuture = null;

        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private void expirePrepared() {
        if (!leader.get() || !client.node().isLeader()) {
            return;
        }

        client.propose(new MetadataCommand.ExpirePreparedObjects(System.currentTimeMillis()))
            .whenComplete((r, e) -> {
                if (Objects.nonNull(e)) {
                    LOGGER.debug("expire prepared objects failed", e);
                }
            });
    }

    private void cleanObjects() {
        if (!leader.get() || !client.node().isLeader()) {
            return;
        }

        try {
            objectCleaner.clean(ObjectCleaner.MAX_DELETE_BATCH_COUNT);
        } catch (Throwable t) {
            LOGGER.warn("object cleaner failed, destroyed marks retained", t);
        }
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (Objects.nonNull(future)) {
            future.cancel(false);
        }
    }

    @Override
    public void close() {
        onLeaderStop();
    }
}
