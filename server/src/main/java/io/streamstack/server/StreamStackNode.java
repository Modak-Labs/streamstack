package io.streamstack.server;

import io.streamstack.Version;
import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.metadata.raft.RaftKVClient;
import io.streamstack.metadata.raft.RaftObjectManager;
import io.streamstack.metadata.raft.RaftStreamManager;
import io.streamstack.s3.ByteBufAlloc;
import io.streamstack.s3.Config;
import io.streamstack.s3.ConfigValidator;
import io.streamstack.s3.S3Storage;
import io.streamstack.s3.S3StreamClient;
import io.streamstack.s3.cache.blockcache.DefaultObjectReaderFactory;
import io.streamstack.s3.cache.blockcache.StreamReaders;
import io.streamstack.s3.compact.CompactionManager;
import io.streamstack.s3.failover.StorageFailureHandler;
import io.streamstack.s3.operator.BucketURI;
import io.streamstack.s3.operator.ObjectStorage;
import io.streamstack.s3.operator.ObjectStorageFactory;
import io.streamstack.s3.wal.WriteAheadLog;
import io.streamstack.s3.wal.impl.MemoryWriteAheadLog;
import io.streamstack.s3.wal.impl.object.ObjectWALConfig;
import io.streamstack.s3.wal.impl.object.ObjectWALService;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.config.ServerConfig;
import io.streamstack.server.service.RaftOwnershipService;
import io.streamstack.server.service.S3StreamService;
import io.streamstack.server.StreamWaiterRegistry;
import io.streamstack.utils.IdURI;
import io.streamstack.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class StreamStackNode implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStackNode.class);

    private static final int STORAGE_READINESS_MAX_ATTEMPTS = 10;
    private static final long STORAGE_READINESS_INITIAL_BACKOFF_MS = 1_000;
    private static final long STORAGE_READINESS_MAX_BACKOFF_MS = 30_000;

    private final ServerConfig config;
    private final MetadataNode metadataNode;
    private final ObjectStorage objectStorage;
    private final ObjectStorage walObjectStorage;
    private final S3Storage storage;
    private final S3StreamClient streamClient;
    private final CompactionManager compactionManager;
    private final S3StreamService streamService;
    private final RaftKVClient kvClient;
    private final StreamService service;
    private final AdminServer adminServer;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public StreamStackNode(ServerConfig config) throws Exception {
        this.config = Objects.requireNonNull(config, "config");
        Files.createDirectories(config.dataDir().toPath());
        File objectDir = config.objectDir();

        if (Objects.nonNull(objectDir)) {
            Files.createDirectories(objectDir.toPath());
        }

        config.streamConfig().allocPolicy().ifPresent(ByteBufAlloc::setPolicy);
        BucketURI storageBucket = BucketURI.parse(config.storageUri());

        this.objectStorage = ObjectStorageFactory.instance()
            .builder(storageBucket)
            .threadPrefix("data")
            .build();
        this.metadataNode = new MetadataNode(
            config.nodeId(),
            config.raftHost(),
            config.raftPort(),
            new File(config.dataDir(), "meta-" + config.nodeId()),
            config.raftPeers(),
            config.nodeEpoch(),
            objectStorage,
            MetadataNode.Options.defaults(),
            config.httpAddress());
        RaftStreamManager streamManager = new RaftStreamManager(metadataNode);
        RaftObjectManager objectManager = new RaftObjectManager(metadataNode);
        Config streamConfig = new Config();

        streamConfig.nodeId(config.nodeId());
        streamConfig.nodeEpoch(config.nodeEpoch());
        streamConfig.version(() -> Version.LATEST);
        config.applyTo(streamConfig);

        try {
            ConfigValidator.validate(streamConfig);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid stream config: " + e.getMessage(), e);
        }

        String walUri = config.resolveWalUri();
        WalBundle walBundle = createWal(walUri, storageBucket, objectStorage);

        this.walObjectStorage = walBundle.objectStorage();
        this.storage = new S3Storage(
            streamConfig,
            walBundle.wal(),
            streamManager,
            objectManager,
            new StreamReaders(streamConfig.blockCacheSize(), objectManager, objectStorage,
                new DefaultObjectReaderFactory(objectStorage)),
            objectStorage,
            (StorageFailureHandler) ex -> {
                throw new RuntimeException(ex);
            });
        this.streamClient = new S3StreamClient(streamManager, storage, objectManager, objectStorage, streamConfig);
        this.compactionManager = new CompactionManager(streamConfig, objectManager, streamManager, objectStorage);
        this.kvClient = new RaftKVClient(metadataNode);
        this.streamService = new S3StreamService(streamClient, kvClient, metadataNode, new StreamWaiterRegistry());
        RaftOwnershipService ownership = new RaftOwnershipService(metadataNode, streamService);

        this.service = new StreamService(streamService, streamService, streamService, ownership);
        this.adminServer = config.adminEnabled() ? new AdminServer(this) : null;
    }

    private WalBundle createWal(String walUri, BucketURI dataBucket, ObjectStorage dataStorage) {
        if (isMemoryWal(walUri)) {
            return new WalBundle(new MemoryWriteAheadLog(), null);
        }

        BucketURI walBucket = BucketURI.parse(walUri);
        ObjectStorage walStorage;
        boolean shared = sameBucket(dataBucket, walBucket);

        if (shared) {
            walStorage = dataStorage;
        } else {
            walStorage = ObjectStorageFactory.instance()
                .builder(walBucket)
                .threadPrefix("wal")
                .build();
        }

        ObjectWALConfig walConfig = ObjectWALConfig.builder()
            .withURI(IdURI.parse(walUri))
            .withClusterId(config.clusterId())
            .withNodeId(config.nodeId())
            .withEpoch(config.nodeEpoch())
            .build();
        return new WalBundle(new ObjectWALService(Time.SYSTEM, walStorage, walConfig), shared ? null : walStorage);
    }

    private static boolean isMemoryWal(String walUri) {
        if (Objects.isNull(walUri) || walUri.isBlank()) {
            return true;
        }

        String normalized = walUri.trim().toLowerCase();

        return "memory".equals(normalized)
            || "mem".equals(normalized)
            || normalized.startsWith("mem://")
            || normalized.matches("^-?\\d+@mem://.*");
    }

    private static boolean sameBucket(BucketURI a, BucketURI b) {
        return a.protocol().equalsIgnoreCase(b.protocol())
            && Objects.equals(a.bucket(), b.bucket())
            && Objects.equals(a.endpoint(), b.endpoint())
            && Objects.equals(a.region(), b.region())
            && a.bucketId() == b.bucketId();
    }

    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        if (Objects.nonNull(adminServer)) {
            adminServer.start();
        }

        awaitObjectStorageReady();
        metadataNode.awaitLeader(30, TimeUnit.SECONDS);
        metadataNode.awaitRegistered(30, TimeUnit.SECONDS);
        storage.startup();
        compactionManager.start();
        ready.set(true);
        LOGGER.info(
            "streamstack node started nodeId={} advertised={} raft={}:{} storage={} wal={}",
            config.nodeId(), config.httpAddress(), config.raftHost(), config.raftPort(),
            config.storageUri(), config.resolveWalUri());
    }

    private void awaitObjectStorageReady() throws InterruptedException {
        long backoffMs = STORAGE_READINESS_INITIAL_BACKOFF_MS;

        for (int attempt = 1; ; attempt++) {
            boolean dataReady = storageReady(objectStorage);
            boolean walReady = Objects.isNull(walObjectStorage) || storageReady(walObjectStorage);

            if (dataReady && walReady) {
                return;
            }

            if (attempt >= STORAGE_READINESS_MAX_ATTEMPTS) {
                throw new IllegalStateException(String.format(
                    "object storage not ready after %d attempts (data=%b wal=%b), storage=%s wal=%s",
                    attempt, dataReady, walReady, config.storageUri(), config.resolveWalUri()));
            }

            LOGGER.warn(
                "object storage not ready (attempt {}/{}, data={} wal={}), retrying in {}ms",
                attempt, STORAGE_READINESS_MAX_ATTEMPTS, dataReady, walReady, backoffMs);
            Thread.sleep(backoffMs);
            backoffMs = Math.min(backoffMs * 2, STORAGE_READINESS_MAX_BACKOFF_MS);
        }
    }

    private static boolean storageReady(ObjectStorage storage) {
        try {
            return storage.readinessCheck();
        } catch (Exception e) {
            LOGGER.warn("object storage readiness check failed: {}", e.getMessage());
            return false;
        }
    }

    public StreamService service() {
        return service;
    }

    public ServerConfig config() {
        return config;
    }

    public String advertisedAddress() {
        return config.httpAddress();
    }

    public MetadataNode metadataNode() {
        return metadataNode;
    }

    public RaftKVClient kvClient() {
        return kvClient;
    }

    public S3StreamService streamService() {
        return streamService;
    }

    public boolean isReady() {
        return ready.get();
    }

    public void markNotReady() {
        ready.set(false);
    }

    public void drainBeforeShutdown() {
        ready.set(false);
        int drainSec = config.shutdownDrainSec();

        if (drainSec <= 0) {
            return;
        }

        LOGGER.info("draining for {}s before shutdown nodeId={}", drainSec, config.nodeId());

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(drainSec));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        ready.set(false);

        try {
            for (var stream : new ArrayList<>(streamService.openStreamSnapshot())) {
                try {
                    stream.close().get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
            }

            streamService.shutdown();
        } catch (Exception ignored) {
        }

        try {
            compactionManager.shutdown();
        } catch (Exception ignored) {
        }

        try {
            streamClient.shutdown();
        } catch (Exception ignored) {
        }

        try {
            storage.shutdown();
        } catch (Exception ignored) {
        }

        if (Objects.nonNull(walObjectStorage)) {
            try {
                walObjectStorage.close();
            } catch (Exception ignored) {
            }
        }

        try {
            objectStorage.close();
        } catch (Exception ignored) {
        }

        metadataNode.close();

        if (Objects.nonNull(adminServer)) {
            adminServer.close();
        }

        started.set(false);
    }

    private record WalBundle(WriteAheadLog wal, ObjectStorage objectStorage) {
    }
}
