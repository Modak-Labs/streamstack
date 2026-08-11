package io.streamstack.server;

import io.javalin.Javalin;
import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.metadata.raft.RaftKVClient;
import io.streamstack.metadata.raft.RaftObjectManager;
import io.streamstack.metadata.raft.RaftStreamManager;
import io.streamstack.s3.Config;
import io.streamstack.s3.S3Storage;
import io.streamstack.s3.S3StreamClient;
import io.streamstack.s3.cache.blockcache.DefaultObjectReaderFactory;
import io.streamstack.s3.cache.blockcache.StreamReaders;
import io.streamstack.s3.failover.StorageFailureHandler;
import io.streamstack.s3.operator.BucketURI;
import io.streamstack.s3.operator.LocalFileObjectStorage;
import io.streamstack.s3.operator.ObjectStorage;
import io.streamstack.s3.wal.impl.MemoryWriteAheadLog;
import io.streamstack.server.http.DurableStreamsHandler;
import io.streamstack.server.http.OwnershipRouter;
import io.streamstack.server.store.S3StreamStore;
import io.streamstack.server.store.StreamWaiterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class StreamStackServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamStackServer.class);

    private final ServerConfig config;
    private final MetadataNode metadataNode;
    private final ObjectStorage objectStorage;
    private final S3Storage storage;
    private final S3StreamClient streamClient;
    private final S3StreamStore store;
    private final Javalin app;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public StreamStackServer(ServerConfig config) throws Exception {
        this.config = Objects.requireNonNull(config, "config");
        Files.createDirectories(config.dataDir().toPath());
        Files.createDirectories(config.objectDir().toPath());

        this.objectStorage = new LocalFileObjectStorage(
            BucketURI.parse("-2@file://" + config.objectDir().getAbsolutePath()));
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
        streamConfig.blockCacheSize(0);

        MemoryWriteAheadLog wal = new MemoryWriteAheadLog();
        this.storage = new S3Storage(
            streamConfig,
            wal,
            streamManager,
            objectManager,
            new StreamReaders(streamConfig.blockCacheSize(), objectManager, objectStorage,
                new DefaultObjectReaderFactory(objectStorage)),
            objectStorage,
            (StorageFailureHandler) ex -> {
                throw new RuntimeException(ex);
            });
        this.streamClient = new S3StreamClient(streamManager, storage, objectManager, objectStorage, streamConfig);
        RaftKVClient kvClient = new RaftKVClient(metadataNode);
        this.store = new S3StreamStore(streamClient, kvClient, metadataNode, new StreamWaiterRegistry());
        DurableStreamsHandler handler = new DurableStreamsHandler(
            store,
            Duration.ofSeconds(config.longPollTimeoutSec()),
            Duration.ofSeconds(config.sseMaxDurationSec()),
            config.maxChunkSize());
        OwnershipRouter router = new OwnershipRouter(metadataNode, store, handler, config.routingMode());

        this.app = Javalin.create(cfg -> cfg.showJavalinBanner = false);
        app.get("/*", router::handle);
        app.post("/*", router::handle);
        app.put("/*", router::handle);
        app.delete("/*", router::handle);
        app.head("/*", router::handle);
    }

    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        metadataNode.awaitLeader(30, TimeUnit.SECONDS);
        metadataNode.awaitRegistered(30, TimeUnit.SECONDS);
        storage.startup();
        app.start(config.httpHost(), config.httpPort());
        LOGGER.info("streamstack server started nodeId={} http={}:{} raft={}:{}",
            config.nodeId(), config.httpHost(), config.httpPort(), config.raftHost(), config.raftPort());
    }

    public String baseUrl() {
        return config.httpAddress();
    }

    public MetadataNode metadataNode() {
        return metadataNode;
    }

    public S3StreamStore store() {
        return store;
    }

    @Override
    public void close() {
        try {
            app.stop();
        } catch (Exception ignored) {
        }
        try {
            for (var stream : new ArrayList<>(store.openStreamSnapshot())) {
                try {
                    stream.close().get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                }
            }
            store.shutdown();
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
        metadataNode.close();
        started.set(false);
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        StreamStackServer server = new StreamStackServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
    }
}
