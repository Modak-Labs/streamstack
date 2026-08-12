package io.streamstack.metadata.raft;

import io.streamstack.s3.Config;
import io.streamstack.s3.DefaultByteBufSupplier;
import io.streamstack.s3.S3Storage;
import io.streamstack.s3.cache.ReadDataBlock;
import io.streamstack.s3.cache.blockcache.DefaultObjectReaderFactory;
import io.streamstack.s3.cache.blockcache.StreamReaders;
import io.streamstack.s3.failover.StorageFailureHandler;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.s3.model.StreamRecordBatch;
import io.streamstack.s3.operator.BucketURI;
import io.streamstack.s3.operator.LocalFileObjectStorage;
import io.streamstack.s3.operator.ObjectStorage;
import io.streamstack.s3.wal.impl.MemoryWriteAheadLog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3StorageRaftIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void appendForceUploadReadAndRecover() throws Exception {
        int port = freePort();
        File dataDir = tempDir.resolve("meta").toFile();
        File objectDir = tempDir.resolve("objects").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        ObjectStorage objectStorage = new LocalFileObjectStorage(
            BucketURI.parse("-2@file://" + objectDir.getAbsolutePath()));
        long streamId;
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L, objectStorage)) {
            node.awaitLeader(15, TimeUnit.SECONDS);
            awaitRegistered(node);
            RaftStreamManager streamManager = new RaftStreamManager(node);
            RaftObjectManager objectManager = new RaftObjectManager(node);
            streamId = streamManager.createStream().get(15, TimeUnit.SECONDS);
            streamManager.openStream(streamId, 1).get(15, TimeUnit.SECONDS);
            Config config = new Config();
            config.blockCacheSize(0);
            MemoryWriteAheadLog wal = new MemoryWriteAheadLog();
            S3Storage storage = new S3Storage(
                config,
                wal,
                streamManager,
                objectManager,
                new StreamReaders(config.blockCacheSize(), objectManager, objectStorage,
                    new DefaultObjectReaderFactory(objectStorage)),
                objectStorage,
                (StorageFailureHandler) ex -> {
                    throw new RuntimeException(ex);
                });
            storage.startup();
            try {
                StreamMetadata reopened = streamManager.openStream(streamId, 2).get(15, TimeUnit.SECONDS);
                assertEquals(StreamState.OPENED, reopened.state());
                storage.append(record(streamId, 0)).get(15, TimeUnit.SECONDS);
                storage.append(record(streamId, 1)).get(15, TimeUnit.SECONDS);
                storage.append(record(streamId, 2)).get(15, TimeUnit.SECONDS);
                storage.forceUpload(streamId).get(30, TimeUnit.SECONDS);
                ReadDataBlock read = storage.read(streamId, 0, 3, 1024).get(15, TimeUnit.SECONDS);
                assertEquals(3, read.getRecords().size());
                assertEquals(3, streamManager.getStreams(List.of(streamId)).get(15, TimeUnit.SECONDS).get(0).endOffset());
                assertTrue(objectManager.getObjectsCount().get(15, TimeUnit.SECONDS) >= 1);
            } finally {
                storage.shutdown();
            }
            node.triggerSnapshot();
        }
        ObjectStorage restartedStorage = new LocalFileObjectStorage(
            BucketURI.parse("-2@file://" + objectDir.getAbsolutePath()));
        try (MetadataNode restarted = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 2L, restartedStorage)) {
            restarted.awaitLeader(15, TimeUnit.SECONDS);
            awaitRegistered(restarted);
            RaftStreamManager streamManager = new RaftStreamManager(restarted);
            RaftObjectManager objectManager = new RaftObjectManager(restarted);
            StreamMetadata recovered = streamManager.getStreams(List.of(streamId)).get(15, TimeUnit.SECONDS).get(0);
            assertEquals(streamId, recovered.streamId());
            assertEquals(3, recovered.endOffset());
            Config config = new Config();
            config.blockCacheSize(0);
            MemoryWriteAheadLog wal = new MemoryWriteAheadLog();
            S3Storage storage = new S3Storage(
                config,
                wal,
                streamManager,
                objectManager,
                new StreamReaders(config.blockCacheSize(), objectManager, restartedStorage,
                    new DefaultObjectReaderFactory(restartedStorage)),
                restartedStorage,
                (StorageFailureHandler) ex -> {
                    throw new RuntimeException(ex);
                });
            storage.startup();
            try {
                streamManager.openStream(streamId, recovered.epoch() + 1).get(15, TimeUnit.SECONDS);
                ReadDataBlock read = storage.read(streamId, 0, 3, 1024).get(15, TimeUnit.SECONDS);
                assertEquals(3, read.getRecords().size());
                assertEquals(1, streamManager.getOpeningStreams().get(15, TimeUnit.SECONDS).size());
            } finally {
                storage.shutdown();
                restartedStorage.close();
            }
        } finally {
            objectStorage.close();
        }
    }

    private static StreamRecordBatch record(long streamId, long offset) {
        byte[] payload = new byte[16];
        ThreadLocalRandom.current().nextBytes(payload);
        return StreamRecordBatch.of(
            streamId, 1, offset, 1, ByteBuffer.wrap(payload), DefaultByteBufSupplier.INSTANCE);
    }

    private static void awaitRegistered(MetadataNode node) throws InterruptedException {
        node.awaitRegistered(15, TimeUnit.SECONDS);
    }

    private static int freePort() throws Exception {
        return MetadataTestSupport.freePort();
    }
}
