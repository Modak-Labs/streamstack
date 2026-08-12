package io.streamstack.metadata.raft;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.metadata.ObjectUtils;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectStreamRange;
import io.streamstack.s3.operator.MemoryObjectStorage;
import io.streamstack.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.streamstack.metadata.raft.MetadataTestSupport.freePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataNodeIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void createOpenCommitSurvivesRestart() throws Exception {
        int port = freePort();
        File dataDir = tempDir.resolve("node1").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        long streamId;
        long objectId;
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L)) {
            node.awaitLeader(10, TimeUnit.SECONDS);
            node.awaitRegistered(10, TimeUnit.SECONDS);
            RaftStreamManager streamManager = new RaftStreamManager(node);
            RaftObjectManager objectManager = new RaftObjectManager(node);
            streamId = streamManager.createStream().get(10, TimeUnit.SECONDS);
            StreamMetadata opened = streamManager.openStream(streamId, 1).get(10, TimeUnit.SECONDS);
            assertEquals(StreamState.OPENED, opened.state());
            objectId = objectManager.prepareObject(1, 60_000).get(10, TimeUnit.SECONDS);
            CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
            request.setObjectId(objectId);
            request.setObjectSize(32);
            request.setOrderId(objectId);
            request.setStreamRanges(List.of(new ObjectStreamRange(streamId, 1, 0, 8, 32)));
            objectManager.commitStreamSetObject(request).get(10, TimeUnit.SECONDS);
            assertEquals(8, streamManager.getStreams(List.of(streamId)).get(10, TimeUnit.SECONDS).get(0).endOffset());
            assertEquals(1, objectManager.getObjectsCount().get(10, TimeUnit.SECONDS));
            node.triggerSnapshot();
        }
        try (MetadataNode restarted = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 2L)) {
            restarted.awaitLeader(10, TimeUnit.SECONDS);
            restarted.awaitRegistered(10, TimeUnit.SECONDS);
            RaftStreamManager streamManager = new RaftStreamManager(restarted);
            RaftObjectManager objectManager = new RaftObjectManager(restarted);
            StreamMetadata recovered = streamManager.getStreams(List.of(streamId)).get(10, TimeUnit.SECONDS).get(0);
            assertEquals(8, recovered.endOffset());
            assertEquals(StreamState.OPENED, recovered.state());
            assertTrue(objectManager.isObjectExist(objectId));
            assertEquals(1, objectManager.getObjectsCount().get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void staleNodeEpochIsFencedAfterRestartWithHigherEpoch() throws Exception {
        int port = freePort();
        File dataDir = tempDir.resolve("fencing").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 5L)) {
            node.awaitLeader(10, TimeUnit.SECONDS);
            node.awaitRegistered(10, TimeUnit.SECONDS);
            Exception e = assertThrows(Exception.class, () ->
                node.client().propose(new MetadataCommand.CreateStream(node.nodeId(), 4L))
                    .get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().contains("epoch"));
            long streamId = (Long) node.client()
                .propose(new MetadataCommand.CreateStream(node.nodeId(), 5L)).get(10, TimeUnit.SECONDS);
            assertTrue(streamId >= 0);
        }
    }

    @Test
    void metadataClientRetriesUntilLeader() throws Exception {
        int port = freePort();
        File dataDir = tempDir.resolve("retry").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L)) {
            node.awaitRegistered(15, TimeUnit.SECONDS);
            Long streamId = (Long) node.client()
                .propose(new MetadataCommand.CreateStream(node.nodeId(), node.nodeEpoch()))
                .get(15, TimeUnit.SECONDS);
            assertTrue(streamId >= 0);
            assertTrue(node.health().applySuccessCount() >= 1);
        }
    }

    @Test
    void objectCleanerDrainsThroughRaftLog() throws Exception {
        ObjectUtils.setNamespace("DEFAULT");
        int port = freePort();
        File dataDir = tempDir.resolve("cleaner").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        MemoryObjectStorage storage = new MemoryObjectStorage();
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L, storage)) {
            node.awaitLeader(10, TimeUnit.SECONDS);
            node.awaitRegistered(10, TimeUnit.SECONDS);
            RaftObjectManager objectManager = new RaftObjectManager(node);
            long firstObjectId = objectManager.prepareObject(2, 60_000).get(10, TimeUnit.SECONDS);
            String key = ObjectUtils.genKey(0, firstObjectId);
            storage.write(ObjectStorage.WriteOptions.DEFAULT, key,
                    io.netty.buffer.Unpooled.wrappedBuffer(new byte[] {1, 2, 3}))
                .get(5, TimeUnit.SECONDS);
            assertTrue(storage.contains(key));
            markObjectDestroyedThroughLog(node, firstObjectId);
            awaitDestroyedBacklogEmpty(node);
            assertFalse(storage.contains(key));
        } finally {
            storage.close();
        }
    }

    @Test
    void objectCleanerRetainsMarksWhenStorageDeleteFails() throws Exception {
        ObjectUtils.setNamespace("DEFAULT");
        int port = freePort();
        File dataDir = tempDir.resolve("cleaner-fail").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        MemoryObjectStorage storage = new MemoryObjectStorage() {
            @Override
            public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
                return CompletableFuture.failedFuture(new IllegalStateException("simulated storage outage"));
            }
        };
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L, storage)) {
            node.awaitLeader(10, TimeUnit.SECONDS);
            node.awaitRegistered(10, TimeUnit.SECONDS);
            markObjectDestroyedThroughLog(node);
            assertThrows(Exception.class, () -> node.lifecycle().objectCleaner().clean(10));
            assertEquals(1, node.health().destroyedBacklog());
        } finally {
            storage.close();
        }
    }

    private static long markObjectDestroyedThroughLog(MetadataNode node) throws Exception {
        RaftObjectManager objectManager = new RaftObjectManager(node);
        long firstObjectId = objectManager.prepareObject(2, 60_000).get(10, TimeUnit.SECONDS);
        markObjectDestroyedThroughLog(node, firstObjectId);
        return firstObjectId;
    }

    private static void markObjectDestroyedThroughLog(MetadataNode node, long firstObjectId) throws Exception {
        RaftStreamManager streamManager = new RaftStreamManager(node);
        RaftObjectManager objectManager = new RaftObjectManager(node);
        long streamId = streamManager.createStream().get(10, TimeUnit.SECONDS);
        streamManager.openStream(streamId, 1).get(10, TimeUnit.SECONDS);
        long replacementObjectId = firstObjectId + 1;
        objectManager.compactStreamObject(new CompactStreamObjectRequest(
            firstObjectId, 3, streamId, 1, 0, 0, List.of(), List.of(), 0)).get(10, TimeUnit.SECONDS);
        objectManager.compactStreamObject(new CompactStreamObjectRequest(
            replacementObjectId, 3, streamId, 1, 0, 0,
            List.of(firstObjectId), List.of(CompactOperations.DELETE), 0)).get(10, TimeUnit.SECONDS);
    }

    private static void awaitDestroyedBacklogEmpty(MetadataNode node) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (node.health().destroyedBacklog() == 0) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("destroyed backlog never drained");
    }
}
