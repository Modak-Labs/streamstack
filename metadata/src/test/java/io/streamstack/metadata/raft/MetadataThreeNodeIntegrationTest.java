package io.streamstack.metadata.raft;

import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.streamstack.metadata.raft.MetadataTestSupport.assertReplicasConverged;
import static io.streamstack.metadata.raft.MetadataTestSupport.awaitLeader;
import static io.streamstack.metadata.raft.MetadataTestSupport.freePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataThreeNodeIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void threeNodeLeaderKillPreservesCatalog() throws Exception {
        int port1 = freePort();
        int port2 = freePort();
        int port3 = freePort();
        List<String> peers = List.of(
            MetadataNode.peerString("127.0.0.1", port1),
            MetadataNode.peerString("127.0.0.1", port2),
            MetadataNode.peerString("127.0.0.1", port3));

        MetadataNode node1 = new MetadataNode(1, "127.0.0.1", port1, tempDir.resolve("n1").toFile(), peers, 1L);
        MetadataNode node2 = new MetadataNode(2, "127.0.0.1", port2, tempDir.resolve("n2").toFile(), peers, 1L);
        MetadataNode node3 = new MetadataNode(3, "127.0.0.1", port3, tempDir.resolve("n3").toFile(), peers, 1L);
        try {
            MetadataNode leader = awaitLeader(40, TimeUnit.SECONDS, node1, node2, node3);
            // Wait for every peer's RegisterNode to commit before mutating catalog state.
            // Otherwise in-flight follower registrations race snapshot/digest checks.
            node1.awaitRegistered(20, TimeUnit.SECONDS);
            node2.awaitRegistered(20, TimeUnit.SECONDS);
            node3.awaitRegistered(20, TimeUnit.SECONDS);
            assertReplicasConverged(30, TimeUnit.SECONDS, node1, node2, node3);

            RaftStreamManager streamManager = new RaftStreamManager(leader);
            long streamId = streamManager.createStream().get(20, TimeUnit.SECONDS);
            StreamMetadata opened = streamManager.openStream(streamId, 1).get(20, TimeUnit.SECONDS);
            assertEquals(StreamState.OPENED, opened.state());
            leader.triggerSnapshot();
            assertReplicasConverged(30, TimeUnit.SECONDS, node1, node2, node3);

            int oldLeaderNodeId = leader.nodeId();
            leader.close();

            MetadataNode[] survivors = survivors(leader, node1, node2, node3);
            MetadataNode newLeader = awaitLeader(40, TimeUnit.SECONDS, survivors);
            assertTrue(newLeader.nodeId() != oldLeaderNodeId);

            StreamMetadata recovered = new RaftStreamManager(newLeader)
                .getStreams(List.of(streamId)).get(30, TimeUnit.SECONDS).get(0);
            assertEquals(StreamState.OPENED, recovered.state());
            assertEquals(streamId, recovered.streamId());
            assertTrue(newLeader.health().streamCount() >= 1);
            assertReplicasConverged(30, TimeUnit.SECONDS, survivors);
        } finally {
            closeQuietly(node1);
            closeQuietly(node2);
            closeQuietly(node3);
        }
    }

    @Test
    void laggingFollowerInstallsSnapshot() throws Exception {
        int port1 = freePort();
        int port2 = freePort();
        int port3 = freePort();
        List<String> peers = List.of(
            MetadataNode.peerString("127.0.0.1", port1),
            MetadataNode.peerString("127.0.0.1", port2),
            MetadataNode.peerString("127.0.0.1", port3));

        Path dir1 = tempDir.resolve("s1");
        Path dir2 = tempDir.resolve("s2");
        Path dir3 = tempDir.resolve("s3");

        MetadataNode node1 = new MetadataNode(1, "127.0.0.1", port1, dir1.toFile(), peers, 1L);
        MetadataNode node2 = new MetadataNode(2, "127.0.0.1", port2, dir2.toFile(), peers, 1L);
        MetadataNode node3 = new MetadataNode(3, "127.0.0.1", port3, dir3.toFile(), peers, 1L);
        long streamId;
        try {
            awaitLeader(40, TimeUnit.SECONDS, node1, node2, node3);
            node1.awaitRegistered(20, TimeUnit.SECONDS);
            node2.awaitRegistered(20, TimeUnit.SECONDS);
            node3.awaitRegistered(20, TimeUnit.SECONDS);

            node3.close();

            MetadataNode leader = awaitLeader(40, TimeUnit.SECONDS, node1, node2);
            node1.awaitRegistered(20, TimeUnit.SECONDS);
            node2.awaitRegistered(20, TimeUnit.SECONDS);

            RaftStreamManager streamManager = new RaftStreamManager(leader);
            streamId = streamManager.createStream().get(20, TimeUnit.SECONDS);
            streamManager.openStream(streamId, 1).get(20, TimeUnit.SECONDS);
            for (int i = 0; i < 8; i++) {
                streamManager.createStream().get(20, TimeUnit.SECONDS);
            }
            leader.triggerSnapshot();
        } finally {
            closeQuietly(node3);
        }

        try (MetadataNode restarted = new MetadataNode(3, "127.0.0.1", port3, dir3.toFile(), peers, 2L)) {
            awaitLeader(40, TimeUnit.SECONDS, node1, node2, restarted);
            awaitCatalog(restarted, streamId, 40, TimeUnit.SECONDS);
            StreamMetadata recovered = restarted.stateMachine().read(() ->
                restarted.stateMachine().streamControlManager().getStream(streamId));
            assertEquals(StreamState.OPENED, recovered.state());
            assertTrue(restarted.stateMachine().read(() ->
                restarted.stateMachine().streamControlManager().streamsMetadata().size()) >= 9);
            assertReplicasConverged(40, TimeUnit.SECONDS, node1, node2, restarted);
        } finally {
            closeQuietly(node1);
            closeQuietly(node2);
        }
    }

    private static MetadataNode[] survivors(MetadataNode closed, MetadataNode... nodes) {
        return java.util.Arrays.stream(nodes).filter(n -> n != closed).toArray(MetadataNode[]::new);
    }

    private static void awaitCatalog(MetadataNode node, long streamId, long timeout, TimeUnit unit)
        throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (node.stateMachine().read(() ->
                node.stateMachine().streamControlManager().getStream(streamId)) != null) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("timed out waiting for stream " + streamId + " on lagging follower");
    }

    private static void closeQuietly(MetadataNode node) {
        try {
            node.close();
        } catch (Throwable ignored) {
        }
    }
}
