package io.streamstack.metadata.raft;

import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.streamstack.metadata.raft.MetadataTestSupport.assertReplicasConverged;
import static io.streamstack.metadata.raft.MetadataTestSupport.awaitLeader;
import static io.streamstack.metadata.raft.MetadataTestSupport.freePort;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataClusterIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void followerProposalsAreForwardedToLeader() throws Exception {
        int port1 = freePort();
        int port2 = freePort();
        List<String> peers = List.of(
            MetadataNode.peerString("127.0.0.1", port1),
            MetadataNode.peerString("127.0.0.1", port2));
        try (MetadataNode node1 = new MetadataNode(1, "127.0.0.1", port1, tempDir.resolve("r1").toFile(), peers, 1L);
             MetadataNode node2 = new MetadataNode(2, "127.0.0.1", port2, tempDir.resolve("r2").toFile(), peers, 1L)) {
            awaitLeader(30, TimeUnit.SECONDS, node1, node2);
            MetadataNode leader = node1.isLeader() ? node1 : node2;
            MetadataNode follower = leader == node1 ? node2 : node1;

            assertTrue(leader.isLeader());
            assertFalse(follower.isLeader());
            leader.awaitRegistered(20, TimeUnit.SECONDS);
            follower.awaitRegistered(20, TimeUnit.SECONDS);
            RaftStreamManager followerStreamManager = new RaftStreamManager(follower);
            long streamId = followerStreamManager.createStream().get(20, TimeUnit.SECONDS);
            StreamMetadata opened = followerStreamManager.openStream(streamId, 1).get(20, TimeUnit.SECONDS);

            assertEquals(StreamState.OPENED, opened.state());
            assertEquals(follower.nodeId(), opened.nodeId());
            StreamMetadata fromLeader = new RaftStreamManager(leader)
                .getStreams(List.of(streamId)).get(20, TimeUnit.SECONDS).get(0);
            assertEquals(StreamState.OPENED, fromLeader.state());
            assertEquals(1, followerStreamManager.getOpeningStreams().get(20, TimeUnit.SECONDS).size());
            assertEquals(0, new RaftStreamManager(leader).getOpeningStreams().get(20, TimeUnit.SECONDS).size());
            assertNotNull(follower.leaderId());
            assertTrue(leader.health().isLeader());
            assertTrue(leader.health().streamCount() >= 1);
            assertReplicasConverged(20, TimeUnit.SECONDS, node1, node2);
        }
    }

    @Test
    void rejectedCommandDoesNotDivergeReplicas() throws Exception {
        int port1 = freePort();
        int port2 = freePort();
        List<String> peers = List.of(
            MetadataNode.peerString("127.0.0.1", port1),
            MetadataNode.peerString("127.0.0.1", port2));
        try (MetadataNode node1 = new MetadataNode(1, "127.0.0.1", port1, tempDir.resolve("d1").toFile(), peers, 1L);
             MetadataNode node2 = new MetadataNode(2, "127.0.0.1", port2, tempDir.resolve("d2").toFile(), peers, 1L)) {
            awaitLeader(30, TimeUnit.SECONDS, node1, node2);
            MetadataNode leader = node1.isLeader() ? node1 : node2;

            leader.awaitRegistered(20, TimeUnit.SECONDS);
            RaftStreamManager streamManager = new RaftStreamManager(leader);
            long streamId = streamManager.createStream().get(20, TimeUnit.SECONDS);

            streamManager.openStream(streamId, 5).get(20, TimeUnit.SECONDS);

            try {
                streamManager.openStream(streamId, 3).get(20, TimeUnit.SECONDS);
                throw new AssertionError("expected fenced open to fail");
            } catch (ExecutionException expected) {
                assertNotNull(expected.getCause());
            }

            long nextStreamId = streamManager.createStream().get(20, TimeUnit.SECONDS);

            assertTrue(nextStreamId > streamId);
            assertReplicasConverged(20, TimeUnit.SECONDS, node1, node2);
        }
    }
}
