package io.streamstack.server;

import io.streamstack.metadata.raft.SnapshotArchive;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;
import io.streamstack.server.service.StreamService;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataArchiveRestoreIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void archivesSnapshotsAndRestoresMetadataFromStorage() throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path objectDir = tempDir.resolve("objects");

        try (StreamStackNode node = new StreamStackNode(config(dataDir, objectDir, 1, false, freePort()))) {
            node.start();
            StreamService services = node.service();

            assertTrue(services.lifecycle().create(new CreateCommand(
                "/streams/durable", "text/plain", null, null, false, new byte[0])).created());
            assertTrue(services.append().append(new AppendCommand(
                "/streams/durable",
                List.of("payload".getBytes(StandardCharsets.UTF_8)),
                "text/plain",
                null,
                null,
                false)).applied());
            services.lifecycle().close("/streams/durable");

            for (var stream : node.streamService().openStreamSnapshot()) {
                stream.close().get(15, TimeUnit.SECONDS);
            }

            awaitCommitted(node, 1);
            node.metadataNode().triggerSnapshot();
            awaitArchived(node.snapshotArchive());
        }

        deleteRecursively(dataDir);

        try (StreamStackNode restored = new StreamStackNode(config(dataDir, objectDir, 2, true, freePort()))) {
            restored.start();
            assertTrue(restored.metadataNode().restoredFromArchive());
            StreamService services = restored.service();
            StreamMeta head = services.lifecycle().head("/streams/durable").orElseThrow();

            assertTrue(head.closed());
            assertEquals(1, head.nextOffset().recordOffset());
            ReadResult batch = services.read().read("/streams/durable", OffsetToken.beginning(), 1024, 0);

            assertEquals(1, batch.records().size());
            assertEquals("payload", new String(batch.records().get(0).payload(), StandardCharsets.UTF_8));
            assertTrue(services.lifecycle().create(new CreateCommand(
                "/streams/after-restore", "text/plain", null, null, false, new byte[0])).created());
        }
    }

    @Test
    void acknowledgedTailSurvivesGracefulShutdownAndRestore() throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path objectDir = tempDir.resolve("objects");

        try (StreamStackNode node = new StreamStackNode(
            config(dataDir, objectDir, 1, false, freePort(), 104857600L))) {
            node.start();
            StreamService services = node.service();

            assertTrue(services.lifecycle().create(new CreateCommand(
                "/streams/tail", "text/plain", null, null, false, new byte[0])).created());
            assertTrue(services.append().append(new AppendCommand(
                "/streams/tail",
                List.of("acked-tail".getBytes(StandardCharsets.UTF_8)),
                "text/plain",
                null,
                null,
                false)).applied());
        }

        deleteRecursively(dataDir);

        try (StreamStackNode restored = new StreamStackNode(
            config(dataDir, objectDir, 2, true, freePort(), 104857600L))) {
            restored.start();
            assertTrue(restored.metadataNode().restoredFromArchive());
            StreamService services = restored.service();
            StreamMeta head = services.lifecycle().head("/streams/tail").orElseThrow();

            assertEquals(1, head.nextOffset().recordOffset());
            ReadResult batch = services.read().read("/streams/tail", OffsetToken.beginning(), 1024, 0);

            assertEquals(1, batch.records().size());
            assertEquals("acked-tail", new String(batch.records().get(0).payload(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void restoreSkippedWhenLocalSnapshotExists() throws Exception {
        Path dataDir = tempDir.resolve("data");
        Path objectDir = tempDir.resolve("objects");
        int raftPort = freePort();

        try (StreamStackNode node = new StreamStackNode(config(dataDir, objectDir, 1, false, raftPort))) {
            node.start();
            node.metadataNode().triggerSnapshot();
            awaitArchived(node.snapshotArchive());
        }

        try (StreamStackNode node = new StreamStackNode(config(dataDir, objectDir, 2, true, raftPort))) {
            node.start();
            assertFalse(node.metadataNode().restoredFromArchive());
        }
    }

    @Test
    void restoreFailsWithoutArchivedSnapshot() {
        Path dataDir = tempDir.resolve("data");
        Path objectDir = tempDir.resolve("objects");

        assertThrows(IllegalStateException.class,
            () -> new StreamStackNode(config(dataDir, objectDir, 1, true, freePort())));
    }

    private ServerConfig config(Path dataDir, Path objectDir, long nodeEpoch, boolean restore, int raftPort)
        throws Exception {
        return config(dataDir, objectDir, nodeEpoch, restore, raftPort, 1L);
    }

    private ServerConfig config(Path dataDir, Path objectDir, long nodeEpoch, boolean restore, int raftPort,
        long walUploadThreshold) throws Exception {
        ServerConfig.Builder builder = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(nodeEpoch)
            .httpHost("127.0.0.1")
            .httpPort(freePort())
            .adminPort(0)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(dataDir.toFile())
            .objectDir(objectDir.toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .restoreFromStorage(restore);

        builder.streamConfig().walUploadThreshold(walUploadThreshold);

        return builder.build();
    }

    private static void awaitCommitted(StreamStackNode node, long expectedEndOffset) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);

        while (System.nanoTime() < deadline) {
            boolean committed = node.metadataNode().stateMachine().read(() ->
                node.metadataNode().stateMachine().streamControlManager().streamsMetadata().values().stream()
                    .anyMatch(stream -> stream.endOffset() >= expectedEndOffset));

            if (committed) {
                return;
            }

            Thread.sleep(50);
        }

        throw new AssertionError("no stream committed endOffset " + expectedEndOffset + " to metadata");
    }

    private static void awaitArchived(SnapshotArchive archive) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);

        while (System.nanoTime() < deadline) {
            if (archive.successCount() > 0) {
                return;
            }

            Thread.sleep(50);
        }

        throw new AssertionError("snapshot was not archived within timeout");
    }

    private static int freePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }
}
