package io.streamstack.server;

import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamList;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.StreamServiceException;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamStackNodeServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void createAppendReadCloseViaServices() throws Exception {
        int raftPort = freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(freePort())
            .adminPort(0)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .build();
        try (StreamStackNode node = new StreamStackNode(config)) {
            node.start();
            StreamService services = node.service();
            CreateResult created = services.lifecycle().create(new CreateCommand(
                "/streams/demo", "text/plain", null, null, false, new byte[0]));
            assertTrue(created.created());
            AppendResult appended = services.append().append(new AppendCommand(
                "/streams/demo",
                List.of("hello".getBytes(StandardCharsets.UTF_8)),
                "text/plain",
                null,
                null,
                false));
            assertTrue(appended.applied());
            assertFalse(appended.closed());
            ReadResult batch = services.read().read("/streams/demo", OffsetToken.beginning(), 1024, 0);

            assertEquals(1, batch.records().size());
            assertEquals("hello", new String(batch.records().get(0).payload(), StandardCharsets.UTF_8));
            assertTrue(batch.upToDate());
            assertTrue(services.lifecycle().close("/streams/demo").nextOffset().recordOffset() >= 1);
            assertTrue(services.lifecycle().head("/streams/demo").orElseThrow().closed());
            atomicBatchAppendReadAndTrim(services);
        }
    }

    private static void atomicBatchAppendReadAndTrim(StreamService services) throws Exception {
        services.lifecycle().create(new CreateCommand(
            "/streams/batch", "application/octet-stream", null, null, false, new byte[0]));
        AppendResult appended = services.append().append(new AppendCommand(
            "/streams/batch",
            List.of(
                "a".getBytes(StandardCharsets.UTF_8),
                "bb".getBytes(StandardCharsets.UTF_8),
                "ccc".getBytes(StandardCharsets.UTF_8)),
            "application/octet-stream",
            null,
            null,
            false,
            true));
        assertTrue(appended.applied());
        assertEquals(3, appended.nextOffset().recordOffset());
        ReadResult all = services.read().read("/streams/batch", OffsetToken.beginning(), 1024, 0);

        assertEquals(3, all.records().size());
        assertEquals("a", new String(all.records().get(0).payload(), StandardCharsets.UTF_8));
        assertEquals("bb", new String(all.records().get(1).payload(), StandardCharsets.UTF_8));
        assertEquals("ccc", new String(all.records().get(2).payload(), StandardCharsets.UTF_8));
        assertEquals(0, all.records().get(0).offset().recordOffset());
        assertEquals(1, all.records().get(1).offset().recordOffset());
        assertEquals(2, all.records().get(2).offset().recordOffset());
        assertTrue(all.upToDate());
        ReadResult tail = services.read().read(
            "/streams/batch", OffsetToken.ofRecordOffset(1), 1024, 0);
        assertEquals(2, tail.records().size());
        assertEquals("bb", new String(tail.records().get(0).payload(), StandardCharsets.UTF_8));
        ReadResult limited = services.read().read("/streams/batch", OffsetToken.beginning(), 1024, 2);

        assertEquals(2, limited.records().size());
        assertEquals(2, limited.nextOffset().recordOffset());
        assertFalse(limited.upToDate());
        long effective = services.lifecycle().trim("/streams/batch", 2);

        assertTrue(effective >= 0 && effective <= 2, "effective trim " + effective);
        assertEquals(effective, services.lifecycle().head("/streams/batch")
            .orElseThrow().startOffset().recordOffset());
        ReadResult trimmed = services.read().read(
            "/streams/batch", OffsetToken.ofRecordOffset(2), 1024, 0);
        assertEquals(1, trimmed.records().size());
        assertEquals("ccc", new String(trimmed.records().get(0).payload(), StandardCharsets.UTF_8));
    }

    @Test
    void listAndMatchSeqViaServices() throws Exception {
        int raftPort = freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(freePort())
            .adminPort(0)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .build();
        try (StreamStackNode node = new StreamStackNode(config)) {
            node.start();
            StreamService services = node.service();

            for (String name : List.of("/list/a", "/list/b", "/list/c", "/other/x")) {
                services.lifecycle().create(new CreateCommand(name, "text/plain", null, null, false, new byte[0]));
            }

            listByPrefix(services);
            matchSeq(services);
        }
    }

    private static void listByPrefix(StreamService services) throws Exception {
        StreamList all = services.lifecycle().list("/list/", null, 0);

        assertEquals(List.of("/list/a", "/list/b", "/list/c"),
            all.streams().stream().map(StreamMeta::name).toList());
        assertFalse(all.hasMore());

        StreamList page = services.lifecycle().list("/list/", null, 2);

        assertEquals(List.of("/list/a", "/list/b"), page.streams().stream().map(StreamMeta::name).toList());
        assertTrue(page.hasMore());

        StreamList rest = services.lifecycle().list("/list/", "/list/b", 0);

        assertEquals(List.of("/list/c"), rest.streams().stream().map(StreamMeta::name).toList());
        assertFalse(rest.hasMore());
    }

    private static void matchSeq(StreamService services) throws Exception {
        AppendResult first = services.append().append(appendAt("/list/a", "one", 0L));

        assertTrue(first.applied());
        assertEquals(1, first.nextOffset().recordOffset());

        StreamServiceException conflict = assertThrows(StreamServiceException.class,
            () -> services.append().append(appendAt("/list/a", "stale", 0L)));

        assertEquals(StreamServiceException.Kind.MATCH_FAILED, conflict.kind());
        assertEquals(1, conflict.nextOffset().recordOffset());
        assertTrue(services.append().append(appendAt("/list/a", "two", 1L)).applied());
    }

    private static AppendCommand appendAt(String name, String payload, long matchSeq) {
        return new AppendCommand(
            name, List.of(payload.getBytes(StandardCharsets.UTF_8)), "text/plain", null, matchSeq, null, false,
            false);
    }

    private static int freePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
