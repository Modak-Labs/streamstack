package io.streamstack.server;

import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        }
    }

    private static int freePort() throws Exception {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
