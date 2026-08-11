package io.streamstack.server;

import io.streamstack.server.http.OwnershipRouter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamStackServerIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void createAppendReadCloseEndToEnd() throws Exception {
        int httpPort = TestPorts.freePort();
        int raftPort = TestPorts.freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(httpPort)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(OwnershipRouter.Mode.LOCAL_ALWAYS)
            .longPollTimeoutSec(1)
            .build();

        try (StreamStackServer server = new StreamStackServer(config)) {
            server.start();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            String base = server.baseUrl() + "/streams/demo";

            HttpResponse<String> create = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "text/plain")
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(201, create.statusCode(), create.body());

            HttpResponse<String> append = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("hello-world"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, append.statusCode());
            assertTrue(append.headers().firstValue("Stream-Next-Offset").isPresent());

            HttpResponse<byte[]> read = client.send(
                HttpRequest.newBuilder(URI.create(base + "?offset=-1")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, read.statusCode());
            assertEquals("hello-world", new String(read.body(), StandardCharsets.UTF_8));

            HttpResponse<String> close = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Stream-Closed", "true")
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, close.statusCode());

            HttpResponse<String> rejected = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("more"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(409, rejected.statusCode());
            assertEquals("true", rejected.headers().firstValue("Stream-Closed").orElse(""));
        }
    }
}
