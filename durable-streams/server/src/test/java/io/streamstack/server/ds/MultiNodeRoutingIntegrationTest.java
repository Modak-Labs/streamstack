package io.streamstack.server.ds;

import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

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

public class MultiNodeRoutingIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void nonOwnerRedirectsToOwnerAndFailoverWorks() throws Exception {
        int httpA = TestPorts.freePort();
        int httpB = TestPorts.freePort();
        int raftA = TestPorts.freePort();
        int raftB = TestPorts.freePort();
        int raftC = TestPorts.freePort();
        List<String> peers = List.of(
            "127.0.0.1:" + raftA,
            "127.0.0.1:" + raftB,
            "127.0.0.1:" + raftC);
        ServerConfig configA = nodeConfig(1, httpA, raftA, peers, tempDir.resolve("a"), tempDir.resolve("shared-objects"));
        ServerConfig configB = nodeConfig(2, httpB, raftB, peers, tempDir.resolve("b"), tempDir.resolve("shared-objects"));
        ServerConfig configC = nodeConfig(3, TestPorts.freePort(), raftC, peers, tempDir.resolve("c"),
            tempDir.resolve("shared-objects"));
        DurableStreamsServer serverA = new DurableStreamsServer(configA);
        DurableStreamsServer serverB = new DurableStreamsServer(configB);
        DurableStreamsServer serverC = new DurableStreamsServer(configC);
        try {
            serverA.start();
            serverB.start();
            serverC.start();
            HttpClient noRedirect = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
            HttpClient follow = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
            String path = "/streams/owned";
            HttpResponse<String> create = follow.send(
                HttpRequest.newBuilder(URI.create(serverA.baseUrl() + path))
                    .header("Content-Type", "text/plain")
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(201, create.statusCode());
            HttpResponse<String> redirected = noRedirect.send(
                HttpRequest.newBuilder(URI.create(serverB.baseUrl() + path))
                    .header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("from-b"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(307, redirected.statusCode());
            assertTrue(redirected.headers().firstValue("Location").orElse("").startsWith(serverA.baseUrl()));
            HttpResponse<String> appended = follow.send(
                HttpRequest.newBuilder(URI.create(serverB.baseUrl() + path))
                    .header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("from-b"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, appended.statusCode());
            HttpResponse<byte[]> read = follow.send(
                HttpRequest.newBuilder(URI.create(serverA.baseUrl() + path + "?offset=-1")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, read.statusCode());
            assertEquals("from-b", new String(read.body(), StandardCharsets.UTF_8));
            serverA.close();
            HttpResponse<String> afterFailover = null;
            for (int i = 0; i < 20; i++) {
                afterFailover = follow.send(
                    HttpRequest.newBuilder(URI.create(serverB.baseUrl() + path))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("after-failover"))
                        .timeout(Duration.ofSeconds(5))
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
                if (afterFailover.statusCode() == 204) {
                    break;
                }
                Thread.sleep(250);
            }
            assertEquals(204, afterFailover.statusCode());
        } finally {
            closeQuietly(serverA);
            closeQuietly(serverB);
            closeQuietly(serverC);
        }
    }

    private static ServerConfig nodeConfig(
        int nodeId,
        int httpPort,
        int raftPort,
        List<String> peers,
        Path dir,
        Path objectDir) {
        return ServerConfig.builder()
            .nodeId(nodeId)
            .nodeEpoch(nodeId)
            .httpHost("127.0.0.1")
            .httpPort(httpPort)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(peers)
            .dataDir(dir.resolve("data").toFile())
            .objectDir(objectDir.toFile())
            .routingMode(RoutingMode.REDIRECT)
            .longPollTimeoutSec(1)
            .build();
    }

    private static void closeQuietly(DurableStreamsServer server) {
        try {
            server.close();
        } catch (Exception ignored) {
        }
    }
}
