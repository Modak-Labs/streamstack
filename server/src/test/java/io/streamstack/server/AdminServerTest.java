package io.streamstack.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdminServerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

    @Test
    void adminPlaneServesHealthReadinessClusterNodesAndStreams() throws Exception {
        int raftPort = freePort();
        int adminPort = freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(freePort())
            .adminPort(adminPort)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .build();

        HttpClient client = HttpClient.newHttpClient();

        try (StreamStackNode node = new StreamStackNode(config)) {
            node.start();

            HttpResponse<String> health = get(client, adminPort, "/health");

            assertEquals(200, health.statusCode());
            assertEquals("ok", health.body());

            HttpResponse<String> ready = get(client, adminPort, "/ready");
            JsonNode readyBody = MAPPER.readTree(ready.body());

            assertEquals(200, ready.statusCode());
            assertTrue(readyBody.get("ready").asBoolean());
            assertTrue(readyBody.get("leaderKnown").asBoolean());
            assertTrue(readyBody.get("registered").asBoolean());

            HttpResponse<String> cluster = get(client, adminPort, "/admin/cluster");
            JsonNode clusterBody = MAPPER.readTree(cluster.body());

            assertEquals(200, cluster.statusCode());
            assertEquals(1, clusterBody.get("nodeId").asInt());
            assertTrue(clusterBody.get("raft").get("isLeader").asBoolean());
            assertEquals("127.0.0.1:" + raftPort, clusterBody.get("raft").get("leader").asText());
            assertEquals(0, clusterBody.get("streamCount").asInt());

            HttpResponse<String> nodes = get(client, adminPort, "/admin/nodes");
            JsonNode nodesBody = MAPPER.readTree(nodes.body());

            assertEquals(200, nodes.statusCode());
            assertEquals(1, nodesBody.get("nodes").size());
            assertEquals(1, nodesBody.get("nodes").get(0).get("nodeId").asInt());
            assertTrue(nodesBody.get("nodes").get(0).get("local").asBoolean());

            HttpResponse<String> missing = get(client, adminPort, "/admin/streams/streams/demo");

            assertEquals(404, missing.statusCode());

            node.service().lifecycle().create(new CreateCommand(
                "/streams/demo", "text/plain", null, null, false, new byte[0]));

            HttpResponse<String> stream = get(client, adminPort, "/admin/streams/streams/demo");
            JsonNode streamBody = MAPPER.readTree(stream.body());

            assertEquals(200, stream.statusCode());
            assertEquals("/streams/demo", streamBody.get("name").asText());
            assertTrue(streamBody.get("ownerLocal").asBoolean());
            assertEquals("text/plain", streamBody.get("meta").get("contentType").asText());
            assertFalse(streamBody.get("meta").get("closed").asBoolean());

            HttpResponse<String> snapshotsBefore = get(client, adminPort, "/admin/snapshots");
            JsonNode snapshotsBeforeBody = MAPPER.readTree(snapshotsBefore.body());

            assertEquals(200, snapshotsBefore.statusCode());
            assertEquals(0, snapshotsBeforeBody.get("snapshots").size());

            HttpResponse<String> snapshot = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + adminPort + "/admin/snapshot"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, snapshot.statusCode());
            assertTrue(MAPPER.readTree(snapshot.body()).get("appliedIndex").asLong() > 0);
            awaitArchivedSnapshot(client, adminPort);

            node.markNotReady();

            HttpResponse<String> notReady = get(client, adminPort, "/ready");

            assertEquals(503, notReady.statusCode());
            assertFalse(MAPPER.readTree(notReady.body()).get("ready").asBoolean());
        }
    }

    private static void awaitArchivedSnapshot(HttpClient client, int adminPort) throws Exception {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15);

        while (System.nanoTime() < deadline) {
            JsonNode body = MAPPER.readTree(get(client, adminPort, "/admin/snapshots").body());

            if (body.get("snapshots").size() > 0) {
                assertTrue(body.get("archiveSuccessCount").asLong() > 0);
                assertTrue(body.get("snapshots").get(0).get("appliedIndex").asLong() > 0);
                assertTrue(body.get("snapshots").get(0).get("size").asLong() > 0);
                return;
            }

            Thread.sleep(50);
        }

        throw new AssertionError("no archived snapshot appeared within timeout");
    }

    @Test
    void peerChangesRequireValidBody() throws Exception {
        int raftPort = freePort();
        int adminPort = freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(freePort())
            .adminPort(adminPort)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .build();

        HttpClient client = HttpClient.newHttpClient();

        try (StreamStackNode node = new StreamStackNode(config)) {
            node.start();

            HttpResponse<String> badBody = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + adminPort + "/admin/peers"))
                    .POST(HttpRequest.BodyPublishers.ofString("{}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(400, badBody.statusCode());

            HttpResponse<String> badPeer = client.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + adminPort + "/admin/peers"))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"peer\":\"not a peer\"}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(400, badPeer.statusCode());
        }
    }

    @Test
    void adminDisabledWhenPortIsZero() throws Exception {
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

        assertFalse(config.adminEnabled());

        try (StreamStackNode node = new StreamStackNode(config)) {
            node.start();
            assertTrue(node.isReady());
        }
    }

    private static HttpResponse<String> get(HttpClient client, int port, String path) throws Exception {
        return client.send(
            HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
