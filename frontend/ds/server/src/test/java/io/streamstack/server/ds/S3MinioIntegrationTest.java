package io.streamstack.server.ds;

import java.util.Objects;

import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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

public class S3MinioIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    @Timeout(120)
    void createAppendReadRestartRecovery() throws Exception {
        String endpoint = System.getenv().getOrDefault("STREAMSTACK_S3_ENDPOINT", "http://127.0.0.1:9000");

        Assumptions.assumeTrue(minioReachable(endpoint), "MinIO not reachable at " + endpoint);
        Assumptions.assumeTrue(Objects.nonNull(System.getenv("AWS_ACCESS_KEY_ID")), "AWS_ACCESS_KEY_ID required");
        Assumptions.assumeTrue(Objects.nonNull(System.getenv("AWS_SECRET_ACCESS_KEY")), "AWS_SECRET_ACCESS_KEY required");
        String storage = "0@s3://streams-data?region=us-east-1&endpoint=" + endpoint + "&pathStyle=true";
        String wal = "0@s3://streams-wal?region=us-east-1&endpoint=" + endpoint + "&pathStyle=true";
        String clusterId = "minio-smoke-" + System.currentTimeMillis();
        int httpPort = TestPorts.freePort();
        int raftPort = TestPorts.freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(httpPort)
            .adminPort(0)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .storageUri(storage)
            .walUri(wal)
            .clusterId(clusterId)
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .longPollTimeoutSec(1)
            .build();
        String base;

        try (DurableStreamsServer server = new DurableStreamsServer(config)) {
            server.start();
            base = server.baseUrl() + "/streams/s3-demo";
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
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
                    .POST(HttpRequest.BodyPublishers.ofString("hello-s3"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, append.statusCode());
        }

        ServerConfig recoverConfig = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(2)
            .httpHost("127.0.0.1")
            .httpPort(TestPorts.freePort())
            .adminPort(0)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .storageUri(storage)
            .walUri(wal)
            .clusterId(clusterId)
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .longPollTimeoutSec(1)
            .build();
        try (DurableStreamsServer server = new DurableStreamsServer(recoverConfig)) {
            server.start();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            String recoverBase = server.baseUrl() + "/streams/s3-demo";
            HttpResponse<byte[]> read = client.send(
                HttpRequest.newBuilder(URI.create(recoverBase + "?offset=-1")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, read.statusCode());
            assertEquals("hello-s3", new String(read.body(), StandardCharsets.UTF_8));
            assertTrue(read.body().length > 0);
        }
    }

    private static boolean minioReachable(String endpoint) {
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpResponse<Void> response = client.send(
                HttpRequest.newBuilder(URI.create(endpoint + "/minio/health/live")).GET().timeout(Duration.ofSeconds(2)).build(),
                HttpResponse.BodyHandlers.discarding());
            return response.statusCode() >= 200 && response.statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
