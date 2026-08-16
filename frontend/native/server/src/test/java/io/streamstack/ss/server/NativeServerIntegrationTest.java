package io.streamstack.ss.server;

import io.streamstack.ss.client.Producer;
import io.streamstack.ss.client.StreamStack;
import io.streamstack.ss.client.StreamStackException;
import io.streamstack.ss.model.RecordEnvelope;
import io.streamstack.ss.model.SequencedRecord;
import io.streamstack.ss.model.request.AppendRequest;
import io.streamstack.ss.model.response.AppendResponse;
import io.streamstack.ss.model.response.HeadResponse;
import io.streamstack.ss.model.response.ListResponse;
import io.streamstack.ss.model.response.ReadResponse;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NativeServerIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void protocolEndToEnd() throws Exception {
        try (NativeServer server = new NativeServer(config());
             StreamStack client = StreamStack.builder().baseUrl(server.baseUrl()).build()) {
            server.start();

            createSemantics(client);
            appendAndRead(server, client);
            matchSeq(client);
            producerSession(client);
            listTrimCloseDelete(client);
        }
    }

    private void createSemantics(StreamStack client) {
        assertTrue(client.create("/native/orders", "text/plain"));
        assertFalse(client.create("/native/orders", "text/plain"));

        StreamStackException conflict = assertThrows(StreamStackException.class,
            () -> client.create("/native/orders", "application/json"));

        assertEquals(409, conflict.status());
        assertEquals("conflict", conflict.code());
    }

    private void appendAndRead(NativeServer server, StreamStack client) throws Exception {
        AppendResponse raw = client.appendRaw("/native/orders", "hello-world".getBytes(StandardCharsets.UTF_8),
            "text/plain");

        assertEquals(0, raw.startSeq());
        assertEquals(1, raw.nextSeq());
        assertNotNull(raw.timestamp());

        AppendResponse batch = client.append("/native/orders", new AppendRequest(List.of(
            new RecordEnvelope(0, Map.of("key", "a"), "one".getBytes(StandardCharsets.UTF_8)),
            new RecordEnvelope(0, Map.of(), "two".getBytes(StandardCharsets.UTF_8)))));

        assertEquals(1, batch.startSeq());
        assertEquals(3, batch.nextSeq());
        assertTrue(batch.timestamp() >= raw.timestamp());

        HttpClient http = HttpClient.newHttpClient();
        String url = server.baseUrl() + "/native/orders";
        HttpResponse<String> json = http.send(
            HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/vnd.streamstack.batch+json")
                .POST(HttpRequest.BodyPublishers.ofString(
                    "{\"records\":[{\"headers\":{\"k\":\"v\"},\"body\":\"three\"},{\"body_b64\":\"AAECgP8=\"}]}"))
                .build(),
            HttpResponse.BodyHandlers.ofString());

        assertEquals(200, json.statusCode(), json.body());
        assertEquals("3", json.headers().firstValue("SS-Start-Seq").orElse(""));
        assertEquals("5", json.headers().firstValue("SS-Next-Seq").orElse(""));

        HeadResponse head = client.head("/native/orders").orElseThrow();

        assertEquals(5, head.nextSeq());
        assertEquals("text/plain", head.contentType());

        ReadResponse page = client.read("/native/orders", 0, 0, 0);

        assertEquals(5, page.records().size());
        assertTrue(page.upToDate());

        for (int i = 0; i < page.records().size(); i++) {
            assertEquals(i, page.records().get(i).seq());
        }

        assertEquals(Map.of("key", "a"), page.records().get(1).envelope().headers());
        assertEquals("three", new String(page.records().get(3).envelope().body(), StandardCharsets.UTF_8));

        long previous = 0;

        for (SequencedRecord record : page.records()) {
            assertTrue(record.envelope().timestamp() >= previous);
            previous = record.envelope().timestamp();
        }

        byte[] rawBody = client.readRaw("/native/orders", 0, 0);

        assertTrue(new String(rawBody, StandardCharsets.UTF_8).startsWith("hello-worldonetwothree"));

        HttpResponse<String> catchUp = http.send(
            HttpRequest.newBuilder(URI.create(url + "?seq=0")).GET().build(),
            HttpResponse.BodyHandlers.ofString());

        assertEquals(200, catchUp.statusCode());
        assertEquals("application/json", catchUp.headers().firstValue("Content-Type").orElse(""));
        String etag = catchUp.headers().firstValue("ETag").orElseThrow();
        HttpResponse<String> cached = http.send(
            HttpRequest.newBuilder(URI.create(url + "?seq=0")).header("If-None-Match", etag).GET().build(),
            HttpResponse.BodyHandlers.ofString());

        assertEquals(304, cached.statusCode());
    }

    private void matchSeq(StreamStack client) {
        AppendResponse ack = client.append("/native/orders", new AppendRequest(List.of(
            new RecordEnvelope(0, Map.of(), "cas".getBytes(StandardCharsets.UTF_8))), 5L));

        assertEquals(6, ack.nextSeq());

        StreamStackException stale = assertThrows(StreamStackException.class,
            () -> client.append("/native/orders", new AppendRequest(List.of(
                new RecordEnvelope(0, Map.of(), "stale".getBytes(StandardCharsets.UTF_8))), 5L)));

        assertEquals(412, stale.status());
        assertEquals("match_failed", stale.code());
        assertEquals(6, stale.nextSeq());
    }

    private void producerSession(StreamStack client) {
        try (Producer producer = client.producer("/native/orders", "p1").maxRecords(10)) {
            for (int i = 0; i < 25; i++) {
                producer.submit(new RecordEnvelope(0, Map.of(), ("r" + i).getBytes(StandardCharsets.UTF_8)));
            }

            AppendResponse ack = producer.flush();

            assertEquals(31, ack.nextSeq());
            assertNotNull(ack.producerEpoch());
        }

        long before = client.head("/native/orders").orElseThrow().nextSeq();
        AppendRequest request = new AppendRequest(
            List.of(new RecordEnvelope(0, Map.of(), new byte[] {1})), null, "p2", 1L, 0L);
        AppendResponse first = client.append("/native/orders", request);
        AppendResponse retry = client.append("/native/orders", request);

        assertEquals(before + 1, first.nextSeq());
        assertEquals(first.nextSeq(), retry.nextSeq());
        assertEquals(before + 1, client.head("/native/orders").orElseThrow().nextSeq());
    }

    private void listTrimCloseDelete(StreamStack client) {
        client.create("/native/other", "application/octet-stream");

        ListResponse listing = client.list("/native/", null, 0);

        assertEquals(List.of("/native/orders", "/native/other"),
            listing.streams().stream().map(HeadResponse::name).toList());
        assertFalse(listing.hasMore());

        ListResponse page = client.list("/native/", null, 1);

        assertEquals(1, page.streams().size());
        assertTrue(page.hasMore());

        awaitTrim(client, "/native/orders", 2);
        assertEquals(2, client.head("/native/orders").orElseThrow().startSeq());

        long tail = client.closeStream("/native/orders");

        assertTrue(client.head("/native/orders").orElseThrow().closed());

        StreamStackException closed = assertThrows(StreamStackException.class,
            () -> client.appendRaw("/native/orders", new byte[] {1}, "text/plain"));

        assertEquals(409, closed.status());
        assertEquals("closed", closed.code());
        assertEquals(tail, closed.nextSeq());

        assertTrue(client.delete("/native/other"));
        assertFalse(client.delete("/native/other"));
        assertTrue(client.head("/native/other").isEmpty());
    }

    @Test
    void liveReads() throws Exception {
        try (NativeServer server = new NativeServer(config());
             StreamStack client = StreamStack.builder().baseUrl(server.baseUrl()).build()) {
            server.start();
            client.create("/live/feed", "text/plain");

            HttpClient http = HttpClient.newHttpClient();
            String url = server.baseUrl() + "/live/feed";
            HttpResponse<String> idle = http.send(
                HttpRequest.newBuilder(URI.create(url + "?seq=0&live=long-poll")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(204, idle.statusCode());
            assertEquals("true", idle.headers().firstValue("SS-Up-To-Date").orElse(""));

            CompletableFuture<HttpResponse<String>> waiting = http.sendAsync(
                HttpRequest.newBuilder(URI.create(url + "?seq=0&live=long-poll")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

            Thread.sleep(300);
            client.appendRaw("/live/feed", "ping".getBytes(StandardCharsets.UTF_8), "text/plain");

            HttpResponse<String> data = waiting.get();

            assertEquals(200, data.statusCode());
            assertEquals("1", data.headers().firstValue("SS-Next-Seq").orElse(""));
            assertTrue(data.body().contains("\"body\":\"ping\""));

            client.appendRaw("/live/feed", "pong".getBytes(StandardCharsets.UTF_8), "text/plain");

            HttpResponse<String> sse = http.send(
                HttpRequest.newBuilder(URI.create(url + "?seq=0&live=sse")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, sse.statusCode());
            assertTrue(sse.headers().firstValue("Content-Type").orElse("").startsWith("text/event-stream"));
            assertTrue(sse.body().contains("event: data"));
            assertTrue(sse.body().contains("id: 2"));
            assertTrue(sse.body().contains("\"body\":\"ping\""));

            HttpResponse<String> resumed = http.send(
                HttpRequest.newBuilder(URI.create(url + "?live=sse"))
                    .header("Last-Event-ID", "1")
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString());

            assertEquals(200, resumed.statusCode());
            assertTrue(resumed.body().contains("\"body\":\"pong\""));
            assertFalse(resumed.body().contains("\"body\":\"ping\""));
        }
    }

    private static void awaitTrim(StreamStack client, String stream, long seq) {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();

        while (client.trim(stream, seq) < seq) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("trim to " + seq + " never committed");
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private ServerConfig config() throws Exception {
        int raftPort = freePort();
        ServerConfig.Builder builder = ServerConfig.builder();

        builder.streamConfig().walUploadIntervalMs(200L);

        return builder
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
            .longPollTimeoutSec(1)
            .sseMaxDurationSec(2)
            .build();
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
