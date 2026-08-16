package io.streamstack.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import io.streamstack.client.helper.RetryPolicy;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamStackClientTest {

    @Test
    void retriesIdempotentReads() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = start(exchange -> {
            if (requests.incrementAndGet() == 1) {
                respond(exchange, 503, "{\"error\":\"unavailable\"}");
            } else {
                respond(exchange, 200, "{\"streams\":[],\"has_more\":false}");
            }
        });

        try (StreamStack client = StreamStack.builder()
            .baseUrl(baseUrl(server))
            .retryPolicy(RetryPolicy.builder()
                .maxAttempts(2)
                .initialBackoff(Duration.ZERO)
                .maxBackoff(Duration.ZERO)
                .build())
            .build()) {
            assertFalse(client.list("/", null, 10).hasMore());
            assertEquals(2, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void doesNotRetryAppend() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = start(exchange -> {
            requests.incrementAndGet();
            respond(exchange, 503, "{\"error\":\"unavailable\"}");
        });

        try (StreamStack client = StreamStack.builder()
            .baseUrl(baseUrl(server))
            .retryPolicy(RetryPolicy.builder()
                .maxAttempts(3)
                .initialBackoff(Duration.ZERO)
                .maxBackoff(Duration.ZERO)
                .build())
            .build()) {
            assertThrows(StreamStackException.class,
                () -> client.appendRaw("/events", new byte[] {1}, "application/octet-stream"));
            assertEquals(1, requests.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void serializableConfigBuildsClientWithHeaders() throws Exception {
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = start(exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            respond(exchange, 404, "");
        });
        RetryPolicy retryPolicy = new RetryPolicy(
            2,
            Duration.ofMillis(10),
            Duration.ofMillis(20),
            2.0,
            Set.of(429, 503));
        StreamStackConfig config = StreamStackConfig.builder()
            .baseUrl(baseUrl(server))
            .connectTimeout(Duration.ofSeconds(2))
            .requestTimeout(Duration.ofSeconds(3))
            .longPollTimeout(Duration.ofSeconds(4))
            .header("Authorization", "Bearer test")
            .retryPolicy(retryPolicy)
            .build();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(config);
        }

        StreamStackConfig restored;

        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (StreamStackConfig) input.readObject();
        }

        try (StreamStack client = restored.build()) {
            assertTrue(client.head("/missing").isEmpty());
            assertEquals("Bearer test", authorization.get());
            assertEquals(Duration.ofSeconds(4), restored.longPollTimeout());
            assertEquals(2, restored.retryPolicy().maxAttempts());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void exceptionHelpersReflectStatus() {
        assertTrue(new StreamStackException(404, "not_found", null, null).isNotFound());
        assertTrue(new StreamStackException(410, "gone", null, null).isGone());
        assertTrue(new StreamStackException(409, "conflict", null, null).isConflict());
        assertTrue(new StreamStackException(0, "invalid_response", null, null).isTransport());
        assertFalse(new StreamStackException(500, "internal", null, null).isTransport());
    }

    private static HttpServer start(HttpHandler handler) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);

        server.createContext("/", handler);
        server.start();

        return server;
    }

    private static String baseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, int status, String body) throws java.io.IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);

        boolean head = "HEAD".equals(exchange.getRequestMethod());

        exchange.sendResponseHeaders(status, head ? -1 : bytes.length);

        if (!head) {
            exchange.getResponseBody().write(bytes);
        }

        exchange.close();
    }
}
