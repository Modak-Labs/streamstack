package io.streamstack.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.streamstack.client.model.CloseResult;
import io.streamstack.client.model.ProducerConfig;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.CreateResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurableStreamClientTest {

    private HttpServer server;
    private DurableStream client;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
        client = DurableStream.create();
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.stop(0);
    }

    private String url(String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    private void handle(String path, BiConsumer<HttpExchange, Integer> handler) {
        AtomicInteger counter = new AtomicInteger();

        server.createContext(path, exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();
                handler.accept(exchange, counter.getAndIncrement());
            } finally {
                exchange.close();
            }
        });
    }

    private static void reply(HttpExchange exchange, int status) {
        try {
            exchange.sendResponseHeaders(status, -1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void plainAppendWith204ReportsAppended() {
        handle("/appended", (exchange, attempt) -> {
            exchange.getResponseHeaders().set("Stream-Next-Offset", "1");
            reply(exchange, 204);
        });

        AppendResponse response = client.append(url("/appended"), "hello".getBytes(StandardCharsets.UTF_8));

        assertTrue(response.appended());
        assertFalse(response.closed());
        assertEquals("1", response.nextOffset().value());
    }

    @Test
    void successiveAppendsKeepSentContentType() {
        List<String> sent = new ArrayList<>();

        handle("/events", (exchange, attempt) -> {
            sent.add(exchange.getRequestHeaders().getFirst("Content-Type"));
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.getResponseHeaders().set("Stream-Next-Offset", Integer.toString(attempt + 1));
            reply(exchange, 204);
        });

        String path = url("/events");

        client.append(path, "test".getBytes(StandardCharsets.UTF_8));
        client.append(path, "test again".getBytes(StandardCharsets.UTF_8));

        assertEquals(List.of("application/octet-stream", "application/octet-stream"), sent);
    }

    @Test
    void closeReportsAlreadyClosedOnlyForDuplicateClose() {
        AtomicBoolean closed = new AtomicBoolean(false);

        handle("/close", (exchange, attempt) -> {
            exchange.getResponseHeaders().set("Stream-Next-Offset", "3");

            if ("HEAD".equals(exchange.getRequestMethod())) {
                if (closed.get()) {
                    exchange.getResponseHeaders().set("Stream-Closed", "true");
                }

                reply(exchange, 200);

                return;
            }

            closed.set(true);
            exchange.getResponseHeaders().set("Stream-Closed", "true");
            reply(exchange, 204);
        });

        CloseResult first = client.close(url("/close"));

        assertFalse(first.alreadyClosed(), "first successful close must not report alreadyClosed");
        assertEquals("3", first.finalOffset().value());
        CloseResult second = client.close(url("/close"));

        assertTrue(second.alreadyClosed(), "duplicate close must report alreadyClosed");
    }

    @Test
    void plainAppendIsNotRetriedOnServerError() {
        AtomicInteger requests = new AtomicInteger();

        handle("/append-500", (exchange, attempt) -> {
            requests.incrementAndGet();
            reply(exchange, 500);
        });

        assertThrows(DurableStreamException.class,
            () -> client.append(url("/append-500"), "x".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, requests.get(), "non-idempotent append must not be re-sent");
    }

    @Test
    void idempotentRequestIsRetriedOnRetryableStatus() {
        handle("/retry-head", (exchange, attempt) -> {
            if (attempt == 0) {
                reply(exchange, 503);
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.getResponseHeaders().set("Stream-Next-Offset", "5");
            reply(exchange, 200);
        });

        assertEquals("5", client.head(url("/retry-head")).nextOffset().value());
    }

    @Test
    void asyncAppendSurfacesServerErrorWithoutRetry() {
        AtomicInteger requests = new AtomicInteger();

        handle("/async-500", (exchange, attempt) -> {
            requests.incrementAndGet();
            reply(exchange, 500);
        });

        CompletableFuture<AppendResponse> future =
            client.appendAsync(url("/async-500"), "x".getBytes(StandardCharsets.UTF_8));
        Exception thrown = assertThrows(Exception.class, future::get);

        assertInstanceOf(DurableStreamException.class, thrown.getCause());
        assertEquals(1, requests.get());
    }

    @Test
    void createUsesServerReportedContentType() {
        handle("/created", (exchange, attempt) -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Stream-Next-Offset", "0");
            reply(exchange, 201);
        });

        CreateResponse response = client.create(url("/created"), "application/json");

        assertTrue(response.created());
        assertEquals("application/json; charset=utf-8", response.contentType());
    }

    @Test
    void producerBatchOfJsonRecordsIsFramedAsJsonArray() {
        List<byte[]> bodies = new ArrayList<>();

        server.createContext("/json-batch", exchange -> {
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();

                synchronized (bodies) {
                    bodies.add(body);
                }

                exchange.getResponseHeaders().set("Stream-Next-Offset", "1");
                exchange.getResponseHeaders().set("Producer-Epoch", "0");
                exchange.getResponseHeaders().set(
                    "Producer-Seq", exchange.getRequestHeaders().getFirst("Producer-Seq"));
                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
            }
        });

        ProducerConfig config = ProducerConfig.builder()
            .contentType("application/json")
            .lingerMs(200)
            .build();
        try (IdempotentProducer producer = client.producer(url("/json-batch"), "p1", config)) {
            producer.append("{\"a\":1}");
            producer.append("{\"b\":2}");
            producer.flush();
        }

        synchronized (bodies) {
            assertEquals(1, bodies.size(), "both records should be sent in one batch");
            assertEquals("[{\"a\":1},{\"b\":2}]", new String(bodies.get(0), StandardCharsets.UTF_8));
        }
    }

    @Test
    void producerSingleJsonRecordIsWrappedInArray() {
        List<byte[]> bodies = new ArrayList<>();

        server.createContext("/json-single", exchange -> {
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();

                synchronized (bodies) {
                    bodies.add(body);
                }

                exchange.getResponseHeaders().set("Stream-Next-Offset", "1");
                exchange.sendResponseHeaders(200, -1);
            } finally {
                exchange.close();
            }
        });

        ProducerConfig config = ProducerConfig.builder()
            .contentType("application/json")
            .lingerMs(0)
            .build();
        try (IdempotentProducer producer = client.producer(url("/json-single"), "p1", config)) {
            producer.append("{\"a\":1}");
            producer.flush();
        }

        synchronized (bodies) {
            assertEquals(1, bodies.size());
            assertEquals("[{\"a\":1}]", new String(bodies.get(0), StandardCharsets.UTF_8));
        }
    }
}
