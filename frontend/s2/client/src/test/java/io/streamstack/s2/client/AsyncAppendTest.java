package io.streamstack.s2.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.StreamPosition;
import io.streamstack.s2.model.exception.S2Exception;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.response.AppendResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncAppendTest {

    private HttpServer server;
    private S2 s2;
    private Stream stream;

    private final List<AppendRequest> requests = new CopyOnWriteArrayList<>();
    private final AtomicLong nextSeqNum = new AtomicLong();
    private final AtomicInteger failStatus = new AtomicInteger();
    private volatile CountDownLatch firstResponseGate;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handleAppend);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        s2 = S2.builder("http://127.0.0.1:" + server.getAddress().getPort()).build();
        stream = s2.basin("test-basin").stream("test-stream");
    }

    @AfterEach
    void tearDown() {
        s2.close();
        server.stop(0);
    }

    @Test
    void flushAsyncChainsMatchSeqNumPredictively() throws Exception {
        nextSeqNum.set(10);
        AppendSession session = stream.appendSession().matchSeqNum(10L);

        submit(session, 2);
        CompletableFuture<AppendResponse> first = session.flushAsync();

        submit(session, 3);
        CompletableFuture<AppendResponse> second = session.flushAsync();

        submit(session, 4);
        CompletableFuture<AppendResponse> third = session.flushAsync();

        CompletableFuture.allOf(first, second, third).get(10, TimeUnit.SECONDS);

        assertEquals(3, requests.size());
        assertEquals(10L, matchSeqNumForBatchSize(2));
        assertEquals(12L, matchSeqNumForBatchSize(3));
        assertEquals(15L, matchSeqNumForBatchSize(4));
        assertEquals(19, session.lastAck().end().seqNum());
    }

    @Test
    void submitBlocksWhenInFlightWindowIsFull() throws Exception {
        firstResponseGate = new CountDownLatch(1);
        Producer producer = stream.producer().maxRecords(1).maxInFlightBytes(1);

        producer.submit(record("first"));
        AtomicBoolean secondSubmitted = new AtomicBoolean();
        Thread submitter = new Thread(() -> {
            producer.submit(record("second"));
            secondSubmitted.set(true);
        });

        submitter.start();
        submitter.join(400);
        assertFalse(secondSubmitted.get());
        assertEquals(1, requests.size());

        firstResponseGate.countDown();
        submitter.join(10_000);
        assertTrue(secondSubmitted.get());

        AppendResponse ack = producer.flush();

        assertEquals(2, requests.size());
        assertEquals(2, ack.end().seqNum());
        producer.close();
    }

    @Test
    void failedAppendPoisonsProducer() throws Exception {
        failStatus.set(500);
        Producer producer = stream.producer().maxRecords(1).maxInFlightBytes(1024);

        IllegalStateException submitFailure = null;

        for (int i = 0; i < 200 && Objects.isNull(submitFailure); i++) {
            try {
                producer.submit(record("record-" + i));
                Thread.sleep(25);
            } catch (IllegalStateException e) {
                submitFailure = e;
            }
        }

        assertNotNull(submitFailure);
        assertInstanceOf(S2Exception.class, submitFailure.getCause());
        assertThrows(IllegalStateException.class, producer::flush);
        IllegalStateException closeFailure = assertThrows(IllegalStateException.class, producer::close);
        assertInstanceOf(S2Exception.class, closeFailure.getCause());
    }

    @Test
    void syncFlushStillChainsFromAck() {
        AppendSession session = stream.appendSession().matchSeqNum(0L);

        submit(session, 2);
        AppendResponse first = session.flush();

        submit(session, 1);
        AppendResponse second = session.flush();

        assertEquals(2, first.end().seqNum());
        assertEquals(3, second.end().seqNum());
        assertEquals(0L, matchSeqNumForBatchSize(2));
        assertEquals(2L, matchSeqNumForBatchSize(1));
    }

    private void handleAppend(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        AppendRequest request = S2Json.read(body, AppendRequest.class, Format.RAW);

        requests.add(request);

        CountDownLatch gate = firstResponseGate;

        if (Objects.nonNull(gate) && requests.size() == 1) {
            try {
                gate.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        byte[] response;
        int status = failStatus.get();

        if (status > 0) {
            response = "{\"code\":\"internal\",\"message\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
        } else {
            long start = nextSeqNum.getAndAdd(request.records().size());
            long end = start + request.records().size();

            response = S2Json.write(new AppendResponse(
                new StreamPosition(start, 1L),
                new StreamPosition(end, 1L),
                new StreamPosition(end, 1L)), Format.RAW);
            status = 200;
        }

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);

        try (OutputStream out = exchange.getResponseBody()) {
            out.write(response);
        }
    }

    private long matchSeqNumForBatchSize(int size) {
        return requests.stream()
            .filter(request -> request.records().size() == size)
            .findFirst()
            .orElseThrow()
            .matchSeqNum();
    }

    private static void submit(AppendSession session, int count) {
        for (int i = 0; i < count; i++) {
            session.submit(record("r" + i));
        }
    }

    private static AppendRecord record(String body) {
        return new AppendRecord(null, List.of(), body.getBytes(StandardCharsets.UTF_8));
    }
}
