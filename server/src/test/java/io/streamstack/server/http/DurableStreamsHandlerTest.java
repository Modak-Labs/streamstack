package io.streamstack.server.http;

import io.javalin.Javalin;
import io.streamstack.server.store.CreateResult;
import io.streamstack.server.store.OffsetToken;
import io.streamstack.server.store.ReadResult;
import io.streamstack.server.store.StoreException;
import io.streamstack.server.store.StreamInfo;
import io.streamstack.server.store.StreamStore;
import io.streamstack.server.store.StreamWaiterRegistry;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurableStreamsHandlerTest {
    @Test
    void createAppendReadCloseMatrix() throws Exception {
        InMemoryStore store = new InMemoryStore();
        DurableStreamsHandler handler = new DurableStreamsHandler(store, Duration.ofMillis(50), Duration.ofSeconds(1), 1024);
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);
        app.get("/*", handler::handle);
        app.post("/*", handler::handle);
        app.put("/*", handler::handle);
        app.start(0);
        try {
            String base = "http://127.0.0.1:" + app.port() + "/streams/demo";
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> created = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(201, created.statusCode());
            assertTrue(created.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).isPresent());

            HttpResponse<String> idempotent = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, idempotent.statusCode());

            HttpResponse<String> conflict = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(409, conflict.statusCode());

            HttpResponse<String> appended = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("hello")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, appended.statusCode());

            HttpResponse<byte[]> read = client.send(
                HttpRequest.newBuilder(URI.create(base + "?offset=-1")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
            assertEquals(200, read.statusCode());
            assertEquals("hello", new String(read.body(), StandardCharsets.UTF_8));

            HttpResponse<String> closed = client.send(
                HttpRequest.newBuilder(URI.create(base)).header(Protocol.H_STREAM_CLOSED, "true")
                    .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, closed.statusCode());
            assertEquals("true", closed.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(""));

            HttpResponse<String> rejected = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                    .POST(HttpRequest.BodyPublishers.ofString("x")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(409, rejected.statusCode());
            assertEquals("true", rejected.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(""));
        } finally {
            app.stop();
        }
    }

    @Test
    void longPollReturnsImmediatelyWhenClosedAtTail() throws Exception {
        InMemoryStore store = new InMemoryStore();
        DurableStreamsHandler handler = new DurableStreamsHandler(store, Duration.ofSeconds(5), Duration.ofSeconds(1), 1024);
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);
        app.get("/*", handler::handle);
        app.post("/*", handler::handle);
        app.put("/*", handler::handle);
        app.start(0);
        try {
            String base = "http://127.0.0.1:" + app.port() + "/streams/closed";
            HttpClient client = HttpClient.newHttpClient();

            client.send(HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            client.send(HttpRequest.newBuilder(URI.create(base)).header(Protocol.H_STREAM_CLOSED, "true")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());

            long start = System.nanoTime();
            HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(URI.create(base + "?offset=0&live=long-poll")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            assertEquals(204, resp.statusCode());
            assertEquals("true", resp.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(""));
            assertTrue(elapsedMs < 1000, "closed long-poll should not wait, took " + elapsedMs + "ms");
        } finally {
            app.stop();
        }
    }

    static final class InMemoryStore implements StreamStore {
        private final ConcurrentHashMap<String, Entry> streams = new ConcurrentHashMap<>();
        private final StreamWaiterRegistry waiters = new StreamWaiterRegistry();
        private final AtomicLong ids = new AtomicLong();

        @Override
        public CreateResult create(URI url, String contentType, Long ttlSeconds, Instant expiresAt, InputStream initialBody)
            throws StoreException {
            String path = url.getPath();
            Entry existing = streams.get(path);
            if (existing != null) {
                if (!existing.matches(contentType, ttlSeconds, expiresAt)) {
                    throw new StoreException(StoreException.Kind.CONFLICT);
                }
                return new CreateResult(false, existing.info());
            }
            Entry created = new Entry(path, ids.incrementAndGet(), contentType, ttlSeconds, expiresAt);
            streams.put(path, created);
            return new CreateResult(true, created.info());
        }

        @Override
        public OffsetToken append(URI url, String contentType, InputStream body) throws StoreException {
            try {
                Entry entry = require(url);
                if (entry.closed) {
                    throw new StoreException(StoreException.Kind.CLOSED, OffsetToken.ofRecordOffset(entry.nextOffset), true);
                }
                if (!entry.contentType.equals(contentType)) {
                    throw new StoreException(StoreException.Kind.CONFLICT, OffsetToken.ofRecordOffset(entry.nextOffset), false);
                }
                byte[] bytes = body.readAllBytes();
                if (bytes.length == 0) {
                    throw new StoreException(StoreException.Kind.BAD_REQUEST);
                }
                entry.chunks.put(entry.nextOffset, bytes);
                entry.nextOffset += 1;
                waiters.notifyAppend(entry.path, entry.nextOffset);
                return OffsetToken.ofRecordOffset(entry.nextOffset);
            } catch (StoreException e) {
                throw e;
            } catch (Exception e) {
                throw new StoreException(StoreException.Kind.BAD_REQUEST, null, false, e.getMessage());
            }
        }

        @Override
        public OffsetToken close(URI url) throws StoreException {
            Entry entry = require(url);
            OffsetToken next = OffsetToken.ofRecordOffset(entry.nextOffset);
            if (entry.closed) {
                throw new StoreException(StoreException.Kind.CLOSED, next, true);
            }
            entry.closed = true;
            waiters.notifyClosed(entry.path);
            return next;
        }

        @Override
        public boolean delete(URI url) {
            Entry removed = streams.remove(url.getPath());
            if (removed != null) {
                waiters.notifyClosed(removed.path);
            }
            return removed != null;
        }

        @Override
        public Optional<StreamInfo> head(URI url) {
            Entry entry = streams.get(url.getPath());
            return entry == null ? Optional.empty() : Optional.of(entry.info());
        }

        @Override
        public ReadResult read(URI url, OffsetToken startOffset, int maxBytes) throws StoreException {
            Entry entry = require(url);
            long start = startOffset.recordOffset();
            if (start > entry.nextOffset) {
                throw new StoreException(StoreException.Kind.BAD_REQUEST);
            }
            if (start == entry.nextOffset) {
                return new ReadResult(new byte[0], entry.contentType, OffsetToken.ofRecordOffset(entry.nextOffset), true,
                    entry.closed);
            }
            byte[] body = entry.chunks.getOrDefault(start, new byte[0]);
            long next = start + 1;
            return new ReadResult(body, entry.contentType, OffsetToken.ofRecordOffset(next), next >= entry.nextOffset,
                entry.closed);
        }

        @Override
        public boolean await(URI url, OffsetToken startOffset, Duration timeout) throws StoreException {
            try {
                Entry entry = require(url);
                if (entry.closed || entry.nextOffset > startOffset.recordOffset()) {
                    return true;
                }
                return waiters.await(entry.path, startOffset, timeout);
            } catch (StoreException e) {
                throw e;
            } catch (Exception e) {
                throw new StoreException(StoreException.Kind.BAD_REQUEST, null, false, e.getMessage());
            }
        }

        private Entry require(URI url) throws StoreException {
            Entry entry = streams.get(url.getPath());
            if (entry == null) {
                throw new StoreException(StoreException.Kind.NOT_FOUND);
            }
            return entry;
        }

        static final class Entry {
            final String path;
            final long streamId;
            final String contentType;
            final Long ttlSeconds;
            final Instant expiresAt;
            final LinkedHashMap<Long, byte[]> chunks = new LinkedHashMap<>();
            long nextOffset;
            boolean closed;

            Entry(String path, long streamId, String contentType, Long ttlSeconds, Instant expiresAt) {
                this.path = path;
                this.streamId = streamId;
                this.contentType = Objects.requireNonNull(contentType);
                this.ttlSeconds = ttlSeconds;
                this.expiresAt = expiresAt;
            }

            boolean matches(String contentType, Long ttlSeconds, Instant expiresAt) {
                return this.contentType.equals(contentType)
                    && Objects.equals(this.ttlSeconds, ttlSeconds)
                    && Objects.equals(this.expiresAt, expiresAt);
            }

            StreamInfo info() {
                return new StreamInfo(path, streamId, contentType, ttlSeconds, expiresAt,
                    OffsetToken.ofRecordOffset(nextOffset), closed, 1);
            }
        }
    }
}
