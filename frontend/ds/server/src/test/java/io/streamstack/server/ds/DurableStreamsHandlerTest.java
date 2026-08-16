package io.streamstack.server.ds;

import io.javalin.Javalin;
import io.streamstack.model.Protocol;
import io.streamstack.server.service.AppendService;
import io.streamstack.server.service.OwnershipService;
import io.streamstack.server.service.ReadService;
import io.streamstack.server.service.StreamLifecycleService;
import io.streamstack.server.model.StreamServiceException;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.CloseResult;
import io.streamstack.server.model.StreamList;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamRecord;
import io.streamstack.server.StreamWaiterRegistry;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurableStreamsHandlerTest {

    @Test
    void createAppendReadCloseMatrix() throws Exception {
        InMemoryServices mem = new InMemoryServices();
        DurableStreamsHandler handler = new DurableStreamsHandler(
            mem.service(), Duration.ofMillis(50), Duration.ofSeconds(1), 1024);
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);

        app.get("/*", handler::handle);
        app.post("/*", handler::handle);
        app.put("/*", handler::handle);
        app.start(0);

        try {
            String base = "http://127.0.0.1:" + app.port() + "/streams/demo";
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> created = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(201, created.statusCode());
            assertTrue(created.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).isPresent());
            HttpResponse<String> idempotent = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/octet-stream")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, idempotent.statusCode());
            HttpResponse<String> conflict = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(409, conflict.statusCode());
            HttpResponse<String> appended = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofString("hello")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, appended.statusCode());
            assertEquals("application/octet-stream", appended.headers().firstValue("Content-Type").orElse(""));
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
            HttpResponse<String> closedAgain = client.send(
                HttpRequest.newBuilder(URI.create(base)).header(Protocol.H_STREAM_CLOSED, "true")
                    .POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, closedAgain.statusCode());
            assertEquals("true", closedAgain.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(""));
            HttpResponse<String> rejected = client.send(
                HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "application/octet-stream")
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
        InMemoryServices mem = new InMemoryServices();
        DurableStreamsHandler handler = new DurableStreamsHandler(
            mem.service(), Duration.ofSeconds(5), Duration.ofSeconds(1), 1024);
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

    @Test
    void producerAcceptReturns200AndDuplicateReturns204() throws Exception {
        InMemoryServices mem = new InMemoryServices();
        DurableStreamsHandler handler = new DurableStreamsHandler(
            mem.service(), Duration.ofMillis(50), Duration.ofSeconds(1), 1024);
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);

        app.get("/*", handler::handle);
        app.post("/*", handler::handle);
        app.put("/*", handler::handle);
        app.start(0);

        try {
            String base = "http://127.0.0.1:" + app.port() + "/streams/producer";
            HttpClient client = HttpClient.newHttpClient();

            client.send(HttpRequest.newBuilder(URI.create(base)).header("Content-Type", "text/plain")
                .PUT(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            HttpResponse<String> first = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "text/plain")
                    .header(Protocol.H_PRODUCER_ID, "p1")
                    .header(Protocol.H_PRODUCER_EPOCH, "0")
                    .header(Protocol.H_PRODUCER_SEQ, "0")
                    .POST(HttpRequest.BodyPublishers.ofString("a")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, first.statusCode());
            assertEquals("0", first.headers().firstValue(Protocol.H_PRODUCER_SEQ).orElse(""));
            HttpResponse<String> dup = client.send(
                HttpRequest.newBuilder(URI.create(base))
                    .header("Content-Type", "text/plain")
                    .header(Protocol.H_PRODUCER_ID, "p1")
                    .header(Protocol.H_PRODUCER_EPOCH, "0")
                    .header(Protocol.H_PRODUCER_SEQ, "0")
                    .POST(HttpRequest.BodyPublishers.ofString("a")).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(204, dup.statusCode());
            assertEquals("0", dup.headers().firstValue(Protocol.H_PRODUCER_SEQ).orElse(""));
        } finally {
            app.stop();
        }
    }

    static final class InMemoryServices
        implements StreamLifecycleService, AppendService, ReadService, OwnershipService {
        private final ConcurrentHashMap<String, Entry> streams = new ConcurrentHashMap<>();
        private final StreamWaiterRegistry waiters = new StreamWaiterRegistry();
        private final AtomicLong ids = new AtomicLong();
        StreamService service() {
            return new StreamService(this, this, this, this);
        }

        @Override
        public CreateResult create(CreateCommand command) throws StreamServiceException {
            Entry existing = streams.get(command.name());

            if (Objects.nonNull(existing)) {
                if (!existing.matches(command.contentType(), command.ttlSeconds(),
                    command.expiresAt(), command.closed())) {
                    throw new StreamServiceException(StreamServiceException.Kind.CONFLICT);
                }

                return new CreateResult(false, existing.meta());
            }

            Entry created = new Entry(command.name(), ids.incrementAndGet(), command.contentType(),
                command.ttlSeconds(), command.expiresAt(), command.closed());
            streams.put(command.name(), created);

            return new CreateResult(true, created.meta());
        }

        @Override
        public Optional<StreamMeta> head(String name) {
            Entry entry = streams.get(name);
            return Objects.isNull(entry) ? Optional.empty() : Optional.of(entry.meta());
        }

        @Override
        public StreamList list(String prefix, String startAfter, int limit) {
            int max = limit > 0 ? limit : Integer.MAX_VALUE;
            List<StreamMeta> out = streams.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .filter(e -> Objects.isNull(startAfter) || e.getKey().compareTo(startAfter) > 0)
                .sorted(Map.Entry.comparingByKey())
                .limit(max)
                .map(e -> e.getValue().meta())
                .toList();

            return new StreamList(out, false);
        }

        @Override
        public CloseResult close(String name) throws StreamServiceException {
            return new CloseResult(append(new AppendCommand(name, List.of(), null, null, null, true)).nextOffset());
        }

        @Override
        public boolean delete(String name) {
            Entry removed = streams.remove(name);

            if (Objects.nonNull(removed)) {
                waiters.notifyClosed(removed.name);
            }

            return Objects.nonNull(removed);
        }

        @Override
        public long trim(String name, long newStartOffset) throws StreamServiceException {
            Entry entry = require(name);
            long clamped = Math.min(Math.max(newStartOffset, entry.startOffset), entry.nextOffset);

            for (long offset = entry.startOffset; offset < clamped; offset++) {
                entry.chunks.remove(offset);
            }

            entry.startOffset = clamped;

            return entry.startOffset;
        }

        @Override
        public AppendResult append(AppendCommand command) throws StreamServiceException {
            Entry entry = require(command.name());
            OffsetToken next = OffsetToken.ofRecordOffset(entry.nextOffset);

            if (command.hasProducer()) {
                ProducerState state = entry.producers.get(command.producer().producerId());
                long epoch = command.producer().epoch();
                long seq = command.producer().seq();

                if (Objects.isNull(state)) {
                    if (seq != 0L) {
                        throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST, next, false,
                            "New epoch must start with sequence 0");
                    }
                } else if (epoch < state.epoch) {
                    throw StreamServiceException.fenced(state.epoch);
                } else if (epoch > state.epoch) {
                    if (seq != 0L) {
                        throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST, next, false,
                            "New epoch must start with sequence 0");
                    }
                } else if (seq <= state.lastSeq) {
                    return new AppendResult(next, false, entry.closed, epoch, state.lastSeq);
                } else if (seq != state.lastSeq + 1) {
                    throw StreamServiceException.sequenceGap(state.lastSeq + 1, seq);
                }
            }

            byte[] body = command.concatenatedPayload();

            if (entry.closed) {
                if (command.closeAfter() && body.length == 0) {
                    return new AppendResult(next, false, true,
                        command.hasProducer() ? command.producer().epoch() : null,
                        command.hasProducer() ? command.producer().seq() : null);
                }

                throw new StreamServiceException(StreamServiceException.Kind.CLOSED, next, true);
            }

            boolean closeOnly = body.length == 0 && command.closeAfter();

            if (!closeOnly) {
                if (Objects.isNull(command.contentType()) || !entry.contentType.equals(command.contentType())) {
                    throw new StreamServiceException(StreamServiceException.Kind.CONFLICT, next, false);
                }

                if (body.length == 0) {
                    throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST);
                }

                if (Objects.nonNull(command.streamSeq()) && Objects.nonNull(entry.lastSeq)
                    && command.streamSeq().compareTo(entry.lastSeq) <= 0) {
                    throw new StreamServiceException(
                        StreamServiceException.Kind.CONFLICT, next, false, "Sequence conflict");
                }

                entry.chunks.put(entry.nextOffset, body);
                entry.nextOffset += 1;
                waiters.notifyAppend(entry.name, entry.nextOffset);
                next = OffsetToken.ofRecordOffset(entry.nextOffset);

                if (Objects.nonNull(command.streamSeq())) {
                    entry.lastSeq = command.streamSeq();
                }
            }

            if (command.hasProducer()) {
                entry.producers.put(command.producer().producerId(),
                    new ProducerState(command.producer().epoch(), command.producer().seq()));
            }

            if (command.closeAfter()) {
                entry.closed = true;
                waiters.notifyClosed(entry.name);
            }

            return new AppendResult(next, !closeOnly, entry.closed,
                command.hasProducer() ? command.producer().epoch() : null,
                command.hasProducer() ? command.producer().seq() : null);
        }

        @Override
        public ReadResult read(String name, OffsetToken from, int maxBytes, int maxRecords)
            throws StreamServiceException {
            Entry entry = require(name);
            long start = from.recordOffset();

            if (start > entry.nextOffset) {
                throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST);
            }

            if (start == entry.nextOffset) {
                return new ReadResult(List.of(), entry.contentType, OffsetToken.ofRecordOffset(entry.nextOffset), true,
                    entry.closed);
            }

            byte[] body = entry.chunks.getOrDefault(start, new byte[0]);
            long next = start + 1;

            return new ReadResult(
                List.of(new StreamRecord(OffsetToken.ofRecordOffset(start), body)),
                entry.contentType,
                OffsetToken.ofRecordOffset(next),
                next >= entry.nextOffset,
                entry.closed);
        }

        @Override
        public boolean await(String name, OffsetToken from, Duration timeout) throws StreamServiceException {
            try {
                Entry entry = require(name);

                if (entry.closed || entry.nextOffset > from.recordOffset()) {
                    return true;
                }

                return waiters.await(entry.name, from, timeout);
            } catch (StreamServiceException e) {
                throw e;
            } catch (Exception e) {
                throw new StreamServiceException(StreamServiceException.Kind.BAD_REQUEST, null, false, e.getMessage());
            }
        }

        @Override
        public CompletableFuture<Boolean> whenAppended(String name, OffsetToken from, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return await(name, from, timeout);
                } catch (StreamServiceException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public Owner ownerOf(String name) {
            Entry entry = streams.get(name);
            return Owner.local(Objects.isNull(entry) ? OptionalLong.empty() : OptionalLong.of(entry.streamId));
        }

        @Override
        public NodeMeta localNode() {
            return new NodeMeta(1, "http://127.0.0.1:0");
        }

        private Entry require(String name) throws StreamServiceException {
            Entry entry = streams.get(name);

            if (Objects.isNull(entry)) {
                throw new StreamServiceException(StreamServiceException.Kind.NOT_FOUND);
            }

            return entry;
        }

        static final class ProducerState {
            final long epoch;
            final long lastSeq;

            ProducerState(long epoch, long lastSeq) {
                this.epoch = epoch;
                this.lastSeq = lastSeq;
            }
        }

        static final class Entry {
            final String name;
            final long streamId;
            final String contentType;
            final Long ttlSeconds;
            final Instant expiresAt;

            final LinkedHashMap<Long, byte[]> chunks = new LinkedHashMap<>();
            final Map<String, ProducerState> producers = new LinkedHashMap<>();
            long startOffset;
            long nextOffset;
            boolean closed;
            String lastSeq;

            Entry(String name, long streamId, String contentType, Long ttlSeconds, Instant expiresAt, boolean closed) {
                this.name = name;
                this.streamId = streamId;
                this.contentType = Objects.requireNonNull(contentType);
                this.ttlSeconds = ttlSeconds;
                this.expiresAt = expiresAt;
                this.closed = closed;
            }

            boolean matches(String contentType, Long ttlSeconds, Instant expiresAt, boolean closed) {
                return this.contentType.equals(contentType)
                    && Objects.equals(this.ttlSeconds, ttlSeconds)
                    && Objects.equals(this.expiresAt, expiresAt)
                    && this.closed == closed;
            }

            StreamMeta meta() {
                return new StreamMeta(name, streamId, contentType, ttlSeconds, expiresAt,
                    OffsetToken.ofRecordOffset(startOffset), OffsetToken.ofRecordOffset(nextOffset),
                    OffsetToken.ofRecordOffset(nextOffset), closed);
            }
        }
    }
}
