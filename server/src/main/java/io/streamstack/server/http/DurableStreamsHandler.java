package io.streamstack.server.http;

import io.javalin.http.Context;
import io.streamstack.model.request.AppendRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.request.CreateRequest;
import io.streamstack.model.response.CreateResponse;
import io.streamstack.model.response.HeadResponse;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.Protocol;
import io.streamstack.model.request.ReadRequest;
import io.streamstack.model.response.ReadResponse;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamInfo;
import io.streamstack.server.store.StoreException;
import io.streamstack.server.store.StreamStore;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * HTTP boundary: maps wire {@link io.streamstack.model} types to/from the store SPI.
 */
public final class DurableStreamsHandler {
    private static final String DEFAULT_CT = "application/octet-stream";
    private static final String STRICT_INT = "0|[1-9][0-9]*";

    private final StreamStore store;
    private final Duration longPollTimeout;
    private final Duration sseMaxDuration;
    private final int maxChunkSize;

    public DurableStreamsHandler(StreamStore store) {
        this(store, Duration.ofSeconds(25), Duration.ofSeconds(55), 64 * 1024);
    }

    public DurableStreamsHandler(StreamStore store, Duration longPollTimeout, Duration sseMaxDuration, int maxChunkSize) {
        this.store = Objects.requireNonNull(store, "store");
        this.longPollTimeout = longPollTimeout == null ? Duration.ofSeconds(25) : longPollTimeout;
        this.sseMaxDuration = sseMaxDuration == null ? Duration.ofSeconds(55) : sseMaxDuration;
        this.maxChunkSize = maxChunkSize > 0 ? maxChunkSize : 64 * 1024;
    }

    public void handle(Context ctx) {
        try {
            switch (ctx.method().name()) {
                case "OPTIONS" -> options(ctx);
                case "PUT" -> put(ctx);
                case "POST" -> post(ctx);
                case "DELETE" -> respond(ctx, store.delete(streamUri(ctx)) ? 204 : 404, null, false);
                case "HEAD" -> head(ctx);
                case "GET" -> get(ctx);
                default -> fail(ctx, 405, "method not allowed");
            }
        } catch (StoreException e) {
            switch (e.kind()) {
                case NOT_FOUND -> respond(ctx, 404, null, false);
                case BAD_REQUEST -> fail(ctx, 400, e.getMessage());
                case FENCED -> {
                    secure(ctx);
                    ctx.status(403);
                    ctx.header(Protocol.H_CONTENT_TYPE, "text/plain; charset=utf-8");
                    ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
                    if (e.producerEpoch() != null) {
                        ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(e.producerEpoch()));
                    }
                    ctx.result(e.getMessage() == null ? "Stale producer epoch" : e.getMessage());
                }
                case SEQUENCE_GAP -> {
                    secure(ctx);
                    ctx.status(409);
                    ctx.header(Protocol.H_CONTENT_TYPE, "text/plain; charset=utf-8");
                    ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
                    if (e.expectedSeq() != null) {
                        ctx.header(Protocol.H_PRODUCER_EXPECTED_SEQ, Long.toString(e.expectedSeq()));
                    }
                    if (e.receivedSeq() != null) {
                        ctx.header(Protocol.H_PRODUCER_RECEIVED_SEQ, Long.toString(e.receivedSeq()));
                    }
                    ctx.result(e.getMessage() == null ? "Producer sequence gap" : e.getMessage());
                }
                case CONFLICT, CLOSED -> respond(ctx, 409, e.nextOffset(), e.closed());
            }
        } catch (IllegalArgumentException e) {
            fail(ctx, 400, e.getMessage());
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            String message = root.getMessage() == null ? e.toString() : root.getMessage();
            if (isContentTooLarge(message)) {
                fail(ctx, 413, message);
                return;
            }
            fail(ctx, 500, message);
        }
    }

    private void options(Context ctx) {
        secure(ctx);
        ctx.status(204);
        ctx.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS");
        ctx.header("Access-Control-Allow-Headers",
            "content-type, authorization, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, "
                + "Stream-Closed, Producer-Id, Producer-Epoch, Producer-Seq");
        ctx.header("Access-Control-Expose-Headers",
            "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, Producer-Epoch, "
                + "Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, etag, content-type");
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
    }

    private void put(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        byte[] body = ctx.bodyAsBytes();
        if (body == null) {
            body = new byte[0];
        }
        CreateRequest request = new CreateRequest(
            orDefault(ctx.header(Protocol.H_CONTENT_TYPE), DEFAULT_CT),
            parseLong(ctx.header(Protocol.H_STREAM_TTL), "invalid Stream-TTL"),
            parseInstant(ctx.header(Protocol.H_STREAM_EXPIRES_AT), "invalid Stream-Expires-At"),
            truthy(ctx.header(Protocol.H_STREAM_CLOSED)),
            body);
        if (request.ttlSeconds() != null && request.expiresAt() != null) {
            throw new IllegalArgumentException("Stream-TTL and Stream-Expires-At both set");
        }

        CreateResult out = store.create(
            url,
            request.contentType(),
            request.ttlSeconds(),
            request.expiresAt(),
            request.closed(),
            new ByteArrayInputStream(request.initialBody()));
        CreateResponse response = toCreateResponse(out);
        respond(ctx, response.created() ? 201 : 200, toToken(response.nextOffset()), response.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, response.contentType());
        if (response.created()) {
            ctx.header(Protocol.H_LOCATION, url.toString());
        }
    }

    private void post(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        boolean close = truthy(ctx.header(Protocol.H_STREAM_CLOSED));
        String producerId = ctx.header(Protocol.H_PRODUCER_ID);
        String epochRaw = ctx.header(Protocol.H_PRODUCER_EPOCH);
        String seqRaw = ctx.header(Protocol.H_PRODUCER_SEQ);
        boolean anyProducer = producerId != null || epochRaw != null || seqRaw != null;
        boolean allProducer = producerId != null && epochRaw != null && seqRaw != null;
        if (anyProducer && !allProducer) {
            throw new IllegalArgumentException(
                "All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together");
        }
        Long producerEpoch = null;
        Long producerSeq = null;
        if (allProducer) {
            if (producerId.isEmpty()) {
                throw new IllegalArgumentException("Invalid Producer-Id: must not be empty");
            }
            producerEpoch = parseStrictLong(epochRaw, "Invalid Producer-Epoch: must be a non-negative integer");
            producerSeq = parseStrictLong(seqRaw, "Invalid Producer-Seq: must be a non-negative integer");
        }

        byte[] body = ctx.bodyAsBytes();
        if (body == null) {
            body = new byte[0];
        }
        String contentType = ctx.header(Protocol.H_CONTENT_TYPE);
        if (body.length == 0 && !close) {
            throw new IllegalArgumentException("Empty body");
        }
        if (body.length > 0 && (contentType == null || contentType.isEmpty())) {
            throw new IllegalArgumentException("missing Content-Type");
        }

        AppendRequest request = new AppendRequest(
            contentType,
            body,
            ctx.header(Protocol.H_STREAM_SEQ),
            allProducer ? producerId : null,
            producerEpoch,
            producerSeq,
            close);
        AppendResponse response = toAppendResponse(store.append(url, toCommand(request)));

        int status = allProducer && response.appended() ? 200 : 204;
        respond(ctx, status, toToken(response.nextOffset()), response.closed());
        if (response.producerEpoch() != null) {
            ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(response.producerEpoch()));
        }
        if (response.producerSeq() != null) {
            ctx.header(Protocol.H_PRODUCER_SEQ, Long.toString(response.producerSeq()));
        }
    }

    private void head(Context ctx) throws Exception {
        StreamInfo meta = store.head(streamUri(ctx)).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        HeadResponse response = toHeadResponse(meta);
        respond(ctx, 200, toToken(response.nextOffset()), response.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, response.contentType());
        if (response.ttlSeconds() != null) {
            ctx.header(Protocol.H_STREAM_TTL, Long.toString(response.ttlSeconds()));
        }
        if (response.expiresAt() != null) {
            ctx.header(Protocol.H_STREAM_EXPIRES_AT, response.expiresAt().toString());
        }
    }

    private void get(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        String liveRaw = ctx.queryParam(Protocol.Q_LIVE);
        String offsetRaw = ctx.queryParam(Protocol.Q_OFFSET);
        boolean offsetNow = offsetRaw != null && Offset.NOW.equalsIgnoreCase(offsetRaw);
        LiveMode live = LiveMode.parse(liveRaw);
        OffsetToken offset = parseOffset(url, offsetRaw, live != null);
        ReadRequest request = new ReadRequest(
            toOffset(offset),
            live,
            ctx.queryParam(Protocol.Q_CURSOR),
            maxChunkSize);

        if (request.live() == null) {
            writeRead(ctx, url, offset, toReadResponse(store.read(url, offset, maxChunkSize)), false, offsetNow);
            return;
        }
        switch (request.live()) {
            case LONG_POLL -> longPoll(ctx, url, offset, request.cursor());
            case SSE -> sse(ctx, url, offset, request.cursor());
        }
    }

    private void longPoll(Context ctx, URI url, OffsetToken offset, String cursorRaw) throws Exception {
        ctx.header(Protocol.H_STREAM_CURSOR, Long.toString(cursor(cursorRaw)));
        ReadResponse out = toReadResponse(store.read(url, offset, maxChunkSize));
        if (!(out.messages().isEmpty() && out.upToDate()) || out.closed()) {
            writeRead(ctx, url, offset, out, true, false);
            return;
        }
        if (store.await(url, offset, longPollTimeout)) {
            writeRead(ctx, url, offset, toReadResponse(store.read(url, offset, maxChunkSize)), true, false);
            return;
        }
        StreamInfo meta = store.head(url).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        HeadResponse head = toHeadResponse(meta);
        respond(ctx, 204, toToken(head.nextOffset()), head.closed());
        ctx.header(Protocol.H_STREAM_UP_TO_DATE, Protocol.BOOL_TRUE);
    }

    private void sse(Context ctx, URI url, OffsetToken start, String cursorRaw) throws Exception {
        StreamInfo meta = store.head(url).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        SseEncoder encoder = new SseEncoder(meta.contentType());
        long cursor = cursor(cursorRaw);
        secure(ctx);
        ctx.status(200);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_EVENT_STREAM);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-cache");
        if (encoder.base64()) {
            ctx.header(Protocol.H_STREAM_SSE_DATA_ENCODING, "base64");
        }
        OutputStream out = ctx.res().getOutputStream();
        OffsetToken offset = start;
        boolean announcedCaughtUp = false;
        long deadline = System.nanoTime() + sseMaxDuration.toNanos();
        while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
            ReadResponse read = toReadResponse(store.read(url, offset, maxChunkSize));
            if (!read.messages().isEmpty()) {
                out.write(encoder.dataEvent(read.messages()));
                offset = toToken(read.nextOffset());
                boolean closedAtTail = read.closed() && read.upToDate();
                out.write(encoder.controlEvent(offset.value(), closedAtTail ? null : cursor,
                    read.upToDate(), closedAtTail));
                out.flush();
                if (closedAtTail) {
                    return;
                }
                announcedCaughtUp = read.upToDate();
                continue;
            }
            if (read.closed()) {
                out.write(encoder.controlEvent(read.nextOffset().value(), null, true, true));
                out.flush();
                return;
            }
            if (!announcedCaughtUp) {
                out.write(encoder.controlEvent(read.nextOffset().value(), cursor, true, false));
                out.flush();
                announcedCaughtUp = true;
            }
            store.await(url, offset, Duration.ofSeconds(1));
        }
    }

    private void writeRead(Context ctx, URI url, OffsetToken start, ReadResponse out, boolean live, boolean offsetNow) {
        boolean emptyTail = out.messages().isEmpty() && out.upToDate();
        boolean json = SseEncoder.isJson(SseEncoder.mimeOf(out.contentType()));
        secure(ctx);
        ctx.status(emptyTail && live ? 204 : 200);
        if (live || offsetNow) {
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        } else {
            ctx.header(Protocol.H_CACHE_CONTROL, Protocol.CACHE_CATCH_UP);
        }
        if (out.nextOffset() != null) {
            ctx.header(Protocol.H_STREAM_NEXT_OFFSET, out.nextOffset().value());
        }
        if (out.closed() && out.upToDate()) {
            ctx.header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        }
        if (out.upToDate()) {
            ctx.header(Protocol.H_STREAM_UP_TO_DATE, Protocol.BOOL_TRUE);
        }
        ctx.header(Protocol.H_CONTENT_TYPE, orDefault(out.contentType(), DEFAULT_CT));

        if (!live && !offsetNow) {
            String etag = etag(url.getPath(), start, toToken(out.nextOffset()), out.closed() && emptyTail);
            ctx.header(Protocol.H_ETAG, etag);
            String inm = ctx.header(Protocol.H_IF_NONE_MATCH);
            if (inm != null && inm.equals(etag)) {
                ctx.status(304);
                ctx.result("");
                return;
            }
        }

        if (!(emptyTail && live)) {
            if (json) {
                ctx.result(SseEncoder.jsonArrayBody(out.messages()));
            } else {
                ctx.result(out.concatenated());
            }
        }
    }

    private static AppendCommand toCommand(AppendRequest request) {
        return new AppendCommand(
            request.contentType(),
            request.body(),
            request.streamSeq(),
            request.producerId(),
            request.producerEpoch(),
            request.producerSeq(),
            request.close());
    }

    private static AppendResponse toAppendResponse(AppendResult result) {
        return new AppendResponse(
            toOffset(result.nextOffset()),
            result.appended(),
            result.closed(),
            result.producerEpoch(),
            result.producerSeq());
    }

    private static CreateResponse toCreateResponse(CreateResult result) {
        StreamInfo info = result.info();
        return new CreateResponse(
            result.created(),
            info.contentType(),
            toOffset(info.nextOffset()),
            info.closed());
    }

    private static HeadResponse toHeadResponse(StreamInfo info) {
        return new HeadResponse(
            info.contentType(),
            info.ttlSeconds(),
            info.expiresAt(),
            toOffset(info.nextOffset()),
            info.closed());
    }

    private static ReadResponse toReadResponse(ReadResult result) {
        return new ReadResponse(
            result.messages(),
            result.contentType(),
            toOffset(result.nextOffset()),
            result.upToDate(),
            result.closed());
    }

    private static Offset toOffset(OffsetToken token) {
        return token == null ? null : Offset.of(token.value());
    }

    private static OffsetToken toToken(Offset offset) {
        if (offset == null) {
            return null;
        }
        if (offset.isBeginning()) {
            return OffsetToken.beginning();
        }
        return OffsetToken.parse(offset.value());
    }

    private static String etag(String path, OffsetToken start, OffsetToken end, boolean closedAtTail) {
        String startVal = start == null ? "-1" : start.value();
        String endVal = end == null ? startVal : end.value();
        String encoded = Base64.getEncoder().encodeToString(path.getBytes(StandardCharsets.UTF_8));
        String value = encoded + ":" + startVal + ":" + endVal + (closedAtTail ? ":c" : "");
        return "\"" + value + "\"";
    }

    private static long cursor(String raw) {
        long interval = System.currentTimeMillis() / 20_000L;
        if (raw != null && !raw.isEmpty()) {
            try {
                long client = Long.parseLong(raw);
                if (interval <= client) {
                    interval = client + 1 + ThreadLocalRandom.current().nextLong(60);
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return interval;
    }

    private OffsetToken parseOffset(URI url, String raw, boolean required) throws Exception {
        if (raw == null) {
            if (required) {
                throw new IllegalArgumentException("offset required");
            }
            return OffsetToken.beginning();
        }
        if (Offset.NOW.equalsIgnoreCase(raw)) {
            return store.head(url).map(StreamInfo::nextOffset)
                .orElseThrow(() -> new StoreException(StoreException.Kind.NOT_FOUND));
        }
        return OffsetToken.parse(raw);
    }

    private static URI streamUri(Context ctx) {
        try {
            URI full = URI.create(ctx.fullUrl());
            return new URI(full.getScheme(), full.getAuthority(), full.getPath(), null, null);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid uri");
        }
    }

    private static void respond(Context ctx, int status, OffsetToken next, boolean closed) {
        secure(ctx);
        ctx.status(status);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        if (next != null) {
            ctx.header(Protocol.H_STREAM_NEXT_OFFSET, next.value());
        }
        if (closed) {
            ctx.header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        }
    }

    private static void fail(Context ctx, int status, String message) {
        secure(ctx);
        ctx.status(status);
        ctx.header(Protocol.H_CONTENT_TYPE, "text/plain; charset=utf-8");
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        ctx.result(message == null ? "error" : message);
    }

    private static void secure(Context ctx) {
        ctx.header("X-Content-Type-Options", "nosniff");
        ctx.header("Cross-Origin-Resource-Policy", "same-origin");
    }

    private static boolean isContentTooLarge(String message) {
        if (message == null) {
            return false;
        }
        String lower = message.toLowerCase();
        return lower.contains("content too large")
            || lower.contains("request entity too large")
            || lower.contains("payload too large");
    }

    private static boolean truthy(String value) {
        return Protocol.BOOL_TRUE.equalsIgnoreCase(value);
    }

    private static String orDefault(String value, String fallback) {
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static Long parseLong(String raw, String error) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        if (!raw.matches(STRICT_INT)) {
            throw new IllegalArgumentException(error);
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(error);
        }
    }

    private static long parseStrictLong(String raw, String error) {
        if (raw == null || !raw.matches("\\d+")) {
            throw new IllegalArgumentException(error);
        }
        try {
            long value = Long.parseLong(raw);
            if (value < 0) {
                throw new IllegalArgumentException(error);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(error);
        }
    }

    private static Instant parseInstant(String raw, String error) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException(error);
        }
    }
}
