package io.streamstack.server.http;

import io.javalin.http.Context;
import io.streamstack.server.store.CreateResult;
import io.streamstack.server.store.OffsetToken;
import io.streamstack.server.store.ReadResult;
import io.streamstack.server.store.StoreException;
import io.streamstack.server.store.StreamInfo;
import io.streamstack.server.store.StreamStore;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class DurableStreamsHandler {
    private static final String DEFAULT_CT = "application/octet-stream";

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
                case CONFLICT, CLOSED -> respond(ctx, 409, e.nextOffset(), e.closed());
            }
        } catch (IllegalArgumentException e) {
            fail(ctx, 400, e.getMessage());
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            fail(ctx, 500, root.getMessage() == null ? e.toString() : root.getMessage());
        }
    }

    private void put(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        String contentType = orDefault(ctx.header(Protocol.H_CONTENT_TYPE), DEFAULT_CT);
        Long ttl = parseLong(ctx.header(Protocol.H_STREAM_TTL), "invalid Stream-TTL");
        Instant expiresAt = parseInstant(ctx.header(Protocol.H_STREAM_EXPIRES_AT), "invalid Stream-Expires-At");
        if (ttl != null && expiresAt != null) {
            throw new IllegalArgumentException("Stream-TTL and Stream-Expires-At both set");
        }

        CreateResult out = store.create(url, contentType, ttl, expiresAt, ctx.bodyInputStream());
        OffsetToken next = out.info().nextOffset();
        boolean closed = out.info().closed();
        if (out.created() && truthy(ctx.header(Protocol.H_STREAM_CLOSED))) {
            try {
                next = store.close(url);
            } catch (StoreException e) {
                if (e.kind() != StoreException.Kind.CLOSED) {
                    throw e;
                }
                next = e.nextOffset() != null ? e.nextOffset() : next;
            }
            closed = true;
        }

        respond(ctx, out.created() ? 201 : 200, next, closed);
        ctx.header(Protocol.H_CONTENT_TYPE, out.info().contentType());
        if (out.created()) {
            ctx.header(Protocol.H_LOCATION, url.toString());
        }
    }

    private void post(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        if (truthy(ctx.header(Protocol.H_STREAM_CLOSED))) {
            close(ctx, url);
            return;
        }
        String contentType = ctx.header(Protocol.H_CONTENT_TYPE);
        if (contentType == null || contentType.isEmpty()) {
            throw new IllegalArgumentException("missing Content-Type");
        }
        respond(ctx, 204, store.append(url, contentType, ctx.bodyInputStream()), false);
    }

    private void close(Context ctx, URI url) throws Exception {
        byte[] body = ctx.bodyAsBytes();
        if (body != null && body.length > 0) {
            String ct = ctx.header(Protocol.H_CONTENT_TYPE);
            if (ct == null || ct.isEmpty()) {
                ct = store.head(url).map(StreamInfo::contentType).orElse(null);
            }
            if (ct != null) {
                store.append(url, ct, new ByteArrayInputStream(body));
            }
        }
        try {
            respond(ctx, 204, store.close(url), true);
        } catch (StoreException e) {
            if (e.kind() == StoreException.Kind.CLOSED) {
                respond(ctx, 204, e.nextOffset(), true);
                return;
            }
            throw e;
        }
    }

    private void head(Context ctx) throws Exception {
        StreamInfo meta = store.head(streamUri(ctx)).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        respond(ctx, 200, meta.nextOffset(), meta.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, meta.contentType());
        if (meta.ttlSeconds() != null) {
            ctx.header(Protocol.H_STREAM_TTL, Long.toString(meta.ttlSeconds()));
        }
        if (meta.expiresAt() != null) {
            ctx.header(Protocol.H_STREAM_EXPIRES_AT, meta.expiresAt().toString());
        }
    }

    private void get(Context ctx) throws Exception {
        URI url = streamUri(ctx);
        String live = ctx.queryParam(Protocol.Q_LIVE);
        OffsetToken offset = parseOffset(url, ctx.queryParam(Protocol.Q_OFFSET), live != null);
        if (live == null || live.isEmpty()) {
            writeRead(ctx, store.read(url, offset, maxChunkSize), false);
            return;
        }
        switch (live) {
            case Protocol.LIVE_LONG_POLL -> longPoll(ctx, url, offset);
            case Protocol.LIVE_SSE -> sse(ctx, url, offset);
            default -> throw new IllegalArgumentException("invalid live mode");
        }
    }

    private void longPoll(Context ctx, URI url, OffsetToken offset) throws Exception {
        ctx.header(Protocol.H_STREAM_CURSOR, Long.toString(cursor(ctx.queryParam(Protocol.Q_CURSOR))));
        ReadResult out = store.read(url, offset, maxChunkSize);
        if (!(out.body().length == 0 && out.upToDate()) || out.closed()) {
            writeRead(ctx, out, true);
            return;
        }
        if (store.await(url, offset, longPollTimeout)) {
            writeRead(ctx, store.read(url, offset, maxChunkSize), true);
            return;
        }
        StreamInfo meta = store.head(url).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        respond(ctx, 204, meta.nextOffset(), meta.closed());
        ctx.header(Protocol.H_STREAM_UP_TO_DATE, Protocol.BOOL_TRUE);
    }

    private void sse(Context ctx, URI url, OffsetToken start) throws Exception {
        StreamInfo meta = store.head(url).orElse(null);
        if (meta == null) {
            respond(ctx, 404, null, false);
            return;
        }
        SseEncoder encoder = new SseEncoder(meta.contentType());
        long cursor = cursor(ctx.queryParam(Protocol.Q_CURSOR));
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
            ReadResult read = store.read(url, offset, maxChunkSize);
            if (read.body().length > 0) {
                out.write(encoder.dataEvent(read.body()));
                offset = read.nextOffset();
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

    private void writeRead(Context ctx, ReadResult out, boolean live) {
        boolean emptyTail = out.body().length == 0 && out.upToDate();
        respond(ctx, emptyTail && live ? 204 : 200, out.nextOffset(), out.closed());
        if (!(emptyTail && live)) {
            if (out.body().length == 0 && SseEncoder.isJson(SseEncoder.mimeOf(out.contentType()))) {
                ctx.result("[]");
            } else {
                ctx.result(out.body());
            }
        }
        ctx.header(Protocol.H_CONTENT_TYPE, orDefault(out.contentType(), DEFAULT_CT));
        if (out.upToDate()) {
            ctx.header(Protocol.H_STREAM_UP_TO_DATE, Protocol.BOOL_TRUE);
        }
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
        if ("now".equalsIgnoreCase(raw)) {
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
        if (!raw.matches("0|[1-9][0-9]*")) {
            throw new IllegalArgumentException(error);
        }
        try {
            return Long.parseLong(raw);
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
