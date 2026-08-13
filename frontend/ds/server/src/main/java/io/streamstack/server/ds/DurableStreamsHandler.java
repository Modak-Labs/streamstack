package io.streamstack.server.ds;

import io.javalin.http.Context;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.Protocol;
import io.streamstack.model.request.AppendRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.CreateResponse;
import io.streamstack.model.response.HeadResponse;
import io.streamstack.model.response.ReadResponse;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.StreamServiceException;

import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class DurableStreamsHandler {

    private static final String DEFAULT_CT = "application/octet-stream";
    private static final String STRICT_INT = "0|[1-9][0-9]*";
    private final StreamService service;
    private final Duration longPollTimeout;
    private final Duration sseMaxDuration;
    private final int maxChunkSize;

    public DurableStreamsHandler(StreamService service) {
        this(service, Duration.ofSeconds(25), Duration.ofSeconds(55), 64 * 1024);
    }

    public DurableStreamsHandler(
        StreamService service,
        Duration longPollTimeout,
        Duration sseMaxDuration,
        int maxChunkSize) {
        this.service = Objects.requireNonNull(service, "service");
        this.longPollTimeout = Objects.isNull(longPollTimeout) ? Duration.ofSeconds(25) : longPollTimeout;
        this.sseMaxDuration = Objects.isNull(sseMaxDuration) ? Duration.ofSeconds(55) : sseMaxDuration;
        this.maxChunkSize = maxChunkSize > 0 ? maxChunkSize : 64 * 1024;
    }

    public void handle(Context ctx) {
        try {
            switch (ctx.method().name()) {
                case "OPTIONS" -> options(ctx);
                case "PUT" -> put(ctx);
                case "POST" -> post(ctx);
                case "DELETE" -> respond(ctx, service.lifecycle().delete(streamName(ctx)) ? 204 : 404, null, false);
                case "HEAD" -> head(ctx);
                case "GET" -> get(ctx);
                default -> fail(ctx, 405, "method not allowed");
            }
        } catch (StreamServiceException e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            switch (e.kind()) {
                case NOT_FOUND -> respond(ctx, 404, null, false);
                case BAD_REQUEST -> fail(ctx, 400, e.getMessage());
                case FENCED -> {
                    secure(ctx);
                    ctx.status(403);
                    ctx.header(Protocol.H_CONTENT_TYPE, "text/plain; charset=utf-8");
                    ctx.header(Protocol.H_CACHE_CONTROL, "no-store");

                    if (Objects.nonNull(e.producerEpoch())) {
                        ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(e.producerEpoch()));
                    }

                    ctx.result(Objects.isNull(e.getMessage()) ? "Stale producer epoch" : e.getMessage());
                }
                case SEQUENCE_GAP -> {
                    secure(ctx);
                    ctx.status(409);
                    ctx.header(Protocol.H_CONTENT_TYPE, "text/plain; charset=utf-8");
                    ctx.header(Protocol.H_CACHE_CONTROL, "no-store");

                    if (Objects.nonNull(e.expectedSeq())) {
                        ctx.header(Protocol.H_PRODUCER_EXPECTED_SEQ, Long.toString(e.expectedSeq()));
                    }

                    if (Objects.nonNull(e.receivedSeq())) {
                        ctx.header(Protocol.H_PRODUCER_RECEIVED_SEQ, Long.toString(e.receivedSeq()));
                    }

                    ctx.result(Objects.isNull(e.getMessage()) ? "Producer sequence gap" : e.getMessage());
                }
                case CONFLICT, CLOSED -> respond(ctx, 409, e.nextOffset(), e.closed());
            }
        } catch (IllegalArgumentException e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            fail(ctx, 400, e.getMessage());
        } catch (Exception e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            Throwable root = e;

            while (Objects.nonNull(root.getCause()) && root.getCause() != root) {
                root = root.getCause();
            }

            String message = Objects.isNull(root.getMessage()) ? e.toString() : root.getMessage();

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
        String name = streamName(ctx);
        byte[] body = ctx.bodyAsBytes();

        if (Objects.isNull(body)) {
            body = new byte[0];
        }

        Long ttlSeconds = parseLong(ctx.header(Protocol.H_STREAM_TTL), "invalid Stream-TTL");
        Instant expiresAt = parseInstant(ctx.header(Protocol.H_STREAM_EXPIRES_AT), "invalid Stream-Expires-At");

        if (Objects.nonNull(ttlSeconds) && Objects.nonNull(expiresAt)) {
            throw new IllegalArgumentException("Stream-TTL and Stream-Expires-At both set");
        }

        CreateResponse response = ProtocolConverter.toCreateResponse(service.lifecycle().create(
            ProtocolConverter.toCreateCommand(
                name,
                orDefault(ctx.header(Protocol.H_CONTENT_TYPE), DEFAULT_CT),
                ttlSeconds,
                expiresAt,
                truthy(ctx.header(Protocol.H_STREAM_CLOSED)),
                body)));
        respond(ctx, response.created() ? 201 : 200, ProtocolConverter.toToken(response.nextOffset()), response.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, response.contentType());

        if (response.created()) {
            ctx.header(Protocol.H_LOCATION, streamUri(ctx).toString());
        }
    }

    private void post(Context ctx) throws Exception {
        String name = streamName(ctx);
        boolean close = truthy(ctx.header(Protocol.H_STREAM_CLOSED));
        String producerId = ctx.header(Protocol.H_PRODUCER_ID);
        String epochRaw = ctx.header(Protocol.H_PRODUCER_EPOCH);
        String seqRaw = ctx.header(Protocol.H_PRODUCER_SEQ);
        boolean anyProducer = Objects.nonNull(producerId) || Objects.nonNull(epochRaw) || Objects.nonNull(seqRaw);
        boolean allProducer = Objects.nonNull(producerId) && Objects.nonNull(epochRaw) && Objects.nonNull(seqRaw);

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

        if (Objects.isNull(body)) {
            body = new byte[0];
        }

        String contentType = ctx.header(Protocol.H_CONTENT_TYPE);

        if (body.length == 0 && !close) {
            throw new IllegalArgumentException("Empty body");
        }

        if (body.length > 0 && (Objects.isNull(contentType) || contentType.isEmpty())) {
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
        AppendResponse response = ProtocolConverter.toAppendResponse(
            service.append().append(ProtocolConverter.toAppendCommand(name, request)));
        boolean producerAppended = allProducer && response.appended();

        respond(ctx, producerAppended ? 200 : 204, ProtocolConverter.toToken(response.nextOffset()), response.closed());

        if (Objects.nonNull(contentType) && !contentType.isEmpty()) {
            ctx.header(Protocol.H_CONTENT_TYPE, contentType);
        }

        if (Objects.nonNull(response.producerEpoch())) {
            ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(response.producerEpoch()));
        }

        if (Objects.nonNull(response.producerSeq())) {
            ctx.header(Protocol.H_PRODUCER_SEQ, Long.toString(response.producerSeq()));
        }
    }

    private void head(Context ctx) throws Exception {
        StreamMeta meta = service.lifecycle().head(streamName(ctx)).orElse(null);

        if (Objects.isNull(meta)) {
            respond(ctx, 404, null, false);
            return;
        }

        HeadResponse response = ProtocolConverter.toHeadResponse(meta);

        respond(ctx, 200, ProtocolConverter.toToken(response.nextOffset()), response.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, response.contentType());

        if (Objects.nonNull(response.ttlSeconds())) {
            ctx.header(Protocol.H_STREAM_TTL, Long.toString(response.ttlSeconds()));
        }

        if (Objects.nonNull(response.expiresAt())) {
            ctx.header(Protocol.H_STREAM_EXPIRES_AT, response.expiresAt().toString());
        }
    }

    private void get(Context ctx) throws Exception {
        String name = streamName(ctx);
        String liveRaw = ctx.queryParam(Protocol.Q_LIVE);
        String offsetRaw = ctx.queryParam(Protocol.Q_OFFSET);
        boolean offsetNow = Objects.nonNull(offsetRaw) && Offset.NOW.equalsIgnoreCase(offsetRaw);
        LiveMode live = LiveMode.parse(liveRaw);
        OffsetToken offset = parseOffset(name, offsetRaw, Objects.nonNull(live));
        String cursor = ctx.queryParam(Protocol.Q_CURSOR);

        if (Objects.isNull(live)) {
            writeRead(ctx, name, offset,
                ProtocolConverter.toReadResponse(service.read().read(name, offset, maxChunkSize, 0)),
                false, offsetNow);
            return;
        }

        switch (live) {
            case LONG_POLL -> longPoll(ctx, name, offset, cursor);
            case SSE -> sse(ctx, name, offset, cursor);
        }
    }

    private void longPoll(Context ctx, String name, OffsetToken offset, String cursorRaw) throws Exception {
        ctx.header(Protocol.H_STREAM_CURSOR, Long.toString(cursor(cursorRaw)));
        ReadResponse out = ProtocolConverter.toReadResponse(service.read().read(name, offset, maxChunkSize, 0));

        if (!(out.messages().isEmpty() && out.upToDate()) || out.closed()) {
            writeRead(ctx, name, offset, out, true, false);
            return;
        }

        if (service.read().await(name, offset, longPollTimeout)) {
            writeRead(ctx, name, offset,
                ProtocolConverter.toReadResponse(service.read().read(name, offset, maxChunkSize, 0)), true, false);
            return;
        }

        StreamMeta meta = service.lifecycle().head(name).orElse(null);

        if (Objects.isNull(meta)) {
            respond(ctx, 404, null, false);
            return;
        }

        HeadResponse head = ProtocolConverter.toHeadResponse(meta);

        respond(ctx, 204, ProtocolConverter.toToken(head.nextOffset()), head.closed());
        ctx.header(Protocol.H_STREAM_UP_TO_DATE, Protocol.BOOL_TRUE);
    }

    private void sse(Context ctx, String name, OffsetToken start, String cursorRaw) throws Exception {
        StreamMeta meta = service.lifecycle().head(name).orElse(null);

        if (Objects.isNull(meta)) {
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
            ReadResponse read = ProtocolConverter.toReadResponse(service.read().read(name, offset, maxChunkSize, 0));

            if (!read.messages().isEmpty()) {
                out.write(encoder.dataEvent(read.messages()));
                offset = ProtocolConverter.toToken(read.nextOffset());
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

            service.read().await(name, offset, Duration.ofSeconds(1));
        }
    }

    private void writeRead(
        Context ctx,
        String name,
        OffsetToken start,
        ReadResponse out,
        boolean live,
        boolean offsetNow) {
        boolean emptyTail = out.messages().isEmpty() && out.upToDate();
        boolean json = SseEncoder.isJson(SseEncoder.mimeOf(out.contentType()));

        secure(ctx);
        ctx.status(emptyTail && live ? 204 : 200);

        if (live || offsetNow) {
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        } else {
            ctx.header(Protocol.H_CACHE_CONTROL, Protocol.CACHE_CATCH_UP);
        }

        if (Objects.nonNull(out.nextOffset())) {
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
            String etag = etag(name, start, ProtocolConverter.toToken(out.nextOffset()), out.closed() && emptyTail);

            ctx.header(Protocol.H_ETAG, etag);
            String inm = ctx.header(Protocol.H_IF_NONE_MATCH);

            if (Objects.nonNull(inm) && inm.equals(etag)) {
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

    private static String etag(String path, OffsetToken start, OffsetToken end, boolean closedAtTail) {
        String startVal = Objects.isNull(start) ? "-1" : start.value();
        String endVal = Objects.isNull(end) ? startVal : end.value();
        String encoded = Base64.getEncoder().encodeToString(path.getBytes(StandardCharsets.UTF_8));
        String value = encoded + ":" + startVal + ":" + endVal + (closedAtTail ? ":c" : "");

        return "\"" + value + "\"";
    }

    private static long cursor(String raw) {
        long interval = System.currentTimeMillis() / 20_000L;

        if (Objects.nonNull(raw) && !raw.isEmpty()) {
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

    private OffsetToken parseOffset(String name, String raw, boolean required) throws Exception {
        if (Objects.isNull(raw)) {
            if (required) {
                throw new IllegalArgumentException("offset required");
            }

            return OffsetToken.beginning();
        }

        if (Offset.NOW.equalsIgnoreCase(raw)) {
            return service.lifecycle().head(name).map(StreamMeta::nextOffset)
                .orElseThrow(() -> new StreamServiceException(StreamServiceException.Kind.NOT_FOUND));
        }

        return OffsetToken.parse(raw);
    }

    private static String streamName(Context ctx) {
        String path = ctx.path();
        return Objects.isNull(path) || path.isEmpty() ? "/" : path;
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

        if (Objects.nonNull(next)) {
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
        ctx.result(Objects.isNull(message) ? "error" : message);
    }

    private static void secure(Context ctx) {
        ctx.header("X-Content-Type-Options", "nosniff");
        ctx.header("Cross-Origin-Resource-Policy", "same-origin");
    }

    private static boolean abortIfCommitted(Context ctx) {
        if (!ctx.res().isCommitted()) {
            return false;
        }

        try {
            ctx.res().getOutputStream().close();
        } catch (Exception ignored) {
        }

        return true;
    }

    private static boolean isContentTooLarge(String message) {
        if (Objects.isNull(message)) {
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
        return Objects.isNull(value) || value.isEmpty() ? fallback : value;
    }

    private static Long parseLong(String raw, String error) {
        if (Objects.isNull(raw) || raw.isEmpty()) {
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
        if (Objects.isNull(raw) || !raw.matches("\\d+")) {
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
        if (Objects.isNull(raw) || raw.isEmpty()) {
            return null;
        }

        try {
            return Instant.parse(raw);
        } catch (Exception e) {
            throw new IllegalArgumentException(error);
        }
    }
}
