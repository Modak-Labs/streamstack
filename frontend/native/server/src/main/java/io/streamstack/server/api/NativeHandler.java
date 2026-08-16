package io.streamstack.server.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.http.Context;
import io.streamstack.model.BatchCodec;
import io.streamstack.model.JsonCodec;
import io.streamstack.model.Protocol;
import io.streamstack.model.RecordEnvelope;
import io.streamstack.model.RecordEnvelopeCodec;
import io.streamstack.model.SequencedRecord;
import io.streamstack.server.ContentTypes;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.Producer;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamList;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.StreamRecord;
import io.streamstack.server.model.StreamServiceException;
import io.streamstack.server.service.StreamService;

import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class NativeHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String STRICT_INT = "0|[1-9][0-9]*";
    private static final int MAX_READ_BYTES = 4 * 1024 * 1024;
    private final StreamService service;
    private final StreamTimestamps timestamps;
    private final Duration longPollTimeout;
    private final Duration sseMaxDuration;
    private final int maxChunkSize;

    public NativeHandler(StreamService service) {
        this(service, Duration.ofSeconds(25), Duration.ofSeconds(55), 64 * 1024);
    }

    public NativeHandler(
        StreamService service,
        Duration longPollTimeout,
        Duration sseMaxDuration,
        int maxChunkSize) {
        this.service = Objects.requireNonNull(service, "service");
        this.timestamps = new StreamTimestamps(service);
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
                case "DELETE" -> delete(ctx);
                case "HEAD" -> head(ctx);
                case "GET" -> get(ctx);
                default -> error(ctx, 405, "method_not_allowed", "method not allowed", null);
            }
        } catch (StreamServiceException e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            handleServiceError(ctx, e);
        } catch (IllegalArgumentException e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            error(ctx, 400, "bad_request", e.getMessage(), null);
        } catch (Exception e) {
            if (abortIfCommitted(ctx)) {
                return;
            }

            Throwable root = e;

            while (Objects.nonNull(root.getCause()) && root.getCause() != root) {
                root = root.getCause();
            }

            String message = Objects.isNull(root.getMessage()) ? e.toString() : root.getMessage();

            error(ctx, isContentTooLarge(message) ? 413 : 500, "internal", message, null);
        }
    }

    private void handleServiceError(Context ctx, StreamServiceException e) {
        switch (e.kind()) {
            case NOT_FOUND -> error(ctx, 404, "not_found", "no such stream", null);
            case BAD_REQUEST -> error(ctx, 400, "bad_request", e.getMessage(), null);
            case FENCED -> {
                if (Objects.nonNull(e.producerEpoch())) {
                    ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(e.producerEpoch()));
                }

                error(ctx, 403, "fenced", e.getMessage(), null);
            }
            case SEQUENCE_GAP -> {
                if (Objects.nonNull(e.expectedSeq())) {
                    ctx.header(Protocol.H_EXPECTED_SEQ, Long.toString(e.expectedSeq()));
                }

                if (Objects.nonNull(e.receivedSeq())) {
                    ctx.header(Protocol.H_RECEIVED_SEQ, Long.toString(e.receivedSeq()));
                }

                error(ctx, 409, "sequence_gap", e.getMessage(), null);
            }
            case MATCH_FAILED -> error(ctx, 412, "match_failed", e.getMessage(), e.nextOffset());
            case CONFLICT -> error(ctx, 409, "conflict", e.getMessage(), e.nextOffset());
            case CLOSED -> {
                ctx.header(Protocol.H_CLOSED, Protocol.BOOL_TRUE);
                error(ctx, 409, "closed", "stream is closed", e.nextOffset());
            }
            case DURABILITY -> error(ctx, 500, "durability", e.getMessage(), null);
        }
    }

    private void options(Context ctx) {
        secure(ctx);
        ctx.status(204);
        ctx.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS");
        ctx.header("Access-Control-Allow-Headers",
            "content-type, authorization, If-None-Match, SS-Match-Seq, SS-Trim-Seq, SS-TTL, SS-Expires-At, "
                + "SS-Closed, SS-Producer-Id, SS-Producer-Epoch, SS-Producer-Seq");
        ctx.header("Access-Control-Expose-Headers",
            "SS-Start-Seq, SS-Next-Seq, SS-Timestamp, SS-Cursor, SS-Up-To-Date, SS-Closed, SS-Producer-Epoch, "
                + "SS-Producer-Seq, SS-Expected-Seq, SS-Received-Seq, etag, content-type");
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
    }

    private void put(Context ctx) throws Exception {
        String name = streamName(ctx);
        byte[] body = bodyOf(ctx);

        if (isList(name)) {
            error(ctx, 400, "bad_request", "cannot create the root stream", null);
            return;
        }

        if (body.length > 0) {
            error(ctx, 400, "bad_request", "create takes no body, append with POST", null);
            return;
        }

        Long ttlSeconds = parseLong(ctx.header(Protocol.H_TTL), "invalid SS-TTL");
        Instant expiresAt = parseInstant(ctx.header(Protocol.H_EXPIRES_AT), "invalid SS-Expires-At");

        if (Objects.nonNull(ttlSeconds) && Objects.nonNull(expiresAt)) {
            throw new IllegalArgumentException("SS-TTL and SS-Expires-At both set");
        }

        String userCt = orDefault(ctx.header(Protocol.H_CONTENT_TYPE), Protocol.DEFAULT_CT);
        CreateResult result = service.lifecycle().create(new CreateCommand(
            name, engineCt(userCt), ttlSeconds, expiresAt, truthy(ctx.header(Protocol.H_CLOSED)), new byte[0]));
        StreamMeta meta = result.meta();

        if (!result.created() && !ContentTypes.mimeEquals(userCt(meta.contentType()), userCt)) {
            error(ctx, 409, "conflict", "stream exists with content type " + userCt(meta.contentType()),
                meta.nextOffset());
            return;
        }

        respond(ctx, result.created() ? 201 : 200, meta.nextOffset(), meta.closed());
        ctx.header(Protocol.H_CONTENT_TYPE, userCt(meta.contentType()));

        if (result.created()) {
            ctx.header(Protocol.H_LOCATION, streamUri(ctx).toString());
        }
    }

    private void head(Context ctx) throws Exception {
        StreamMeta meta = service.lifecycle().head(streamName(ctx)).orElse(null);

        if (Objects.isNull(meta)) {
            respond(ctx, 404, null, false);
            return;
        }

        respond(ctx, 200, meta.nextOffset(), meta.closed());
        writeMeta(ctx, meta);
    }

    private void post(Context ctx) throws Exception {
        String name = streamName(ctx);

        if (isList(name)) {
            error(ctx, 400, "bad_request", "no stream in path", null);
            return;
        }

        Long trimSeq = parseLong(ctx.header(Protocol.H_TRIM_SEQ), "invalid SS-Trim-Seq");

        if (Objects.nonNull(trimSeq)) {
            trim(ctx, name, trimSeq);
            return;
        }

        append(ctx, name);
    }

    private void trim(Context ctx, String name, long trimSeq) throws Exception {
        if (bodyOf(ctx).length > 0) {
            throw new IllegalArgumentException("trim takes no body");
        }

        long start = service.lifecycle().trim(name, trimSeq);

        respond(ctx, 200, null, false);
        ctx.header(Protocol.H_START_SEQ, Long.toString(start));
    }

    private void append(Context ctx, String name) throws Exception {
        boolean close = truthy(ctx.header(Protocol.H_CLOSED));
        Long matchSeq = parseLong(ctx.header(Protocol.H_MATCH_SEQ), "invalid SS-Match-Seq");
        Producer producer = producerOf(ctx);
        byte[] body = bodyOf(ctx);

        if (body.length == 0 && !close) {
            throw new IllegalArgumentException("empty body");
        }

        List<RecordEnvelope> records = body.length == 0 ? List.of() : decodeRecords(ctx, body);
        Long timestamp = records.isEmpty() ? null : timestamps.next(name);
        List<byte[]> payloads = new ArrayList<>(records.size());

        for (RecordEnvelope record : records) {
            payloads.add(RecordEnvelopeCodec.encode(
                new RecordEnvelope(timestamp, record.headers(), record.body())));
        }

        AppendResult result = service.append().append(new AppendCommand(
            name, payloads, Protocol.CT_CORE, null, matchSeq, producer, close, true));

        if (result.applied() && Objects.nonNull(timestamp)) {
            timestamps.record(name, timestamp);
        }

        respond(ctx, 200, result.nextOffset(), result.closed());

        if (result.applied() && !records.isEmpty()) {
            ctx.header(Protocol.H_START_SEQ, Long.toString(result.nextOffset().recordOffset() - records.size()));
            ctx.header(Protocol.H_TIMESTAMP, Long.toString(timestamp));
        }

        if (Objects.nonNull(result.producerEpoch())) {
            ctx.header(Protocol.H_PRODUCER_EPOCH, Long.toString(result.producerEpoch()));
        }

        if (Objects.nonNull(result.producerSeq())) {
            ctx.header(Protocol.H_PRODUCER_SEQ, Long.toString(result.producerSeq()));
        }
    }

    private List<RecordEnvelope> decodeRecords(Context ctx, byte[] body) {
        String mime = ContentTypes.mimeOf(ctx.header(Protocol.H_CONTENT_TYPE));

        if (Protocol.CT_BATCH_JSON.equals(mime)) {
            return JsonCodec.decodeAppend(body);
        }

        if (Protocol.CT_BATCH_BINARY.equals(mime)) {
            return BatchCodec.decodeAppend(body);
        }

        return List.of(new RecordEnvelope(0, Map.of(), body));
    }

    private void delete(Context ctx) throws Exception {
        String name = streamName(ctx);
        boolean deleted = service.lifecycle().delete(name);

        if (deleted) {
            timestamps.invalidate(name);
        }

        respond(ctx, deleted ? 204 : 404, null, false);
    }

    private void get(Context ctx) throws Exception {
        String name = streamName(ctx);

        if (isList(name)) {
            list(ctx);
            return;
        }

        String liveRaw = ctx.queryParam(Protocol.Q_LIVE);
        OffsetToken seq = parseSeq(ctx, name);

        if (Objects.isNull(liveRaw)) {
            writeRead(ctx, name, seq, read(name, seq, ctx), false);
            return;
        }

        switch (liveRaw) {
            case Protocol.LIVE_LONG_POLL -> longPoll(ctx, name, seq);
            case Protocol.LIVE_SSE -> sse(ctx, name, seq);
            default -> throw new IllegalArgumentException("invalid live mode: " + liveRaw);
        }
    }

    private void list(Context ctx) throws Exception {
        String prefix = orDefault(ctx.queryParam(Protocol.Q_PREFIX), "/");
        String startAfter = ctx.queryParam(Protocol.Q_START_AFTER);
        Long limit = parseLong(ctx.queryParam(Protocol.Q_LIMIT), "invalid limit");
        StreamList result = service.lifecycle().list(
            prefix, startAfter, Objects.isNull(limit) ? 0 : Math.toIntExact(limit));
        ObjectNode root = MAPPER.createObjectNode();
        ArrayNode streams = root.putArray("streams");

        for (StreamMeta meta : result.streams()) {
            ObjectNode node = streams.addObject();

            node.put("name", meta.name());
            node.put("content_type", userCt(meta.contentType()));
            node.put("start_seq", meta.startOffset().recordOffset());
            node.put("next_seq", meta.nextOffset().recordOffset());
            node.put("closed", meta.closed());

            if (Objects.nonNull(meta.ttlSeconds())) {
                node.put("ttl", meta.ttlSeconds());
            }

            if (Objects.nonNull(meta.expiresAt())) {
                node.put("expires_at", meta.expiresAt().toString());
            }
        }

        root.put("has_more", result.hasMore());
        secure(ctx);
        ctx.status(200);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
        ctx.result(MAPPER.writeValueAsBytes(root));
    }

    private ReadResult read(String name, OffsetToken seq, Context ctx) throws Exception {
        Long count = parseLong(ctx.queryParam(Protocol.Q_COUNT), "invalid count");
        Long bytes = parseLong(ctx.queryParam(Protocol.Q_BYTES), "invalid bytes");
        int cap = Math.max(maxChunkSize, MAX_READ_BYTES);
        int maxBytes = Objects.isNull(bytes) ? maxChunkSize : Math.toIntExact(Math.min(bytes, cap));

        return service.read().read(name, seq, maxBytes, Objects.isNull(count) ? 0 : Math.toIntExact(count));
    }

    private void longPoll(Context ctx, String name, OffsetToken seq) throws Exception {
        ctx.header(Protocol.H_CURSOR, Long.toString(cursor(ctx.queryParam(Protocol.Q_CURSOR))));
        ReadResult out = read(name, seq, ctx);

        if (!(out.records().isEmpty() && out.upToDate()) || out.closed()) {
            writeRead(ctx, name, seq, out, true);
            return;
        }

        if (service.read().await(name, seq, longPollTimeout)) {
            writeRead(ctx, name, seq, read(name, seq, ctx), true);
            return;
        }

        StreamMeta meta = service.lifecycle().head(name).orElse(null);

        if (Objects.isNull(meta)) {
            respond(ctx, 404, null, false);
            return;
        }

        respond(ctx, 204, meta.nextOffset(), meta.closed());
        ctx.header(Protocol.H_UP_TO_DATE, Protocol.BOOL_TRUE);
    }

    private void sse(Context ctx, String name, OffsetToken start) throws Exception {
        if (service.lifecycle().head(name).isEmpty()) {
            respond(ctx, 404, null, false);
            return;
        }

        secure(ctx);
        ctx.status(200);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_EVENT_STREAM);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-cache");

        OutputStream out = ctx.res().getOutputStream();
        OffsetToken seq = start;
        boolean announcedCaughtUp = false;
        long deadline = System.nanoTime() + sseMaxDuration.toNanos();

        while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
            ReadResult read = service.read().read(name, seq, maxChunkSize, 0);

            if (!read.records().isEmpty()) {
                seq = read.nextOffset();
                boolean closedAtTail = read.closed() && read.upToDate();

                out.write(SseEncoder.dataEvent(toSequenced(read.records()), seq.recordOffset()));
                out.write(SseEncoder.controlEvent(seq.recordOffset(), read.upToDate(), closedAtTail));
                out.flush();

                if (closedAtTail) {
                    return;
                }

                announcedCaughtUp = read.upToDate();
                continue;
            }

            if (read.closed()) {
                out.write(SseEncoder.controlEvent(read.nextOffset().recordOffset(), true, true));
                out.flush();

                return;
            }

            if (!announcedCaughtUp) {
                out.write(SseEncoder.controlEvent(read.nextOffset().recordOffset(), true, false));
                out.flush();
                announcedCaughtUp = true;
            }

            service.read().await(name, seq, Duration.ofSeconds(1));
        }
    }

    private void writeRead(Context ctx, String name, OffsetToken start, ReadResult out, boolean live)
        throws Exception {
        String format = orDefault(ctx.queryParam(Protocol.Q_FORMAT), Protocol.FORMAT_JSON);
        boolean emptyTail = out.records().isEmpty() && out.upToDate();

        secure(ctx);
        ctx.status(emptyTail && live ? 204 : 200);
        ctx.header(Protocol.H_CACHE_CONTROL, live ? "no-store" : Protocol.CACHE_CATCH_UP);
        ctx.header(Protocol.H_NEXT_SEQ, Long.toString(out.nextOffset().recordOffset()));

        if (out.upToDate()) {
            ctx.header(Protocol.H_UP_TO_DATE, Protocol.BOOL_TRUE);
        }

        if (out.closed() && out.upToDate()) {
            ctx.header(Protocol.H_CLOSED, Protocol.BOOL_TRUE);
        }

        if (!live) {
            String etag = etag(name + ":" + format, start, out.nextOffset(), out.closed() && emptyTail);

            ctx.header(Protocol.H_ETAG, etag);

            if (etag.equals(ctx.header(Protocol.H_IF_NONE_MATCH))) {
                ctx.status(304);
                ctx.result("");

                return;
            }
        }

        if (emptyTail && live) {
            return;
        }

        List<SequencedRecord> records = toSequenced(out.records());

        switch (format) {
            case Protocol.FORMAT_JSON -> {
                ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
                ctx.result(JsonCodec.encodeRead(records));
            }
            case Protocol.FORMAT_BINARY -> {
                ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_BATCH_BINARY);
                ctx.result(BatchCodec.encodeRead(records));
            }
            case Protocol.FORMAT_RAW -> {
                ctx.header(Protocol.H_CONTENT_TYPE, userCt(out.contentType()));
                ctx.result(concatBodies(records));
            }
            default -> throw new IllegalArgumentException("invalid format: " + format);
        }
    }

    private static List<SequencedRecord> toSequenced(List<StreamRecord> records) {
        List<SequencedRecord> out = new ArrayList<>(records.size());

        for (StreamRecord record : records) {
            out.add(new SequencedRecord(
                record.offset().recordOffset(), RecordEnvelopeCodec.decode(record.payload())));
        }

        return out;
    }

    private static byte[] concatBodies(List<SequencedRecord> records) {
        int total = 0;

        for (SequencedRecord record : records) {
            total += record.envelope().body().length;
        }

        byte[] out = new byte[total];
        int pos = 0;

        for (SequencedRecord record : records) {
            byte[] body = record.envelope().body();

            System.arraycopy(body, 0, out, pos, body.length);
            pos += body.length;
        }

        return out;
    }

    private OffsetToken parseSeq(Context ctx, String name) throws Exception {
        String raw = ctx.queryParam(Protocol.Q_SEQ);

        if (Objects.isNull(raw)) {
            String lastEventId = ctx.header("Last-Event-ID");

            if (Objects.nonNull(lastEventId) && !lastEventId.isEmpty()) {
                return OffsetToken.parse(lastEventId);
            }

            return OffsetToken.beginning();
        }

        if (Protocol.SEQ_NOW.equalsIgnoreCase(raw)) {
            return service.lifecycle().head(name).map(StreamMeta::nextOffset)
                .orElseThrow(() -> new StreamServiceException(StreamServiceException.Kind.NOT_FOUND));
        }

        return OffsetToken.parse(raw);
    }

    private Producer producerOf(Context ctx) {
        String id = ctx.header(Protocol.H_PRODUCER_ID);
        String epochRaw = ctx.header(Protocol.H_PRODUCER_EPOCH);
        String seqRaw = ctx.header(Protocol.H_PRODUCER_SEQ);
        boolean any = Objects.nonNull(id) || Objects.nonNull(epochRaw) || Objects.nonNull(seqRaw);

        if (!any) {
            return null;
        }

        if (Objects.isNull(id) || id.isEmpty() || Objects.isNull(epochRaw) || Objects.isNull(seqRaw)) {
            throw new IllegalArgumentException(
                "all producer headers (SS-Producer-Id, SS-Producer-Epoch, SS-Producer-Seq) must be provided together");
        }

        return new Producer(
            id,
            requireLong(parseLong(epochRaw, "invalid SS-Producer-Epoch"), "invalid SS-Producer-Epoch"),
            requireLong(parseLong(seqRaw, "invalid SS-Producer-Seq"), "invalid SS-Producer-Seq"));
    }

    static String engineCt(String userCt) {
        return Protocol.CT_CORE + "; " + Protocol.CT_CORE_PARAM + "=" + userCt;
    }

    static String userCt(String engineCt) {
        if (Objects.isNull(engineCt)) {
            return Protocol.DEFAULT_CT;
        }

        int semi = engineCt.indexOf(';');

        if (semi < 0) {
            return Protocol.DEFAULT_CT;
        }

        String params = engineCt.substring(semi + 1).trim();

        if (!params.startsWith(Protocol.CT_CORE_PARAM + "=")) {
            return Protocol.DEFAULT_CT;
        }

        return params.substring(Protocol.CT_CORE_PARAM.length() + 1);
    }

    private void writeMeta(Context ctx, StreamMeta meta) {
        ctx.header(Protocol.H_CONTENT_TYPE, userCt(meta.contentType()));
        ctx.header(Protocol.H_START_SEQ, Long.toString(meta.startOffset().recordOffset()));

        if (Objects.nonNull(meta.ttlSeconds())) {
            ctx.header(Protocol.H_TTL, Long.toString(meta.ttlSeconds()));
        }

        if (Objects.nonNull(meta.expiresAt())) {
            ctx.header(Protocol.H_EXPIRES_AT, meta.expiresAt().toString());
        }
    }

    private static String etag(String name, OffsetToken start, OffsetToken end, boolean closedAtTail) {
        String startVal = Objects.isNull(start) ? "-1" : start.value();
        String endVal = Objects.isNull(end) ? startVal : end.value();
        String encoded = Base64.getEncoder().encodeToString(name.getBytes(StandardCharsets.UTF_8));

        return "\"" + encoded + ":" + startVal + ":" + endVal + (closedAtTail ? ":c" : "") + "\"";
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

    static String streamName(Context ctx) {
        String path = ctx.path();
        return Objects.isNull(path) || path.isEmpty() ? "/" : path;
    }

    private static boolean isList(String name) {
        return "/".equals(name);
    }

    private static byte[] bodyOf(Context ctx) {
        byte[] body = ctx.bodyAsBytes();
        return Objects.isNull(body) ? new byte[0] : body;
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
            ctx.header(Protocol.H_NEXT_SEQ, Long.toString(next.recordOffset()));
        }

        if (closed) {
            ctx.header(Protocol.H_CLOSED, Protocol.BOOL_TRUE);
        }
    }

    private static void error(Context ctx, int status, String code, String message, OffsetToken next) {
        secure(ctx);
        ctx.status(status);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        ObjectNode node = MAPPER.createObjectNode();

        node.put("error", code);

        if (Objects.nonNull(message)) {
            node.put("message", message);
        }

        if (Objects.nonNull(next)) {
            ctx.header(Protocol.H_NEXT_SEQ, Long.toString(next.recordOffset()));
            node.put("next_seq", next.recordOffset());
        }

        try {
            ctx.result(MAPPER.writeValueAsBytes(node));
        } catch (Exception e) {
            ctx.result("{\"error\":\"" + code + "\"}");
        }
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

    private static long requireLong(Long value, String error) {
        if (Objects.isNull(value)) {
            throw new IllegalArgumentException(error);
        }

        return value;
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
