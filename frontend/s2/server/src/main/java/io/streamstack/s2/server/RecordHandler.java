package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.http.Context;
import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.RecordEnvelopeCodec;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.StreamPosition;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.request.ReadRequest;
import io.streamstack.s2.model.response.AppendResponse;
import io.streamstack.s2.model.response.ReadResponse;
import io.streamstack.s2.model.response.SequencedRecord;
import io.streamstack.s2.model.response.TailResponse;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.StreamRecord;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.StreamServiceException;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class RecordHandler {

    private static final int CORE_READ_MAX_BYTES = 4 * 1024 * 1024;
    private final StreamService service;
    private final BasinRegistry registry;
    private final StreamHandler streams;
    private final StreamState state;
    private final ObjectMapper mapper;
    private final Duration sseMaxDuration;

    public RecordHandler(
        StreamService service,
        BasinRegistry registry,
        StreamHandler streams,
        StreamState state,
        ObjectMapper mapper,
        Duration sseMaxDuration) {
        this.service = service;
        this.registry = registry;
        this.streams = streams;
        this.state = state;
        this.mapper = mapper;
        this.sseMaxDuration = Objects.isNull(sseMaxDuration) ? Duration.ofSeconds(55) : sseMaxDuration;
    }

    public void checkTail(Context ctx) throws Exception {
        StreamContext sc = streams.resolve(ctx, false);
        StreamMeta meta = head(sc);

        Requests.json(ctx, 200, new TailResponse(tailPosition(sc.coreName(), meta)));
    }

    public void append(Context ctx) throws Exception {
        StreamContext sc = streams.resolve(ctx, true);
        Format format = Format.parse(ctx.header(Protocol.H_FORMAT));
        AppendRequest request = readAppend(ctx, format);

        validateAppend(request);
        ObjectNode resolved = ConfigJson.resolveStreamConfig(
            mapper, sc.streamDoc().get("config"), sc.basinDoc().get("config"));
        Object lock = state.lock(sc.coreName());
        AppendResponse response;

        synchronized (lock) {
            StreamMeta meta = head(sc);
            long tailSeq = meta.nextOffset().recordOffset();

            checkPreconditions(request, sc, tailSeq);
            PreparedBatch batch = prepareBatch(request, resolved, lastTimestamp(sc.coreName(), tailSeq));
            AppendResult result = service.append().append(new AppendCommand(
                sc.coreName(), batch.payloads(), StreamHandler.CORE_CONTENT_TYPE, null, null, false, true));
            long end = result.nextOffset().recordOffset();
            long start = end - request.records().size();

            state.cacheTimestamp(sc.coreName(), batch.lastTimestamp());
            applyCommands(sc, batch, end);
            response = new AppendResponse(
                new StreamPosition(start, batch.firstTimestamp()),
                new StreamPosition(end, batch.lastTimestamp()),
                new StreamPosition(end, batch.lastTimestamp()));
        }

        Requests.json(ctx, 200, response, format);
    }

    public void read(Context ctx) throws Exception {
        StreamContext sc = streams.resolve(ctx, false);
        Format format = Format.parse(ctx.header(Protocol.H_FORMAT));
        ReadRequest request = Requests.readRequest(ctx);

        if (Objects.nonNull(request.until()) && Objects.nonNull(request.timestamp()) && request.timestamp() >= request.until()) {
            throw S2Exception.invalid("start `timestamp` must be less than `until`");
        }

        if (Requests.acceptsEventStream(ctx)) {
            readSse(ctx, sc, format, request);
            return;
        }

        StreamMeta meta = head(sc);
        long tailSeq = meta.nextOffset().recordOffset();
        long start = resolveStart(sc, meta, request, tailSeq);

        if (start > tailSeq) {
            if (request.clamp()) {
                start = tailSeq;
            } else {
                Requests.json(ctx, 416, new TailResponse(tailPosition(sc.coreName(), meta)), format);
                return;
            }
        }

        long count = bound(request.count(), Protocol.RECORD_BATCH_MAX_COUNT);
        long bytes = bound(request.bytes(), Protocol.RECORD_BATCH_MAX_BYTES);
        long waitSec = Objects.isNull(request.waitSeconds())
            ? 0
            : Math.min(request.waitSeconds(), Protocol.MAX_UNARY_READ_WAIT_SEC);
        Collected collected = collect(sc, start, count, bytes, request.until());

        if (collected.records().isEmpty() && waitSec > 0) {
            service.read().await(sc.coreName(), OffsetToken.ofRecordOffset(start), Duration.ofSeconds(waitSec));
            collected = collect(sc, start, count, bytes, request.until());
        }

        StreamMeta after = head(sc);
        StreamPosition tail = collected.next() >= after.nextOffset().recordOffset()
            ? tailPosition(sc.coreName(), after)
            : null;
        Requests.json(ctx, 200, new ReadResponse(collected.records(), tail), format);
    }

    private void readSse(Context ctx, StreamContext sc, Format format, ReadRequest request) throws Exception {
        StreamMeta meta = head(sc);
        long tailSeq = meta.nextOffset().recordOffset();
        long start = resolveStart(sc, meta, request, tailSeq);
        long remainingCount = Objects.isNull(request.count()) ? Long.MAX_VALUE : request.count();
        long remainingBytes = Objects.isNull(request.bytes()) ? Long.MAX_VALUE : request.bytes();
        long sentCount = 0;
        long sentBytes = 0;
        String lastEventId = ctx.header(Protocol.H_LAST_EVENT_ID);

        if (Objects.nonNull(lastEventId) && !lastEventId.isEmpty()) {
            String[] parts = lastEventId.split(",", 3);

            if (parts.length == 3) {
                try {
                    start = Long.parseLong(parts[0].trim()) + 1;
                    remainingCount = Math.max(0, subtractCapped(remainingCount, Long.parseLong(parts[1].trim())));
                    remainingBytes = Math.max(0, subtractCapped(remainingBytes, Long.parseLong(parts[2].trim())));
                } catch (NumberFormatException e) {
                    throw S2Exception.badHeader("invalid Last-Event-ID");
                }
            }
        }

        if (start > tailSeq && !request.clamp()) {
            Requests.json(ctx, 416, new TailResponse(tailPosition(sc.coreName(), meta)), format);
            return;
        }

        start = Math.min(start, tailSeq);
        ctx.status(200);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_EVENT_STREAM);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-cache, no-transform");
        ctx.header("x-accel-buffering", "no");
        OutputStream out = ctx.res().getOutputStream();
        long offset = start;
        long deadline = System.nanoTime() + sseMaxDuration.toNanos();
        long lastPingNanos = System.nanoTime();

        while (System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
            if (remainingCount <= 0 || remainingBytes <= 0) {
                out.write(SseEncoder.doneEvent());
                out.flush();

                return;
            }

            Collected collected = collect(sc, offset,
                Math.min(remainingCount, Protocol.RECORD_BATCH_MAX_COUNT),
                Math.min(remainingBytes, Protocol.RECORD_BATCH_MAX_BYTES),
                request.until());
            if (!collected.records().isEmpty()) {
                offset = collected.next();
                sentCount += collected.records().size();
                sentBytes += collected.meteredBytes();
                remainingCount = subtractCapped(remainingCount, collected.records().size());
                remainingBytes = subtractCapped(remainingBytes, collected.meteredBytes());
                StreamMeta after = head(sc);
                StreamPosition tail = offset >= after.nextOffset().recordOffset()
                    ? tailPosition(sc.coreName(), after) : null;
                long lastSeq = collected.records().get(collected.records().size() - 1).seqNum();

                out.write(SseEncoder.batchEvent(
                    new ReadResponse(collected.records(), tail), format, lastSeq, sentCount, sentBytes));
                out.flush();
                continue;
            }

            if (collected.reachedUntil()) {
                out.write(SseEncoder.doneEvent());
                out.flush();

                return;
            }

            if (System.nanoTime() - lastPingNanos > Duration.ofSeconds(10).toNanos()) {
                StreamMeta current = head(sc);

                out.write(SseEncoder.pingEvent(tailPosition(sc.coreName(), current)));
                out.flush();
                lastPingNanos = System.nanoTime();
            }

            service.read().await(sc.coreName(), OffsetToken.ofRecordOffset(offset), Duration.ofSeconds(1));
        }
    }

    private static void checkPreconditions(AppendRequest request, StreamContext sc, long tailSeq) {
        String storedToken = sc.streamDoc().path("fencing_token").asText("");

        if (Objects.nonNull(request.fencingToken()) && !request.fencingToken().equals(storedToken)) {
            throw S2Exception.fencingTokenMismatch(storedToken);
        }

        if (Objects.nonNull(request.matchSeqNum()) && request.matchSeqNum() != tailSeq) {
            throw S2Exception.seqNumMismatch(tailSeq);
        }
    }

    private PreparedBatch prepareBatch(AppendRequest request, ObjectNode resolved, long lastTs) {
        long now = System.currentTimeMillis();
        String mode = resolved.get("timestamping").get("mode").asText();
        boolean uncapped = resolved.get("timestamping").get("uncapped").asBoolean();
        List<byte[]> payloads = new ArrayList<>(request.records().size());
        long firstTs = 0;
        String newFencingToken = null;
        Long trimPoint = null;

        for (int i = 0; i < request.records().size(); i++) {
            AppendRecord record = request.records().get(i);
            long ts = assignTimestamp(record, mode, uncapped, now, lastTs);

            lastTs = ts;

            if (i == 0) {
                firstTs = ts;
            }

            if (record.isCommand()) {
                String command = record.commandName();

                if (Protocol.COMMAND_FENCE.equals(command)) {
                    newFencingToken = new String(record.body(), StandardCharsets.UTF_8);

                    if (newFencingToken.length() > 36) {
                        throw S2Exception.invalid("fencing token must be at most 36 characters");
                    }
                } else if (Protocol.COMMAND_TRIM.equals(command)) {
                    if (record.body().length != 8) {
                        throw S2Exception.invalid("trim command body must be 8 big-endian bytes");
                    }

                    trimPoint = ByteBuffer.wrap(record.body()).getLong();
                } else {
                    throw S2Exception.invalid("unknown command: " + command);
                }
            }

            payloads.add(ProtocolConverter.toEnvelopeBytes(record, ts));
        }

        return new PreparedBatch(payloads, firstTs, lastTs, newFencingToken, trimPoint);
    }

    private void applyCommands(StreamContext sc, PreparedBatch batch, long end) throws Exception {
        if (Objects.isNull(batch.newFencingToken()) && Objects.isNull(batch.trimPoint())) {
            return;
        }

        if (Objects.nonNull(batch.newFencingToken())) {
            sc.streamDoc().put("fencing_token", batch.newFencingToken());
        }

        if (Objects.nonNull(batch.trimPoint())) {
            long bounded = Math.min(batch.trimPoint(), end);
            long current = sc.streamDoc().path("trim_point").asLong(0);

            if (bounded > current) {
                sc.streamDoc().put("trim_point", bounded);
                service.lifecycle().trim(sc.coreName(), bounded);
            }
        }

        registry.putStream(sc.basin(), sc.stream(), sc.streamDoc());
    }

    private Collected collect(StreamContext sc, long start, long maxCount, long maxBytes, Long until)
        throws StreamServiceException {
        List<SequencedRecord> records = new ArrayList<>();
        long metered = 0;
        long offset = start;
        boolean reachedUntil = false;

        while (records.size() < maxCount && metered < maxBytes) {
            int remaining = (int) Math.min(maxCount - records.size(), Protocol.RECORD_BATCH_MAX_COUNT);
            var rr = service.read().read(sc.coreName(), OffsetToken.ofRecordOffset(offset),
                CORE_READ_MAX_BYTES, remaining);
            if (rr.records().isEmpty()) {
                break;
            }

            for (StreamRecord raw : rr.records()) {
                SequencedRecord record = ProtocolConverter.toSequencedRecord(raw);

                if (Objects.nonNull(until) && record.timestamp() >= until) {
                    reachedUntil = true;
                    break;
                }

                long size = meteredSize(record);

                if (metered + size > maxBytes && !records.isEmpty()) {
                    metered = maxBytes;
                    break;
                }

                records.add(record);
                metered += size;
                offset = record.seqNum() + 1;

                if (records.size() >= maxCount || metered >= maxBytes) {
                    break;
                }
            }

            if (reachedUntil || rr.upToDate()) {
                break;
            }
        }

        return new Collected(records, offset, metered, reachedUntil);
    }

    private long resolveStart(StreamContext sc, StreamMeta meta, ReadRequest request, long tailSeq)
        throws StreamServiceException {
        int selectors = (Objects.nonNull(request.seqNum()) ? 1 : 0)
            + (Objects.nonNull(request.timestamp()) ? 1 : 0)
            + (Objects.nonNull(request.tailOffset()) ? 1 : 0);
        if (selectors > 1) {
            throw S2Exception.badQuery("only one of seq_num, timestamp, or tail_offset can be provided");
        }

        long first = Math.max(sc.streamDoc().path("trim_point").asLong(0), meta.startOffset().recordOffset());

        if (Objects.nonNull(request.seqNum())) {
            return Math.max(request.seqNum(), first);
        }

        if (Objects.nonNull(request.timestamp())) {
            return searchTimestamp(sc.coreName(), first, tailSeq, request.timestamp());
        }

        long tailOffset = Objects.isNull(request.tailOffset()) ? 0 : request.tailOffset();

        return Math.max(tailSeq - tailOffset, first);
    }

    private long searchTimestamp(String coreName, long first, long tailSeq, long timestamp)
        throws StreamServiceException {
        long lo = first;
        long hi = tailSeq;

        while (lo < hi) {
            long mid = lo + (hi - lo) / 2;
            var rr = service.read().read(coreName, OffsetToken.ofRecordOffset(mid), CORE_READ_MAX_BYTES, 1);

            if (rr.records().isEmpty()) {
                break;
            }

            long ts = RecordEnvelopeCodec.decodeTimestamp(rr.records().get(0).payload());

            if (ts < timestamp) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        return lo;
    }

    private long lastTimestamp(String coreName, long tailSeq) throws StreamServiceException {
        Long cached = state.cachedTimestamp(coreName);

        if (Objects.nonNull(cached)) {
            return cached;
        }

        if (tailSeq == 0) {
            return 0;
        }

        var rr = service.read().read(coreName, OffsetToken.ofRecordOffset(tailSeq - 1), CORE_READ_MAX_BYTES, 1);
        long ts = rr.records().isEmpty() ? 0 : RecordEnvelopeCodec.decodeTimestamp(rr.records().get(0).payload());

        state.cacheTimestamp(coreName, ts);

        return ts;
    }

    private StreamPosition tailPosition(String coreName, StreamMeta meta) throws StreamServiceException {
        long tailSeq = meta.nextOffset().recordOffset();
        return new StreamPosition(tailSeq, lastTimestamp(coreName, tailSeq));
    }

    private StreamMeta head(StreamContext sc) throws StreamServiceException {
        return service.lifecycle().head(sc.coreName())
            .orElseThrow(() -> S2Exception.streamNotFound(sc.stream()));
    }

    private static AppendRequest readAppend(Context ctx, Format format) {
        byte[] body = ctx.bodyAsBytes();

        if (Objects.isNull(body) || body.length == 0) {
            throw S2Exception.badJson("`records` must be an array");
        }

        try {
            AppendRequest request = S2Json.read(body, AppendRequest.class, format);

            if (Objects.isNull(request.records())) {
                throw S2Exception.badJson("`records` must be an array");
            }

            return request;
        } catch (S2Exception e) {
            throw e;
        } catch (Exception e) {
            throw S2Exception.badJson(e.getMessage());
        }
    }

    private static long assignTimestamp(AppendRecord record, String mode, boolean uncapped, long now, long lastTs) {
        long ts;

        if ("arrival".equals(mode)) {
            ts = now;
        } else if (Objects.nonNull(record.timestamp())) {
            ts = uncapped ? record.timestamp() : Math.min(record.timestamp(), now);
        } else if ("client-require".equals(mode)) {
            throw S2Exception.invalid("timestamp is required by the stream's timestamping mode");
        } else {
            ts = now;
        }

        return Math.max(ts, lastTs);
    }

    private static void validateAppend(AppendRequest request) {
        if (request.records().isEmpty()) {
            throw S2Exception.invalid("batch must contain at least one record");
        }

        if (request.records().size() > Protocol.RECORD_BATCH_MAX_COUNT) {
            throw S2Exception.invalid("batch must contain no more than 1000 records");
        }

        long metered = 0;
        boolean hasCommand = false;

        for (AppendRecord record : request.records()) {
            metered += Protocol.meteredBytes(record.headers(), record.body());
            hasCommand |= record.isCommand();
        }

        if (metered > Protocol.RECORD_BATCH_MAX_BYTES) {
            throw S2Exception.invalid("batch exceeds 1 MiB of metered bytes");
        }

        if (hasCommand && request.records().size() != 1) {
            throw S2Exception.invalid("command records must be appended individually");
        }
    }

    private static long meteredSize(SequencedRecord record) {
        return Protocol.meteredBytes(record.headers(), record.body());
    }

    private static long bound(Long value, long max) {
        return Objects.isNull(value) ? max : Math.min(value, max);
    }

    private static long subtractCapped(long remaining, long used) {
        return remaining == Long.MAX_VALUE ? Long.MAX_VALUE : remaining - used;
    }

    private record PreparedBatch(
        List<byte[]> payloads,
        long firstTimestamp,
        long lastTimestamp,
        String newFencingToken,
        Long trimPoint) {
    }

    private record Collected(List<SequencedRecord> records, long next, long meteredBytes, boolean reachedUntil) {
    }
}
