package io.streamstack.server.ds;

import java.util.Objects;

import io.streamstack.model.Offset;
import io.streamstack.model.request.AppendRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.CreateResponse;
import io.streamstack.model.response.HeadResponse;
import io.streamstack.model.response.ReadResponse;
import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.Producer;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamMeta;

import java.time.Instant;
import java.util.List;

public final class ProtocolConverter {

    private ProtocolConverter() {
    }

    public static CreateCommand toCreateCommand(
        String name,
        String contentType,
        Long ttlSeconds,
        Instant expiresAt,
        boolean closed,
        byte[] initialBody) {
        return new CreateCommand(name, contentType, ttlSeconds, expiresAt, closed, initialBody);
    }

    public static AppendCommand toAppendCommand(String name, AppendRequest request) {
        String streamSeq = request.streamSeq();
        if (Objects.nonNull(streamSeq) && streamSeq.isEmpty()) {
            streamSeq = null;
        }
        Producer producer = null;
        if (Objects.nonNull(request.producerId())) {
            producer = new Producer(request.producerId(), request.producerEpoch(), request.producerSeq());
        }
        List<byte[]> payloads = Objects.isNull(request.body()) || request.body().length == 0
            ? List.of()
            : List.of(request.body());
        return new AppendCommand(
            name,
            payloads,
            request.contentType(),
            streamSeq,
            producer,
            request.close());
    }

    public static AppendResponse toAppendResponse(AppendResult result) {
        return new AppendResponse(
            toOffset(result.nextOffset()),
            result.applied(),
            result.closed(),
            result.producerEpoch(),
            result.producerSeq());
    }

    public static CreateResponse toCreateResponse(CreateResult result) {
        StreamMeta meta = result.meta();
        return new CreateResponse(
            result.created(),
            meta.contentType(),
            toOffset(meta.nextOffset()),
            meta.closed());
    }

    public static HeadResponse toHeadResponse(StreamMeta meta) {
        return new HeadResponse(
            meta.contentType(),
            meta.ttlSeconds(),
            meta.expiresAt(),
            toOffset(meta.nextOffset()),
            meta.closed());
    }

    public static ReadResponse toReadResponse(ReadResult result) {
        return new ReadResponse(
            result.payloads(),
            result.contentType(),
            toOffset(result.nextOffset()),
            result.upToDate(),
            result.closed());
    }

    public static Offset toOffset(OffsetToken token) {
        return Objects.isNull(token) ? null : Offset.of(token.value());
    }

    public static OffsetToken toToken(Offset offset) {
        if (Objects.isNull(offset)) {
            return null;
        }
        if (offset.isBeginning()) {
            return OffsetToken.beginning();
        }
        return OffsetToken.parse(offset.value());
    }
}
