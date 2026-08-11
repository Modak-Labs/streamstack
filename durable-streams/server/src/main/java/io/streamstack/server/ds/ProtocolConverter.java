package io.streamstack.server.ds;

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

/**
 * Maps Durable Streams wire types to/from core service model types.
 */
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
        Long streamSeq = null;
        if (request.streamSeq() != null && !request.streamSeq().isEmpty()) {
            streamSeq = OffsetToken.parse(request.streamSeq()).recordOffset();
        }
        Producer producer = null;
        if (request.producerId() != null) {
            producer = new Producer(request.producerId(), request.producerEpoch(), request.producerSeq());
        }
        List<byte[]> payloads = request.body() == null || request.body().length == 0
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
        return token == null ? null : Offset.of(token.value());
    }

    public static OffsetToken toToken(Offset offset) {
        if (offset == null) {
            return null;
        }
        if (offset.isBeginning()) {
            return OffsetToken.beginning();
        }
        return OffsetToken.parse(offset.value());
    }
}
