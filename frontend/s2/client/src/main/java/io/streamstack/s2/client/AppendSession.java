package io.streamstack.s2.client;

import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.response.AppendResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class AppendSession implements AutoCloseable {

    private final Stream stream;

    private final List<AppendRecord> pending = new ArrayList<>();
    private Long matchSeqNum;
    private String fencingToken;
    private volatile AppendResponse lastAck;

    AppendSession(Stream stream) {
        this.stream = stream;
    }

    public AppendSession matchSeqNum(Long matchSeqNum) {
        this.matchSeqNum = matchSeqNum;
        return this;
    }

    public AppendSession fencingToken(String fencingToken) {
        this.fencingToken = fencingToken;
        return this;
    }

    public void submit(AppendRecord record) {
        pending.add(record);
    }

    public void submit(List<AppendRecord> records) {
        pending.addAll(records);
    }

    public AppendResponse flush() {
        if (pending.isEmpty()) {
            return lastAck;
        }

        lastAck = stream.append(takeBatch());
        matchSeqNum = lastAck.end().seqNum();

        return lastAck;
    }

    public CompletableFuture<AppendResponse> flushAsync() {
        if (pending.isEmpty()) {
            return CompletableFuture.completedFuture(lastAck());
        }

        AppendRequest request = takeBatch();

        if (Objects.nonNull(matchSeqNum)) {
            matchSeqNum += request.records().size();
        }

        return stream.appendAsync(request).whenComplete((ack, failure) -> {
            if (Objects.nonNull(ack)) {
                recordAck(ack);
            }
        });
    }

    private AppendRequest takeBatch() {
        List<AppendRecord> batch = List.copyOf(pending);

        pending.clear();

        return new AppendRequest(batch, matchSeqNum, fencingToken);
    }

    public AppendResponse lastAck() {
        return lastAck;
    }

    private synchronized void recordAck(AppendResponse ack) {
        if (Objects.isNull(lastAck) || ack.end().seqNum() > lastAck.end().seqNum()) {
            lastAck = ack;
        }
    }

    @Override
    public void close() {
        flush();
    }
}
