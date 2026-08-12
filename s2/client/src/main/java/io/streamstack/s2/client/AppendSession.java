package io.streamstack.s2.client;

import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.response.AppendResponse;

import java.util.ArrayList;
import java.util.List;

public final class AppendSession implements AutoCloseable {

    private final Stream stream;

    private final List<AppendRecord> pending = new ArrayList<>();
    private Long matchSeqNum;
    private String fencingToken;
    private AppendResponse lastAck;

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
        List<AppendRecord> batch = List.copyOf(pending);
        pending.clear();
        lastAck = stream.append(new AppendRequest(batch, matchSeqNum, fencingToken));
        matchSeqNum = lastAck.end().seqNum();
        return lastAck;
    }

    public AppendResponse lastAck() {
        return lastAck;
    }

    @Override
    public void close() {
        flush();
    }
}
