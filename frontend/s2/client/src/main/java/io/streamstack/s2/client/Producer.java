package io.streamstack.s2.client;

import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.response.AppendResponse;

import java.util.ArrayList;
import java.util.List;

public final class Producer implements AutoCloseable {

    private final AppendSession session;

    private final List<AppendRecord> buffer = new ArrayList<>();
    private long bufferedBytes;
    private int maxRecords = Protocol.RECORD_BATCH_MAX_COUNT;
    private long maxBytes = Protocol.RECORD_BATCH_MAX_BYTES;

    Producer(AppendSession session) {
        this.session = session;
    }

    public Producer maxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
        return this;
    }

    public Producer maxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public Producer fencingToken(String fencingToken) {
        session.fencingToken(fencingToken);
        return this;
    }

    public void submit(AppendRecord record) {
        long size = metered(record);

        if (!buffer.isEmpty() && (buffer.size() >= maxRecords || bufferedBytes + size > maxBytes)) {
            flush();
        }

        buffer.add(record);
        bufferedBytes += size;

        if (buffer.size() >= maxRecords || bufferedBytes >= maxBytes) {
            flush();
        }
    }

    public AppendResponse flush() {
        if (buffer.isEmpty()) {
            return session.lastAck();
        }

        session.submit(buffer);
        buffer.clear();
        bufferedBytes = 0;

        return session.flush();
    }

    public AppendResponse lastAck() {
        return session.lastAck();
    }

    @Override
    public void close() {
        flush();
        session.close();
    }

    private static long metered(AppendRecord record) {
        return Protocol.meteredBytes(record.headers(), record.body());
    }
}
