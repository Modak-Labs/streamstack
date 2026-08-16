package io.streamstack.ss.server;

import io.streamstack.ss.model.RecordEnvelopeCodec;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.service.StreamService;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class StreamTimestamps {

    private final StreamService service;

    private final ConcurrentHashMap<String, Long> lastByName = new ConcurrentHashMap<>();

    StreamTimestamps(StreamService service) {
        this.service = Objects.requireNonNull(service, "service");
    }

    long next(String name) throws Exception {
        Long last = lastByName.get(name);

        if (Objects.isNull(last)) {
            last = readBack(name);
        }

        return Math.max(System.currentTimeMillis(), last + 1);
    }

    void record(String name, long timestamp) {
        lastByName.merge(name, timestamp, Math::max);
    }

    void invalidate(String name) {
        lastByName.remove(name);
    }

    private long readBack(String name) throws Exception {
        StreamMeta meta = service.lifecycle().head(name).orElse(null);

        if (Objects.isNull(meta) || meta.nextOffset().recordOffset() <= meta.startOffset().recordOffset()) {
            return 0;
        }

        long tail = meta.nextOffset().recordOffset() - 1;
        ReadResult read = service.read().read(name, OffsetToken.ofRecordOffset(tail), 0, 1);

        if (read.records().isEmpty()) {
            return 0;
        }

        try {
            return RecordEnvelopeCodec.decodeTimestamp(read.records().get(read.records().size() - 1).payload());
        } catch (Exception e) {
            return 0;
        }
    }
}
