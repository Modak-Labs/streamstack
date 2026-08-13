package io.streamstack.server;

import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.StreamRecord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class StreamPending {

    private static final int TAIL_MAX_BYTES = 4 * 1024 * 1024;
    private static final int TAIL_MAX_RECORDS = 4096;

    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

    private final ArrayDeque<StreamRecord> recent = new ArrayDeque<>();
    private long recentBytes;

    public synchronized CompletableFuture<Void> enqueue(List<CompletableFuture<?>> submitted, Runnable onFailure) {
        if (tail.isCompletedExceptionally()) {
            tail = CompletableFuture.completedFuture(null);
        }

        if (submitted.isEmpty()) {
            return tail;
        }

        CompletableFuture<Void> own = CompletableFuture.allOf(submitted.toArray(CompletableFuture[]::new));

        own.whenComplete((v, t) -> {
            if (Objects.nonNull(t)) {
                onFailure.run();
            }
        });
        tail = tail.thenCombine(own, (a, b) -> null);

        return tail;
    }

    public synchronized boolean poisoned() {
        return tail.isCompletedExceptionally();
    }

    public synchronized void reset() {
        tail = CompletableFuture.completedFuture(null);
        recent.clear();
        recentBytes = 0;
    }

    public synchronized void recordAppend(long baseOffset, List<byte[]> records) {
        if (records.isEmpty()) {
            return;
        }

        if (!recent.isEmpty()) {
            long expected = recent.peekLast().offset().recordOffset() + 1;

            if (baseOffset < expected) {
                return;
            }

            if (baseOffset > expected) {
                recent.clear();
                recentBytes = 0;
            }
        }

        for (int i = 0; i < records.size(); i++) {
            byte[] bytes = records.get(i);

            recent.addLast(new StreamRecord(OffsetToken.ofRecordOffset(baseOffset + i), bytes));
            recentBytes += bytes.length;
        }

        while (recent.size() > TAIL_MAX_RECORDS || (recentBytes > TAIL_MAX_BYTES && recent.size() > 1)) {
            recentBytes -= recent.removeFirst().payload().length;
        }
    }

    public synchronized List<StreamRecord> tailRecords(long start) {
        if (recent.isEmpty()
            || start < recent.peekFirst().offset().recordOffset()
            || start > recent.peekLast().offset().recordOffset()) {
            return null;
        }

        List<StreamRecord> out = new ArrayList<>();

        for (StreamRecord record : recent) {
            if (record.offset().recordOffset() >= start) {
                out.add(record);
            }
        }

        return out;
    }
}
