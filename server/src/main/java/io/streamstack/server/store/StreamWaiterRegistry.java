package io.streamstack.server.store;

import io.streamstack.server.model.OffsetToken;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class StreamWaiterRegistry {
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Waiter>> waiters = new ConcurrentHashMap<>();

    public boolean await(String path, OffsetToken offset, Duration timeout) throws InterruptedException {
        ConcurrentLinkedQueue<Waiter> queue = waiters.computeIfAbsent(path, key -> new ConcurrentLinkedQueue<>());
        Waiter waiter = new Waiter(offset.recordOffset());
        queue.add(waiter);
        try {
            waiter.future().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException e) {
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            return true;
        } finally {
            queue.remove(waiter);
            if (queue.isEmpty()) {
                waiters.remove(path, queue);
            }
        }
    }

    public void notifyAppend(String path, long nextRecordOffset) {
        ConcurrentLinkedQueue<Waiter> queue = waiters.get(path);
        if (queue == null) {
            return;
        }
        Iterator<Waiter> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Waiter waiter = iterator.next();
            if (nextRecordOffset > waiter.waitOffset()) {
                waiter.future().complete(null);
                iterator.remove();
            }
        }
        if (queue.isEmpty()) {
            waiters.remove(path, queue);
        }
    }

    public void notifyClosed(String path) {
        ConcurrentLinkedQueue<Waiter> queue = waiters.remove(path);
        if (queue == null) {
            return;
        }
        for (Waiter waiter : queue) {
            waiter.future().complete(null);
        }
    }

    public void clear() {
        for (Map.Entry<String, ConcurrentLinkedQueue<Waiter>> entry : waiters.entrySet()) {
            for (Waiter waiter : entry.getValue()) {
                waiter.future().complete(null);
            }
        }
        waiters.clear();
    }

    private record Waiter(long waitOffset, CompletableFuture<Void> future) {
        Waiter(long waitOffset) {
            this(waitOffset, new CompletableFuture<>());
        }
    }
}
