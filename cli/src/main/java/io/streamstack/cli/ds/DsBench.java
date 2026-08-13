package io.streamstack.cli.ds;

import io.streamstack.cli.BenchStats;
import io.streamstack.cli.Io;
import io.streamstack.client.ChunkIterator;
import io.streamstack.client.DurableStream;
import io.streamstack.client.model.Chunk;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.request.ReadRequest;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Command(name = "bench", description = "Write and live-read a temporary stream")
public final class DsBench implements Callable<Integer> {

    @ParentCommand
    DsCommand parent;

    @Option(names = {"-b", "--record-size"}, defaultValue = "1024")
    int recordSize;

    @Option(names = {"-t", "--target-mibps"}, defaultValue = "0", description = "throttle writes (0 = unthrottled)")
    double targetMibps;

    @Option(names = {"-d", "--duration"}, defaultValue = "15")
    int durationSec;

    @Option(names = {"-w", "--in-flight"}, defaultValue = "32", description = "max concurrent appends")
    int inFlight;

    @Option(names = {"-n", "--batch"}, defaultValue = "1", description = "messages per append (JSON array when > 1)")
    int batch;

    @Override
    public Integer call() throws Exception {
        String stream = "/bench/" + UUID.randomUUID();
        String url = parent.url(stream);
        int perAppend = Math.max(1, batch);
        byte[] body = perAppend > 1 ? jsonBatch(recordSize, perAppend) : new byte[Math.max(1, recordSize)];
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch readerReady = new CountDownLatch(1);
        BenchStats writeStats = new BenchStats();
        BenchStats readStats = new BenchStats();

        try (DurableStream client = parent.open()) {
            client.create(url, perAppend > 1 ? "application/json" : "application/octet-stream");
            Io.err("created " + stream);
            Thread reader = new Thread(() -> {
                try (ChunkIterator chunks = client.read(
                    url, new ReadRequest(Offset.beginning(), LiveMode.SSE, null))) {
                    readerReady.countDown();

                    for (Chunk chunk : chunks) {
                        if (chunk.data().length > 0) {
                            readStats.add(chunk.data().length);
                        }

                        if (stop.get()) {
                            break;
                        }
                    }
                } catch (RuntimeException e) {
                    readerReady.countDown();

                    if (!stop.get()) {
                        Io.err("read failed: " + e.getMessage());
                    }
                }
            }, "ds-bench-read");

            reader.setDaemon(true);
            reader.start();

            if (!readerReady.await(10, TimeUnit.SECONDS)) {
                Io.err("reader did not start");
            }

            long start = System.nanoTime();
            long deadline = start + TimeUnit.SECONDS.toNanos(durationSec);
            double targetBps = targetMibps > 0 ? targetMibps * 1024 * 1024 : 0;
            int window = Math.max(1, inFlight);
            Semaphore permits = new Semaphore(window);
            AtomicReference<Throwable> writeError = new AtomicReference<>();
            long sent = 0;

            while (System.nanoTime() < deadline && Objects.isNull(writeError.get())) {
                permits.acquire();
                client.appendAsync(url, body).whenComplete((ack, error) -> {
                    permits.release();

                    if (Objects.isNull(error)) {
                        for (int i = 0; i < perAppend; i++) {
                            writeStats.add(body.length / perAppend);
                        }
                    } else {
                        writeError.compareAndSet(null, error);
                    }
                });
                sent += body.length;

                if (targetBps > 0) {
                    long expected = (long) (sent / targetBps * 1_000_000_000L);
                    long elapsed = System.nanoTime() - start;

                    if (expected > elapsed) {
                        TimeUnit.NANOSECONDS.sleep(expected - elapsed);
                    }
                }
            }

            permits.acquire(window);

            if (Objects.nonNull(writeError.get())) {
                Io.err("append failed: " + writeError.get().getMessage());
            }

            stop.set(true);
            reader.join(5_000);
            Io.err(writeStats.summary("Write"));
            Io.err(readStats.summary("Read"));
            client.delete(url);
        }

        return 0;
    }

    private static byte[] jsonBatch(int recordSize, int batch) {
        String message = "x".repeat(Math.max(1, recordSize - 2));
        StringBuilder out = new StringBuilder(batch * (recordSize + 1) + 2);

        out.append('[');

        for (int i = 0; i < batch; i++) {
            if (i > 0) {
                out.append(',');
            }

            out.append('"').append(message).append('"');
        }

        out.append(']');

        return out.toString().getBytes(StandardCharsets.UTF_8);
    }
}
