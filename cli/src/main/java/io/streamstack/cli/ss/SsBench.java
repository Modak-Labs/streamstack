package io.streamstack.cli.ss;

import io.streamstack.cli.BenchStats;
import io.streamstack.cli.Io;
import io.streamstack.ss.client.StreamStack;
import io.streamstack.ss.model.RecordEnvelope;
import io.streamstack.ss.model.request.AppendRequest;
import io.streamstack.ss.model.response.ReadResponse;
import io.streamstack.ss.model.SequencedRecord;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Command(name = "bench", description = "Write and live-read a temporary stream")
public final class SsBench implements Callable<Integer> {

    @ParentCommand
    SsCommand parent;

    @Option(names = {"-b", "--record-size"}, defaultValue = "1024")
    int recordSize;

    @Option(names = {"-d", "--duration"}, defaultValue = "15")
    int durationSec;

    @Option(names = {"-w", "--in-flight"}, defaultValue = "32", description = "max concurrent appends")
    int inFlight;

    @Option(names = {"-n", "--batch"}, defaultValue = "1", description = "records per append (binary batch when > 1)")
    int batch;

    @Override
    public Integer call() throws Exception {
        String stream = "/bench/" + UUID.randomUUID();
        int perAppend = Math.max(1, batch);
        AppendRequest request = new AppendRequest(records(recordSize, perAppend));
        long requestBytes = (long) Math.max(1, recordSize) * perAppend;
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch readerReady = new CountDownLatch(1);
        BenchStats writeStats = new BenchStats();
        BenchStats readStats = new BenchStats();

        try (StreamStack client = parent.open()) {
            client.create(stream, "application/octet-stream");
            Io.err("created " + stream);
            Thread reader = reader(client, stream, stop, readerReady, readStats);

            reader.start();

            if (!readerReady.await(10, TimeUnit.SECONDS)) {
                Io.err("reader did not start");
            }

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSec);
            Semaphore permits = new Semaphore(Math.max(1, inFlight));
            AtomicReference<Throwable> writeError = new AtomicReference<>();

            while (System.nanoTime() < deadline && Objects.isNull(writeError.get())) {
                permits.acquire();
                client.appendAsync(stream, request).whenComplete((ack, error) -> {
                    permits.release();

                    if (Objects.isNull(error)) {
                        for (int i = 0; i < perAppend; i++) {
                            writeStats.add(requestBytes / perAppend);
                        }
                    } else {
                        writeError.compareAndSet(null, error);
                    }
                });
            }

            permits.acquire(Math.max(1, inFlight));

            if (Objects.nonNull(writeError.get())) {
                Io.err("append failed: " + writeError.get().getMessage());
            }

            stop.set(true);
            reader.join(5_000);
            Io.err(writeStats.summary("Write"));
            Io.err(readStats.summary("Read"));
            client.delete(stream);
        }

        return 0;
    }

    private static Thread reader(
        StreamStack client,
        String stream,
        AtomicBoolean stop,
        CountDownLatch readerReady,
        BenchStats readStats) {
        Thread reader = new Thread(() -> {
            readerReady.countDown();
            long next = 0;

            while (!stop.get()) {
                try {
                    ReadResponse page = client.read(stream, next, 0, 4 * 1024 * 1024);

                    for (SequencedRecord record : page.records()) {
                        readStats.add(record.envelope().body().length);
                    }

                    next = page.nextSeq();

                    if (page.records().isEmpty()) {
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    if (!stop.get()) {
                        Io.err("read failed: " + e.getMessage());
                    }

                    return;
                }
            }
        }, "native-bench-read");

        reader.setDaemon(true);

        return reader;
    }

    private static List<RecordEnvelope> records(int recordSize, int batch) {
        byte[] body = new byte[Math.max(1, recordSize)];
        List<RecordEnvelope> records = new ArrayList<>(batch);

        for (int i = 0; i < batch; i++) {
            records.add(new RecordEnvelope(0, Map.of(), body));
        }

        return records;
    }
}
