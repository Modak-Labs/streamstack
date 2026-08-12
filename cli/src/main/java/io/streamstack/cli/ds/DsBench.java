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

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "bench", description = "Write and live-read a temporary stream")
public final class DsBench implements Callable<Integer> {

    @ParentCommand
    DsCommand parent;

    @Option(names = {"-b", "--record-size"}, defaultValue = "1024")
    int recordSize;

    @Option(names = {"-t", "--target-mibps"}, defaultValue = "1")
    double targetMibps;

    @Option(names = {"-d", "--duration"}, defaultValue = "15")
    int durationSec;

    @Override
    public Integer call() throws Exception {
        String stream = "/bench/" + UUID.randomUUID();
        String url = parent.url(stream);
        byte[] body = new byte[Math.max(1, recordSize)];
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch readerReady = new CountDownLatch(1);
        BenchStats writeStats = new BenchStats();
        BenchStats readStats = new BenchStats();

        try (DurableStream client = parent.open()) {
            client.create(url);
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
            double targetBps = Math.max(targetMibps, 0.01) * 1024 * 1024;
            long sent = 0;

            while (System.nanoTime() < deadline) {
                client.append(url, body);
                sent += body.length;
                writeStats.add(body.length);
                long expected = (long) (sent / targetBps * 1_000_000_000L);
                long elapsed = System.nanoTime() - start;

                if (expected > elapsed) {
                    TimeUnit.NANOSECONDS.sleep(expected - elapsed);
                }
            }

            stop.set(true);
            reader.join(5_000);
            Io.err(writeStats.summary("Write"));
            Io.err(readStats.summary("Read"));
            client.delete(url);
        }

        return 0;
    }
}
