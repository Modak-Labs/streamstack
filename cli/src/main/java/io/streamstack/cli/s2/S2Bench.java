package io.streamstack.cli.s2;

import io.streamstack.cli.BenchStats;
import io.streamstack.cli.Io;
import io.streamstack.s2.client.Producer;
import io.streamstack.s2.client.ReadSession;
import io.streamstack.s2.client.S2;
import io.streamstack.s2.client.Stream;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.ReadRequest;
import io.streamstack.s2.model.response.SequencedRecord;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "bench", description = "Write and live-read a temporary stream")
public final class S2Bench implements Callable<Integer> {

    @ParentCommand
    S2Command parent;

    @Parameters(paramLabel = "BASIN")
    String basin;

    @Option(names = {"-b", "--record-size"}, defaultValue = "1024")
    int recordSize;

    @Option(names = {"-t", "--target-mibps"}, defaultValue = "0", description = "throttle writes (0 = unthrottled)")
    double targetMibps;

    @Option(names = {"-d", "--duration"}, defaultValue = "15")
    int durationSec;

    @Option(names = {"-n", "--batch-size"}, defaultValue = "0", description = "records per append batch (0 = client max)")
    int batchSize;

    @Option(names = {"-w", "--in-flight-mib"}, defaultValue = "16",
        description = "max in-flight append MiB (0 = serial appends)")
    int inFlightMib;

    @Override
    public Integer call() throws Exception {
        String streamName = "bench/" + UUID.randomUUID();
        byte[] body = new byte[Math.max(1, recordSize)];
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch readerReady = new CountDownLatch(1);
        BenchStats writeStats = new BenchStats();
        BenchStats readStats = new BenchStats();

        try (S2 s2 = parent.open()) {
            try {
                s2.createBasin(basin);
            } catch (RuntimeException ignored) {
            }

            s2.basin(basin).createStream(streamName);
            Io.err("created " + basin + "/" + streamName);
            Stream stream = s2.basin(basin).stream(streamName);
            Thread reader = new Thread(() -> {
                try (ReadSession session = stream.readSession(
                    new ReadRequest(0L, null, null, false, null, null, null, 25L))) {
                    readerReady.countDown();

                    for (SequencedRecord record : session) {
                        readStats.add(record.body().length);

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
            }, "s2-bench-read");

            reader.setDaemon(true);
            reader.start();

            if (!readerReady.await(10, TimeUnit.SECONDS)) {
                Io.err("reader did not start");
            }

            long start = System.nanoTime();
            long deadline = start + TimeUnit.SECONDS.toNanos(durationSec);
            double targetBps = targetMibps > 0 ? targetMibps * 1024 * 1024 : 0;
            long sent = 0;

            try (Producer producer = stream.producer()) {
                if (batchSize > 0) {
                    producer.maxRecords(batchSize);
                }

                if (inFlightMib > 0) {
                    producer.maxInFlightBytes((long) inFlightMib * 1024 * 1024);
                }

                producer.matchSeqNum(0L);

                while (System.nanoTime() < deadline) {
                    producer.submit(new AppendRecord(null, null, body));
                    sent += body.length;
                    writeStats.add(body.length);

                    if (targetBps > 0) {
                        long expected = (long) (sent / targetBps * 1_000_000_000L);
                        long elapsed = System.nanoTime() - start;

                        if (expected > elapsed) {
                            TimeUnit.NANOSECONDS.sleep(expected - elapsed);
                        }
                    }
                }
            } catch (RuntimeException e) {
                Throwable cause = e;

                while (Objects.nonNull(cause.getCause()) && cause.getCause() != cause) {
                    cause = cause.getCause();
                }

                Io.err("write failed: " + cause);
                stop.set(true);

                return 1;
            }

            stop.set(true);
            reader.join(5_000);
            Io.err(writeStats.summary("Write"));
            Io.err(readStats.summary("Read"));
            s2.basin(basin).deleteStream(streamName);
        }

        return 0;
    }
}
