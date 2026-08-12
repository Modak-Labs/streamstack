package io.streamstack.cli;

import java.util.concurrent.atomic.AtomicLong;

public final class BenchStats {

    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong records = new AtomicLong();
    private final long started = System.nanoTime();

    public void add(long recordBytes) {
        bytes.addAndGet(recordBytes);
        records.incrementAndGet();
    }

    public long bytes() {
        return bytes.get();
    }

    public long records() {
        return records.get();
    }

    public double elapsedSec() {
        return (System.nanoTime() - started) / 1_000_000_000.0;
    }

    public String summary(String label) {
        double sec = Math.max(elapsedSec(), 0.001);
        double mib = bytes.get() / (1024.0 * 1024.0);

        return String.format(
            "%s: %.2f MiB/s, %.0f records/s (%d bytes, %d records in %.2fs)",
            label, mib / sec, records.get() / sec, bytes.get(), records.get(), sec);
    }
}
