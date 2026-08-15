package io.streamstack.metadata.raft;

import io.netty.buffer.Unpooled;
import io.streamstack.s3.operator.MemoryObjectStorage;
import io.streamstack.s3.operator.ObjectStorage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectStorageSnapshotArchiveTest {

    @Test
    void archivesLatestAndReadsBack() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();

        try (ObjectStorageSnapshotArchive archive =
                 new ObjectStorageSnapshotArchive(storage, "_streamstack/metadata/test/snapshots", 5)) {
            byte[] first = "snapshot-one".getBytes(StandardCharsets.UTF_8);
            byte[] second = "snapshot-two".getBytes(StandardCharsets.UTF_8);

            archive.submit(3, first);
            await(() -> archive.successCount() == 1);
            archive.submit(7, second);
            await(() -> archive.successCount() == 2);

            List<SnapshotArchive.ArchivedSnapshot> snapshots = archive.list();

            assertEquals(2, snapshots.size());
            assertEquals(3, snapshots.get(0).appliedIndex());
            assertEquals(7, snapshots.get(1).appliedIndex());

            Optional<SnapshotArchive.ArchivedSnapshot> latest = archive.latest();

            assertTrue(latest.isPresent());
            assertEquals(7, latest.get().appliedIndex());
            assertEquals(second.length, latest.get().size());
            assertArrayEquals(second, archive.read(latest.get()));
            assertArrayEquals(first, archive.read(snapshots.get(0)));
            assertEquals(7, archive.lastArchivedIndex());
            assertEquals(0, archive.failureCount());
        }
    }

    @Test
    void prunesBeyondRetainCount() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();

        try (ObjectStorageSnapshotArchive archive =
                 new ObjectStorageSnapshotArchive(storage, "snapshots/", 2)) {
            for (int i = 1; i <= 4; i++) {
                byte[] bytes = ("snapshot-" + i).getBytes(StandardCharsets.UTF_8);
                long expected = i;

                archive.submit(i, bytes);
                await(() -> archive.successCount() == expected);
            }

            List<SnapshotArchive.ArchivedSnapshot> snapshots = archive.list();

            assertEquals(2, snapshots.size());
            assertEquals(3, snapshots.get(0).appliedIndex());
            assertEquals(4, snapshots.get(1).appliedIndex());
        }
    }

    @Test
    void ignoresForeignKeysUnderPrefix() throws Exception {
        MemoryObjectStorage storage = new MemoryObjectStorage();

        storage.write(new ObjectStorage.WriteOptions(), "snapshots/not-a-snapshot.txt",
            Unpooled.wrappedBuffer("junk".getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);

        try (ObjectStorageSnapshotArchive archive =
                 new ObjectStorageSnapshotArchive(storage, "snapshots", 5)) {
            assertTrue(archive.list().isEmpty());
            assertTrue(archive.latest().isEmpty());
        }
    }

    @Test
    void rejectsInvalidConfiguration() {
        MemoryObjectStorage storage = new MemoryObjectStorage();

        assertThrows(IllegalArgumentException.class,
            () -> new ObjectStorageSnapshotArchive(storage, " ", 5));
        assertThrows(IllegalArgumentException.class,
            () -> new ObjectStorageSnapshotArchive(storage, "snapshots", 0));
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);

        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }

            Thread.sleep(20);
        }

        throw new AssertionError("condition not met within timeout");
    }
}
