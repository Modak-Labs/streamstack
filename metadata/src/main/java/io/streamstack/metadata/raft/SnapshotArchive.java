package io.streamstack.metadata.raft;

import java.util.List;
import java.util.Optional;

public interface SnapshotArchive extends AutoCloseable {

    void submit(long appliedIndex, byte[] snapshotBytes);

    List<ArchivedSnapshot> list();

    Optional<ArchivedSnapshot> latest();

    byte[] read(ArchivedSnapshot snapshot);

    long successCount();

    long failureCount();

    long lastArchivedIndex();

    @Override
    void close();

    record ArchivedSnapshot(String key, long appliedIndex, long timestampMs, long size) {
    }
}
