package io.streamstack.metadata.raft;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.metadata.ObjectUtils;
import io.streamstack.s3.operator.ObjectStorage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ObjectCleaner {
    public static final int MAX_DELETE_BATCH_COUNT = 2000;

    private final MetadataClient client;
    private final ObjectStorage objectStorage;

    public ObjectCleaner(MetadataClient client, ObjectStorage objectStorage) {
        this.client = client;
        this.objectStorage = objectStorage;
    }

    public List<Long> clean(int limit) throws Exception {
        int batch = Math.min(limit, MAX_DELETE_BATCH_COUNT);
        Map<Long, CompactOperations> marked = client.node().stateMachine().read(() ->
            new LinkedHashMap<>(client.node().stateMachine().objectControlManager().peekDestroyedObjects(batch)));
        if (marked.isEmpty()) {
            return List.of();
        }

        List<Long> deletable = new ArrayList<>();
        List<Long> catalogOnly = new ArrayList<>();
        for (Map.Entry<Long, CompactOperations> entry : marked.entrySet()) {
            if (entry.getValue() == CompactOperations.KEEP_DATA) {
                catalogOnly.add(entry.getKey());
            } else {
                deletable.add(entry.getKey());
            }
        }

        List<Long> cleaned = new ArrayList<>(catalogOnly);
        if (!deletable.isEmpty() && objectStorage != null) {
            List<ObjectStorage.ObjectPath> paths = new ArrayList<>(deletable.size());
            short bucketId = objectStorage.bucketId();
            for (Long objectId : deletable) {
                paths.add(new ObjectStorage.ObjectPath(bucketId, ObjectUtils.genKey(0, objectId)));
            }
            objectStorage.delete(paths).get(30, TimeUnit.SECONDS);
            cleaned.addAll(deletable);
        }

        if (cleaned.isEmpty()) {
            return List.of();
        }
        client.propose(new MetadataCommand.CleanDestroyedObjects(cleaned)).get(30, TimeUnit.SECONDS);
        return cleaned;
    }
}
