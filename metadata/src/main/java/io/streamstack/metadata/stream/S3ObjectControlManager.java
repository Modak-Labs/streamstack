package io.streamstack.metadata.stream;

import java.util.Objects;

import io.streamstack.metadata.MetadataException;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.metadata.ObjectUtils;
import io.streamstack.s3.metadata.S3ObjectMetadata;
import io.streamstack.s3.metadata.S3ObjectType;
import io.streamstack.s3.metadata.StreamOffsetRange;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectStreamRange;
import io.streamstack.s3.objects.StreamObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class S3ObjectControlManager {

    private long nextAssignedObjectId;

    private final Map<Long, Long> preparedObjectDeadlines = new HashMap<>();

    private final Map<Long, List<S3ObjectMetadata>> streamObjects = new HashMap<>();

    private final Map<Long, OwnedS3Object> streamSetObjects = new HashMap<>();

    private final Map<Long, CompactOperations> markDestroyedObjects = new LinkedHashMap<>();

    private final StreamControlManager streamControlManager;

    public S3ObjectControlManager(StreamControlManager streamControlManager) {
        this.streamControlManager = streamControlManager;
        this.nextAssignedObjectId = 0L;
    }

    public long nextAssignedObjectId() {
        return nextAssignedObjectId;
    }

    public Map<Long, Long> preparedObjectDeadlines() {
        return preparedObjectDeadlines;
    }

    public Map<Long, List<S3ObjectMetadata>> streamObjects() {
        return streamObjects;
    }

    public Map<Long, OwnedS3Object> streamSetObjects() {
        return streamSetObjects;
    }

    public Map<Long, CompactOperations> markDestroyedObjects() {
        return markDestroyedObjects;
    }

    public long prepareObject(int nodeId, long nodeEpoch, int count, long ttlMs, long nowMs) {
        streamControlManager.nodeEpochCheck(nodeId, nodeEpoch);
        if (count <= 0) {
            throw MetadataException.unexpected("prepare count must be positive");
        }
        long first = nextAssignedObjectId;
        nextAssignedObjectId += count;
        long deadline = nowMs + Math.max(0L, ttlMs);
        for (long id = first; id < first + count; id++) {
            preparedObjectDeadlines.put(id, deadline);
        }
        return first;
    }

    public int expirePreparedObjects(long nowMs) {
        int removed = 0;
        Iterator<Map.Entry<Long, Long>> it = preparedObjectDeadlines.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
            if (entry.getValue() <= nowMs) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    public void commitStreamSetObject(int nodeId, long nodeEpoch, CommitStreamSetObjectRequest request, long nowMs) {
        streamControlManager.nodeEpochCheck(nodeId, nodeEpoch);
        redundantCommitCheck(request);
        long dataTimeInMs = nowMs;
        boolean compact = !request.getCompactedObjectIds().isEmpty();
        if (compact) {
            List<Long> compactedIds = request.getCompactedObjectIds();
            for (long id : compactedIds) {
                OwnedS3Object owned = streamSetObjects.get(id);
                if (Objects.isNull(owned)) {
                    throw MetadataException.unexpected("compacted stream-set object " + id + " not found");
                }
                dataTimeInMs = Math.min(owned.metadata().dataTimeInMs(), dataTimeInMs);
            }
            for (long id : compactedIds) {
                streamSetObjects.remove(id);
                markDestroyedObjects.put(id, CompactOperations.DELETE);
            }
        }
        if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
            commitPrepared(request.getObjectId());
            for (ObjectStreamRange range : request.getStreamRanges()) {
                streamControlManager.advanceEndOffset(
                    range.getStreamId(), range.getStartOffset(), range.getEndOffset(), compact);
            }
            S3ObjectMetadata object = new S3ObjectMetadata(
                request.getObjectId(),
                S3ObjectType.STREAM_SET,
                request.getStreamRanges().stream().map(S3ObjectControlManager::to).collect(Collectors.toList()),
                dataTimeInMs,
                nowMs,
                request.getObjectSize(),
                request.getOrderId());
            streamSetObjects.put(request.getObjectId(), new OwnedS3Object(nodeId, object));
        }
        for (StreamObject streamObject : request.getStreamObjects()) {
            commitPrepared(streamObject.getObjectId());
            long streamId = streamObject.getStreamId();
            streamControlManager.advanceEndOffset(
                streamId, streamObject.getStartOffset(), streamObject.getEndOffset(), compact);
            streamObjectList(streamId).add(
                new S3ObjectMetadata(
                    streamObject.getObjectId(),
                    S3ObjectType.STREAM,
                    List.of(new StreamOffsetRange(streamId, streamObject.getStartOffset(), streamObject.getEndOffset())),
                    dataTimeInMs,
                    nowMs,
                    streamObject.getObjectSize(),
                    0));
        }
    }

    private void redundantCommitCheck(CommitStreamSetObjectRequest request) {
        if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID) {
            if (streamSetObjects.containsKey(request.getObjectId())) {
                throw MetadataException.redundant("object " + request.getObjectId() + " already committed");
            }
            return;
        }
        List<StreamObject> streamObjectsInRequest = request.getStreamObjects();
        if (streamObjectsInRequest.isEmpty()) {
            return;
        }
        boolean allCommitted = streamObjectsInRequest.stream().allMatch(
            so -> streamObjectCommitted(so.getStreamId(), so.getObjectId()));
        if (allCommitted) {
            throw MetadataException.redundant("all stream objects in commit already committed");
        }
    }

    public void compactStreamObject(int nodeId, long nodeEpoch, CompactStreamObjectRequest request, long nowMs) {
        streamControlManager.nodeEpochCheck(nodeId, nodeEpoch);
        long streamId = request.getStreamId();
        if (request.getObjectId() != ObjectUtils.NOOP_OBJECT_ID
            && streamObjectCommitted(streamId, request.getObjectId())) {
            throw MetadataException.redundant(
                "compact object " + request.getObjectId() + " already committed for stream " + streamId);
        }
        var stream = streamControlManager.getStream(streamId);
        if (Objects.isNull(stream)) {
            throw MetadataException.streamNotExist(streamId);
        }
        if (stream.epoch() != request.getStreamEpoch()) {
            throw MetadataException.expiredEpoch(
                "stream " + streamId + " epoch " + stream.epoch()
                    + " is not equal to request " + request.getStreamEpoch());
        }
        if (stream.endOffset() < request.getEndOffset()) {
            throw MetadataException.unexpected(
                "stream " + streamId + " end offset " + stream.endOffset()
                    + " is lesser than request " + request.getEndOffset());
        }
        if (stream.startOffset() > request.getStartOffset()) {
            throw MetadataException.unexpected(
                "stream " + streamId + " start offset " + stream.startOffset()
                    + " is greater than request " + request.getStartOffset());
        }
        commitPrepared(request.getObjectId());
        streamObjectList(streamId).add(
            new S3ObjectMetadata(
                request.getObjectId(),
                S3ObjectType.STREAM,
                List.of(new StreamOffsetRange(streamId, request.getStartOffset(), request.getEndOffset())),
                nowMs,
                nowMs,
                request.getObjectSize(),
                0));
        HashSet<Long> idSet = new HashSet<>(request.getSourceObjectIds());
        streamObjectList(streamId).removeIf(metadata -> idSet.contains(metadata.objectId()));
        markDestroyObjects(request.getSourceObjectIds(), request.getOperations());
    }

    public void markDestroyObjects(List<Long> ids, List<CompactOperations> ops) {
        if (Objects.isNull(ids) || ids.isEmpty()) {
            return;
        }
        if (Objects.isNull(ops) || ops.isEmpty()) {
            for (Long id : ids) {
                markDestroyedObjects.put(id, CompactOperations.DELETE);
            }
            return;
        }
        if (ops.size() != ids.size()) {
            throw MetadataException.unexpected(
                "mark destroy ids size " + ids.size() + " does not match operations size " + ops.size());
        }
        for (int i = 0; i < ids.size(); i++) {
            markDestroyedObjects.put(ids.get(i), ops.get(i));
        }
    }

    public Map<Long, CompactOperations> peekDestroyedObjects(int limit) {
        Map<Long, CompactOperations> result = new LinkedHashMap<>();
        for (Map.Entry<Long, CompactOperations> entry : markDestroyedObjects.entrySet()) {
            if (result.size() >= limit) {
                break;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public void cleanDestroyedObjects(List<Long> objectIds) {
        for (Long id : objectIds) {
            markDestroyedObjects.remove(id);
        }
    }

    public void onStreamDeleted(long streamId) {
        List<S3ObjectMetadata> objects = streamObjects.remove(streamId);
        if (Objects.nonNull(objects)) {
            for (S3ObjectMetadata metadata : objects) {
                markDestroyedObjects.put(metadata.objectId(), CompactOperations.DELETE);
            }
        }
    }

    public List<S3ObjectMetadata> getObjects(long streamId, long startOffset, long endOffset, int limit) {
        List<S3ObjectMetadata> streamSetObjectList = streamSetObjects.values().stream()
            .map(OwnedS3Object::metadata)
            .filter(o -> matchesRange(o, streamId, startOffset, endOffset))
            .collect(Collectors.toList());
        List<S3ObjectMetadata> streamObjectList = streamObjectList(streamId).stream()
            .filter(o -> matchesRange(o, streamId, startOffset, endOffset))
            .collect(Collectors.toList());
        List<S3ObjectMetadata> result = new ArrayList<>();
        result.addAll(streamSetObjectList);
        result.addAll(streamObjectList);
        result.sort((o1, o2) -> Long.compare(rangeStart(o1, streamId), rangeStart(o2, streamId)));
        return result.stream().limit(limit).collect(Collectors.toList());
    }

    public List<S3ObjectMetadata> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
        return streamObjectList(streamId).stream()
            .filter(o -> matchesRange(o, streamId, startOffset, endOffset))
            .limit(limit)
            .collect(Collectors.toList());
    }

    public List<S3ObjectMetadata> getServerObjects(int nodeId) {
        return streamSetObjects.values().stream()
            .filter(owned -> owned.nodeId() == nodeId)
            .map(OwnedS3Object::metadata)
            .collect(Collectors.toList());
    }

    public boolean isObjectExist(long objectId) {
        if (streamSetObjects.containsKey(objectId)) {
            return true;
        }
        if (preparedObjectDeadlines.containsKey(objectId)) {
            return true;
        }
        for (List<S3ObjectMetadata> list : streamObjects.values()) {
            if (list.stream().anyMatch(m -> m.objectId() == objectId)) {
                return true;
            }
        }
        return false;
    }

    public int getObjectsCount() {
        int count = streamSetObjects.size();
        for (List<S3ObjectMetadata> list : streamObjects.values()) {
            count += list.size();
        }
        return count;
    }

    public void replaceAll(
        long nextAssignedObjectId,
        Map<Long, Long> prepared,
        Map<Long, List<S3ObjectMetadata>> streamObjects,
        Map<Long, OwnedS3Object> streamSetObjects,
        Map<Long, CompactOperations> markDestroyed) {
        this.nextAssignedObjectId = nextAssignedObjectId;
        this.preparedObjectDeadlines.clear();
        this.preparedObjectDeadlines.putAll(prepared);
        this.streamObjects.clear();
        this.streamObjects.putAll(streamObjects);
        this.streamSetObjects.clear();
        this.streamSetObjects.putAll(streamSetObjects);
        this.markDestroyedObjects.clear();
        this.markDestroyedObjects.putAll(markDestroyed);
    }

    private boolean streamObjectCommitted(long streamId, long objectId) {
        List<S3ObjectMetadata> list = streamObjects.get(streamId);
        if (Objects.isNull(list)) {
            return false;
        }
        return list.stream().anyMatch(m -> m.objectId() == objectId);
    }

    private void commitPrepared(long objectId) {
        preparedObjectDeadlines.remove(objectId);
    }

    private List<S3ObjectMetadata> streamObjectList(long streamId) {
        return streamObjects.computeIfAbsent(streamId, id -> new LinkedList<>());
    }

    private static StreamOffsetRange to(ObjectStreamRange s) {
        return new StreamOffsetRange(s.getStreamId(), s.getStartOffset(), s.getEndOffset());
    }

    private static boolean matchesRange(S3ObjectMetadata object, long streamId, long startOffset, long endOffset) {
        return object.getOffsetRanges().stream().anyMatch(
            r -> r.streamId() == streamId
                && r.endOffset() > startOffset
                && (r.startOffset() < endOffset || endOffset == -1));
    }

    private static long rangeStart(S3ObjectMetadata object, long streamId) {
        return object.getOffsetRanges().stream()
            .filter(r -> r.streamId() == streamId)
            .findFirst()
            .orElseThrow()
            .startOffset();
    }

    public static final class OwnedS3Object {
        private final int nodeId;
        private final S3ObjectMetadata metadata;
        public OwnedS3Object(int nodeId, S3ObjectMetadata metadata) {
            this.nodeId = nodeId;
            this.metadata = metadata;
        }
        public int nodeId() {
            return nodeId;
        }
        public S3ObjectMetadata metadata() {
            return metadata;
        }
    }
}
