package io.streamstack.metadata.stream;

import io.streamstack.metadata.MetadataException;
import io.streamstack.metadata.raft.MetadataSnapshotCodec;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectStreamRange;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StreamControlManagerTest {

    private static final int NODE_1 = 1;
    private static final int NODE_2 = 2;
    private static final long EPOCH_1 = 10L;
    private static final long EPOCH_2 = 20L;

    StreamControlManager streams;
    S3ObjectControlManager objects;

    @BeforeEach
    void setup() {
        streams = new StreamControlManager();
        objects = new S3ObjectControlManager(streams);
        streams.registerNode(NODE_1, EPOCH_1);
        streams.registerNode(NODE_2, EPOCH_2);
    }

    @Test
    void writesRequireRegisteredNodeEpoch() {
        MetadataException e = assertThrows(MetadataException.class, () -> streams.createStream(3, 1L));
        assertEquals(MetadataException.NODE_EPOCH_MISMATCH, e.code());
        e = assertThrows(MetadataException.class, () -> streams.createStream(NODE_1, EPOCH_1 + 1));
        assertEquals(MetadataException.NODE_EPOCH_MISMATCH, e.code());
    }

    @Test
    void staleNodeEpochIsFencedAfterReRegistration() {
        streams.registerNode(NODE_1, EPOCH_1 + 5);
        MetadataException e = assertThrows(MetadataException.class, () -> streams.createStream(NODE_1, EPOCH_1));
        assertEquals(MetadataException.NODE_EPOCH_MISMATCH, e.code());
        e = assertThrows(MetadataException.class, () -> streams.registerNode(NODE_1, EPOCH_1));
        assertEquals(MetadataException.NODE_EPOCH_MISMATCH, e.code());
    }

    @Test
    void openStreamLifecycleAndFencing() {
        long streamId = streams.createStream(NODE_1, EPOCH_1);
        StreamMetadata opened = streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        assertEquals(StreamState.OPENED, opened.state());
        StreamMetadata retried = streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        assertEquals(StreamState.OPENED, retried.state());
        MetadataException e = assertThrows(MetadataException.class,
            () -> streams.openStream(NODE_2, EPOCH_2, streamId, 1));
        assertEquals(MetadataException.STREAM_FENCED, e.code());
        e = assertThrows(MetadataException.class,
            () -> streams.openStream(NODE_2, EPOCH_2, streamId, 0));
        assertEquals(MetadataException.STREAM_FENCED, e.code());
        e = assertThrows(MetadataException.class,
            () -> streams.openStream(NODE_2, EPOCH_2, streamId, 2));
        assertEquals(MetadataException.STREAM_NOT_CLOSED, e.code());
        streams.closeStream(NODE_1, EPOCH_1, streamId, 1);
        StreamMetadata reopened = streams.openStream(NODE_2, EPOCH_2, streamId, 2);
        assertEquals(NODE_2, reopened.nodeId());
    }

    @Test
    void closeStreamIsIdempotent() {
        long streamId = streams.createStream(NODE_1, EPOCH_1);
        streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        streams.closeStream(NODE_1, EPOCH_1, streamId, 1);
        streams.closeStream(NODE_1, EPOCH_1, streamId, 1);
        assertEquals(StreamState.CLOSED, streams.getStream(streamId).state());
    }

    @Test
    void deleteStreamIsIdempotentAndMarksObjectsDestroyed() {
        long streamId = streams.createStream(NODE_1, EPOCH_1);
        streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        long objectId = objects.prepareObject(NODE_1, EPOCH_1, 1, 60_000, 0);
        CompactStreamObjectRequest compact = new CompactStreamObjectRequest(
            objectId, 10, streamId, 1, 0, 0, List.of(), List.of(), 0);
        objects.compactStreamObject(NODE_1, EPOCH_1, compact, 1);
        streams.closeStream(NODE_1, EPOCH_1, streamId, 1);
        streams.deleteStream(NODE_1, EPOCH_1, streamId, 1);
        objects.onStreamDeleted(streamId);
        streams.deleteStream(NODE_1, EPOCH_1, streamId, 1);
        objects.onStreamDeleted(streamId);
        assertEquals(CompactOperations.DELETE, objects.markDestroyedObjects().get(objectId));
    }

    @Test
    void getOpeningStreamsFiltersByNode() {
        long stream1 = streams.createStream(NODE_1, EPOCH_1);
        long stream2 = streams.createStream(NODE_2, EPOCH_2);
        streams.openStream(NODE_1, EPOCH_1, stream1, 1);
        streams.openStream(NODE_2, EPOCH_2, stream2, 1);
        assertEquals(1, streams.getOpeningStreams(NODE_1).size());
        assertEquals(stream1, streams.getOpeningStreams(NODE_1).get(0).streamId());
        assertEquals(1, streams.getOpeningStreams(NODE_2).size());
        assertEquals(stream2, streams.getOpeningStreams(NODE_2).get(0).streamId());
    }

    @Test
    void commitAdvancesEndOffsetAndIsRedundantOnRetry() {
        long streamId = streams.createStream(NODE_1, EPOCH_1);
        streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        long objectId = objects.prepareObject(NODE_1, EPOCH_1, 1, 60_000, 0);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();
        request.setObjectId(objectId);
        request.setOrderId(objectId);
        request.setObjectSize(64);
        request.setStreamRanges(List.of(new ObjectStreamRange(streamId, 1, 0, 8, 64)));
        objects.commitStreamSetObject(NODE_1, EPOCH_1, request, 1);
        assertEquals(8, streams.getStream(streamId).endOffset());
        MetadataException e = assertThrows(MetadataException.class,
            () -> objects.commitStreamSetObject(NODE_1, EPOCH_1, request, 2));
        assertTrue(e.isRedundant());
        assertEquals(8, streams.getStream(streamId).endOffset());
        assertEquals(1, objects.getObjectsCount());
    }

    @Test
    void compactRetryIsRedundant() {
        long streamId = streams.createStream(NODE_1, EPOCH_1);
        streams.openStream(NODE_1, EPOCH_1, streamId, 1);
        long objectId = objects.prepareObject(NODE_1, EPOCH_1, 1, 60_000, 0);
        CompactStreamObjectRequest compact = new CompactStreamObjectRequest(
            objectId, 10, streamId, 1, 0, 0, List.of(), List.of(), 0);
        objects.compactStreamObject(NODE_1, EPOCH_1, compact, 1);
        MetadataException e = assertThrows(MetadataException.class,
            () -> objects.compactStreamObject(NODE_1, EPOCH_1, compact, 2));
        assertTrue(e.isRedundant());
        assertEquals(1, objects.getObjectsCount());
    }

    @Test
    void markDestroyRejectsMismatchedSizes() {
        MetadataException e = assertThrows(MetadataException.class,
            () -> objects.markDestroyObjects(List.of(1L, 2L), List.of(CompactOperations.DELETE)));
        assertEquals(MetadataException.UNEXPECTED, e.code());
    }

    @Test
    void peekAndCleanDestroyedObjects() {
        objects.markDestroyObjects(
            List.of(1L, 2L, 3L),
            List.of(CompactOperations.KEEP_DATA, CompactOperations.DELETE, CompactOperations.DEEP_DELETE));
        var peeked = objects.peekDestroyedObjects(10);
        assertEquals(3, peeked.size());
        assertEquals(3, objects.markDestroyedObjects().size());
        objects.cleanDestroyedObjects(List.of(1L, 2L));
        assertEquals(1, objects.markDestroyedObjects().size());
        assertEquals(CompactOperations.DEEP_DELETE, objects.markDestroyedObjects().get(3L));
    }

    @Test
    void expirePreparedObjectsRemovesOnlyExpired() {
        objects.prepareObject(NODE_1, EPOCH_1, 2, 100, 0);
        objects.prepareObject(NODE_1, EPOCH_1, 1, 10_000, 0);
        assertEquals(2, objects.expirePreparedObjects(200));
        assertEquals(1, objects.preparedObjectDeadlines().size());
    }

    @Test
    void applyingSameOperationsProducesIdenticalState() {
        long seed = 424242L;
        byte[] first = runScriptedOperations(seed);
        byte[] second = runScriptedOperations(seed);
        assertTrue(Arrays.equals(first, second));
    }

    private static byte[] runScriptedOperations(long seed) {
        StreamControlManager streams = new StreamControlManager();
        S3ObjectControlManager objects = new S3ObjectControlManager(streams);
        streams.registerNode(NODE_1, EPOCH_1);
        Random random = new Random(seed);
        for (int i = 0; i < 200; i++) {
            int op = random.nextInt(4);
            try {
                switch (op) {
                    case 0 -> streams.createStream(NODE_1, EPOCH_1);
                    case 1 -> {
                        long streamId = random.nextInt(20);
                        streams.openStream(NODE_1, EPOCH_1, streamId, random.nextInt(5));
                    }
                    case 2 -> {
                        long streamId = random.nextInt(20);
                        streams.closeStream(NODE_1, EPOCH_1, streamId, random.nextInt(5));
                    }
                    default -> objects.prepareObject(NODE_1, EPOCH_1, 1 + random.nextInt(3), 1000, i);
                }
            } catch (MetadataException ignored) {
            }
        }
        return MetadataSnapshotCodec.encode(streams, objects);
    }
}
