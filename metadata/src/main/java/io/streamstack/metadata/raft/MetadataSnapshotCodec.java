package io.streamstack.metadata.raft;

import java.util.Objects;

import io.streamstack.metadata.stream.KVControlManager;
import io.streamstack.metadata.stream.S3ObjectControlManager;
import io.streamstack.metadata.stream.S3ObjectControlManager.OwnedS3Object;
import io.streamstack.metadata.stream.StreamControlManager;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.metadata.S3ObjectMetadata;
import io.streamstack.s3.metadata.S3ObjectType;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamOffsetRange;
import io.streamstack.s3.metadata.StreamState;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class MetadataSnapshotCodec {

    private static final short VERSION = 1;

    private MetadataSnapshotCodec() {
    }

    public static byte[] encode(StreamControlManager streams, S3ObjectControlManager objects) {
        return encode(streams, objects, new KVControlManager());
    }

    public static byte[] encode(StreamControlManager streams, S3ObjectControlManager objects, KVControlManager kv) {
        ByteBuffer buf = ByteBuffer.allocate(estimate(streams, objects, kv));
        buf.putShort(VERSION);
        buf.putLong(streams.nextAssignedStreamId());
        Map<Long, StreamMetadata> streamMap = new TreeMap<>(streams.streamsMetadata());
        buf.putInt(streamMap.size());
        for (StreamMetadata stream : streamMap.values()) {
            buf.putLong(stream.streamId());
            buf.putLong(stream.epoch());
            buf.putLong(stream.startOffset());
            buf.putLong(stream.endOffset());
            buf.put(stream.state().toByte());
            buf.putInt(stream.nodeId());
        }
        Map<Integer, Long> nodeEpochs = new TreeMap<>(streams.nodeEpochs());
        buf.putInt(nodeEpochs.size());
        for (Map.Entry<Integer, Long> entry : nodeEpochs.entrySet()) {
            buf.putInt(entry.getKey());
            buf.putLong(entry.getValue());
        }
        Map<Integer, String> nodeAddresses = new TreeMap<>(streams.nodeAddresses());
        buf.putInt(nodeAddresses.size());
        for (Map.Entry<Integer, String> entry : nodeAddresses.entrySet()) {
            buf.putInt(entry.getKey());
            writeString(buf, entry.getValue());
        }
        buf.putLong(objects.nextAssignedObjectId());
        Map<Long, Long> prepared = new TreeMap<>(objects.preparedObjectDeadlines());
        buf.putInt(prepared.size());
        for (Map.Entry<Long, Long> entry : prepared.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putLong(entry.getValue());
        }
        Map<Long, List<S3ObjectMetadata>> streamObjects = new TreeMap<>(objects.streamObjects());
        buf.putInt(streamObjects.size());
        for (Map.Entry<Long, List<S3ObjectMetadata>> entry : streamObjects.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().size());
            for (S3ObjectMetadata metadata : entry.getValue()) {
                writeObjectMetadata(buf, metadata);
            }
        }
        Map<Long, OwnedS3Object> streamSetObjects = new TreeMap<>(objects.streamSetObjects());
        buf.putInt(streamSetObjects.size());
        for (Map.Entry<Long, OwnedS3Object> entry : streamSetObjects.entrySet()) {
            buf.putLong(entry.getKey());
            buf.putInt(entry.getValue().nodeId());
            writeObjectMetadata(buf, entry.getValue().metadata());
        }
        Map<Long, CompactOperations> markDestroyed = new TreeMap<>(objects.markDestroyedObjects());
        buf.putInt(markDestroyed.size());
        for (Map.Entry<Long, CompactOperations> entry : markDestroyed.entrySet()) {
            buf.putLong(entry.getKey());
            buf.put(entry.getValue().value());
        }
        Map<String, byte[]> kvEntries = kv.snapshot();
        buf.putInt(kvEntries.size());
        for (Map.Entry<String, byte[]> entry : kvEntries.entrySet()) {
            writeString(buf, entry.getKey());
            writeBytes(buf, entry.getValue());
        }
        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    public static void decode(byte[] bytes, StreamControlManager streams, S3ObjectControlManager objects) {
        decode(bytes, streams, objects, new KVControlManager());
    }

    public static void decode(
        byte[] bytes,
        StreamControlManager streams,
        S3ObjectControlManager objects,
        KVControlManager kv) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        short version = buf.getShort();
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported snapshot version " + version);
        }
        long nextStreamId = buf.getLong();
        int streamCount = buf.getInt();
        Map<Long, StreamMetadata> streamMap = new HashMap<>(streamCount);
        for (int i = 0; i < streamCount; i++) {
            long streamId = buf.getLong();
            long epoch = buf.getLong();
            long startOffset = buf.getLong();
            long endOffset = buf.getLong();
            StreamState state = StreamState.fromByte(buf.get());
            int nodeId = buf.getInt();
            StreamMetadata metadata = new StreamMetadata(streamId, epoch, startOffset, endOffset, state);
            metadata.nodeId(nodeId);
            streamMap.put(streamId, metadata);
        }
        int nodeEpochCount = buf.getInt();
        Map<Integer, Long> nodeEpochs = new HashMap<>(nodeEpochCount);
        for (int i = 0; i < nodeEpochCount; i++) {
            nodeEpochs.put(buf.getInt(), buf.getLong());
        }
        int nodeAddressCount = buf.getInt();
        Map<Integer, String> nodeAddresses = new HashMap<>(nodeAddressCount);
        for (int i = 0; i < nodeAddressCount; i++) {
            nodeAddresses.put(buf.getInt(), readString(buf));
        }
        streams.replaceAll(nextStreamId, streamMap, nodeEpochs, nodeAddresses);
        long nextObjectId = buf.getLong();
        int preparedCount = buf.getInt();
        Map<Long, Long> prepared = new HashMap<>(preparedCount);
        for (int i = 0; i < preparedCount; i++) {
            prepared.put(buf.getLong(), buf.getLong());
        }
        int streamObjectKeys = buf.getInt();
        Map<Long, List<S3ObjectMetadata>> streamObjects = new HashMap<>(streamObjectKeys);
        for (int i = 0; i < streamObjectKeys; i++) {
            long streamId = buf.getLong();
            int count = buf.getInt();
            List<S3ObjectMetadata> list = new LinkedList<>();
            for (int j = 0; j < count; j++) {
                list.add(readObjectMetadata(buf));
            }
            streamObjects.put(streamId, list);
        }
        int streamSetCount = buf.getInt();
        Map<Long, OwnedS3Object> streamSetObjects = new HashMap<>(streamSetCount);
        for (int i = 0; i < streamSetCount; i++) {
            long objectId = buf.getLong();
            int nodeId = buf.getInt();
            S3ObjectMetadata metadata = readObjectMetadata(buf);
            streamSetObjects.put(objectId, new OwnedS3Object(nodeId, metadata));
        }
        int destroyedCount = buf.getInt();
        Map<Long, CompactOperations> markDestroyed = new LinkedHashMap<>();
        for (int i = 0; i < destroyedCount; i++) {
            markDestroyed.put(buf.getLong(), CompactOperations.fromValue(buf.get()));
        }
        objects.replaceAll(nextObjectId, prepared, streamObjects, streamSetObjects, markDestroyed);
        int kvCount = buf.getInt();
        Map<String, byte[]> kvEntries = new HashMap<>(kvCount);
        for (int i = 0; i < kvCount; i++) {
            kvEntries.put(readString(buf), readBytes(buf));
        }
        kv.replaceAll(kvEntries);
    }

    private static int estimate(StreamControlManager streams, S3ObjectControlManager objects, KVControlManager kv) {
        int size = 64 + streams.streamsMetadata().size() * 48 + streams.nodeEpochs().size() * 16;
        for (String address : streams.nodeAddresses().values()) {
            size += 8 + stringSize(address);
        }
        size += objects.preparedObjectDeadlines().size() * 16;
        for (List<S3ObjectMetadata> list : objects.streamObjects().values()) {
            size += 16;
            for (S3ObjectMetadata metadata : list) {
                size += objectMetadataSize(metadata);
            }
        }
        for (OwnedS3Object owned : objects.streamSetObjects().values()) {
            size += 16 + objectMetadataSize(owned.metadata());
        }
        size += objects.markDestroyedObjects().size() * 16;
        for (Map.Entry<String, byte[]> entry : kv.entries().entrySet()) {
            size += stringSize(entry.getKey()) + 4 + entry.getValue().length;
        }
        return Math.max(size, 256);
    }

    private static int objectMetadataSize(S3ObjectMetadata metadata) {
        return 48 + metadata.getOffsetRanges().size() * 24;
    }

    private static void writeObjectMetadata(ByteBuffer buf, S3ObjectMetadata metadata) {
        buf.putLong(metadata.objectId());
        buf.putLong(metadata.getOrderId());
        buf.put((byte) metadata.getType().ordinal());
        buf.putLong(metadata.dataTimeInMs());
        buf.putLong(metadata.committedTimestamp());
        buf.putLong(metadata.objectSize());
        buf.putInt(metadata.attributes());
        List<StreamOffsetRange> ranges = metadata.getOffsetRanges();
        buf.putInt(ranges.size());
        for (StreamOffsetRange range : ranges) {
            buf.putLong(range.streamId());
            buf.putLong(range.startOffset());
            buf.putLong(range.endOffset());
        }
    }

    private static S3ObjectMetadata readObjectMetadata(ByteBuffer buf) {
        long objectId = buf.getLong();
        long orderId = buf.getLong();
        S3ObjectType type = S3ObjectType.values()[buf.get()];
        long dataTimeInMs = buf.getLong();
        long committedTimestamp = buf.getLong();
        long objectSize = buf.getLong();
        int attributes = buf.getInt();
        int rangeCount = buf.getInt();
        List<StreamOffsetRange> ranges = new ArrayList<>(rangeCount);
        for (int i = 0; i < rangeCount; i++) {
            ranges.add(new StreamOffsetRange(buf.getLong(), buf.getLong(), buf.getLong()));
        }
        return new S3ObjectMetadata(
            objectId, type, ranges, dataTimeInMs, committedTimestamp, objectSize, orderId, attributes);
    }

    private static void writeString(ByteBuffer buf, String value) {
        byte[] bytes = Objects.isNull(value) ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        buf.putInt(bytes.length);
        buf.put(bytes);
    }

    private static String readString(ByteBuffer buf) {
        int length = buf.getInt();
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeBytes(ByteBuffer buf, byte[] value) {
        buf.putInt(value.length);
        buf.put(value);
    }

    private static byte[] readBytes(ByteBuffer buf) {
        int length = buf.getInt();
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return bytes;
    }

    private static int stringSize(String value) {
        return 4 + (Objects.isNull(value) ? 0 : value.getBytes(StandardCharsets.UTF_8).length);
    }
}
