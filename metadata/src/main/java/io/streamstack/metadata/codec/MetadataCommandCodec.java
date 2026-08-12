package io.streamstack.metadata.codec;

import java.util.Objects;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectStreamRange;
import io.streamstack.s3.objects.StreamObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class MetadataCommandCodec {

    private static final short VERSION = 1;

    private MetadataCommandCodec() {
    }

    public static byte[] encode(MetadataCommand command) {
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(command));

        buf.putShort(VERSION);
        buf.put(command.type());

        switch (command.type()) {
            case MetadataCommand.REGISTER_NODE -> {
                MetadataCommand.RegisterNode c = (MetadataCommand.RegisterNode) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                writeString(buf, c.httpAddress());
            }
            case MetadataCommand.CREATE_STREAM -> {
                MetadataCommand.CreateStream c = (MetadataCommand.CreateStream) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
            }
            case MetadataCommand.OPEN_STREAM -> {
                MetadataCommand.OpenStream c = (MetadataCommand.OpenStream) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.streamId());
                buf.putLong(c.epoch());
            }
            case MetadataCommand.TRIM_STREAM -> {
                MetadataCommand.TrimStream c = (MetadataCommand.TrimStream) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.streamId());
                buf.putLong(c.epoch());
                buf.putLong(c.newStartOffset());
            }
            case MetadataCommand.CLOSE_STREAM -> {
                MetadataCommand.CloseStream c = (MetadataCommand.CloseStream) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.streamId());
                buf.putLong(c.epoch());
            }
            case MetadataCommand.DELETE_STREAM -> {
                MetadataCommand.DeleteStream c = (MetadataCommand.DeleteStream) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.streamId());
                buf.putLong(c.epoch());
            }
            case MetadataCommand.PREPARE_OBJECT -> {
                MetadataCommand.PrepareObject c = (MetadataCommand.PrepareObject) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putInt(c.count());
                buf.putLong(c.ttlMs());
                buf.putLong(c.nowMs());
            }
            case MetadataCommand.COMMIT_STREAM_SET_OBJECT -> {
                MetadataCommand.CommitStreamSetObject c = (MetadataCommand.CommitStreamSetObject) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.nowMs());
                writeCommitRequest(buf, c.request());
            }
            case MetadataCommand.COMPACT_STREAM_OBJECT -> {
                MetadataCommand.CompactStreamObject c = (MetadataCommand.CompactStreamObject) command;

                buf.putInt(c.nodeId());
                buf.putLong(c.nodeEpoch());
                buf.putLong(c.nowMs());
                writeCompactRequest(buf, c.request());
            }
            case MetadataCommand.EXPIRE_PREPARED_OBJECTS -> {
                MetadataCommand.ExpirePreparedObjects c = (MetadataCommand.ExpirePreparedObjects) command;

                buf.putLong(c.nowMs());
            }
            case MetadataCommand.CLEAN_DESTROYED_OBJECTS -> {
                MetadataCommand.CleanDestroyedObjects c = (MetadataCommand.CleanDestroyedObjects) command;

                buf.putInt(c.objectIds().size());

                for (Long id : c.objectIds()) {
                    buf.putLong(id);
                }
            }
            case MetadataCommand.PUT_KV -> {
                MetadataCommand.PutKV c = (MetadataCommand.PutKV) command;

                writeString(buf, c.key());
                writeBytes(buf, c.value());
            }
            case MetadataCommand.PUT_KV_IF_ABSENT -> {
                MetadataCommand.PutKVIfAbsent c = (MetadataCommand.PutKVIfAbsent) command;

                writeString(buf, c.key());
                writeBytes(buf, c.value());
            }
            case MetadataCommand.DELETE_KV -> {
                MetadataCommand.DeleteKV c = (MetadataCommand.DeleteKV) command;

                writeString(buf, c.key());
            }
            default -> throw new IllegalArgumentException("unknown command type " + command.type());
        }

        byte[] out = new byte[buf.position()];

        buf.flip();
        buf.get(out);

        return out;
    }

    public static MetadataCommand decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        short version = buf.getShort();

        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported metadata command version " + version);
        }

        byte type = buf.get();

        return switch (type) {
            case MetadataCommand.REGISTER_NODE ->
                new MetadataCommand.RegisterNode(buf.getInt(), buf.getLong(), readString(buf));
            case MetadataCommand.CREATE_STREAM ->
                new MetadataCommand.CreateStream(buf.getInt(), buf.getLong());
            case MetadataCommand.OPEN_STREAM ->
                new MetadataCommand.OpenStream(buf.getInt(), buf.getLong(), buf.getLong(), buf.getLong());
            case MetadataCommand.TRIM_STREAM ->
                new MetadataCommand.TrimStream(buf.getInt(), buf.getLong(), buf.getLong(), buf.getLong(), buf.getLong());
            case MetadataCommand.CLOSE_STREAM ->
                new MetadataCommand.CloseStream(buf.getInt(), buf.getLong(), buf.getLong(), buf.getLong());
            case MetadataCommand.DELETE_STREAM ->
                new MetadataCommand.DeleteStream(buf.getInt(), buf.getLong(), buf.getLong(), buf.getLong());
            case MetadataCommand.PREPARE_OBJECT ->
                new MetadataCommand.PrepareObject(buf.getInt(), buf.getLong(), buf.getInt(), buf.getLong(), buf.getLong());
            case MetadataCommand.COMMIT_STREAM_SET_OBJECT -> {
                int nodeId = buf.getInt();
                long nodeEpoch = buf.getLong();
                long nowMs = buf.getLong();

                yield new MetadataCommand.CommitStreamSetObject(nodeId, nodeEpoch, readCommitRequest(buf), nowMs);
            }
            case MetadataCommand.COMPACT_STREAM_OBJECT -> {
                int nodeId = buf.getInt();
                long nodeEpoch = buf.getLong();
                long nowMs = buf.getLong();

                yield new MetadataCommand.CompactStreamObject(nodeId, nodeEpoch, readCompactRequest(buf), nowMs);
            }
            case MetadataCommand.EXPIRE_PREPARED_OBJECTS ->
                new MetadataCommand.ExpirePreparedObjects(buf.getLong());
            case MetadataCommand.CLEAN_DESTROYED_OBJECTS -> {
                int count = buf.getInt();
                List<Long> ids = new ArrayList<>(count);

                for (int i = 0; i < count; i++) {
                    ids.add(buf.getLong());
                }

                yield new MetadataCommand.CleanDestroyedObjects(ids);
            }
            case MetadataCommand.PUT_KV ->
                new MetadataCommand.PutKV(readString(buf), readBytes(buf));
            case MetadataCommand.PUT_KV_IF_ABSENT ->
                new MetadataCommand.PutKVIfAbsent(readString(buf), readBytes(buf));
            case MetadataCommand.DELETE_KV ->
                new MetadataCommand.DeleteKV(readString(buf));
            default -> throw new IllegalArgumentException("unknown command type " + type);
        };
    }

    private static int estimateSize(MetadataCommand command) {
        return switch (command.type()) {
            case MetadataCommand.REGISTER_NODE -> {
                MetadataCommand.RegisterNode c = (MetadataCommand.RegisterNode) command;

                yield 24 + stringSize(c.httpAddress());
            }
            case MetadataCommand.CREATE_STREAM -> 24;
            case MetadataCommand.OPEN_STREAM, MetadataCommand.CLOSE_STREAM, MetadataCommand.DELETE_STREAM -> 40;
            case MetadataCommand.TRIM_STREAM -> 48;
            case MetadataCommand.PREPARE_OBJECT -> 48;
            case MetadataCommand.COMMIT_STREAM_SET_OBJECT -> {
                MetadataCommand.CommitStreamSetObject c = (MetadataCommand.CommitStreamSetObject) command;

                yield 72 + commitRequestSize(c.request());
            }
            case MetadataCommand.COMPACT_STREAM_OBJECT -> {
                MetadataCommand.CompactStreamObject c = (MetadataCommand.CompactStreamObject) command;

                yield 48 + compactRequestSize(c.request());
            }
            case MetadataCommand.EXPIRE_PREPARED_OBJECTS -> 16;
            case MetadataCommand.CLEAN_DESTROYED_OBJECTS -> {
                MetadataCommand.CleanDestroyedObjects c = (MetadataCommand.CleanDestroyedObjects) command;

                yield 16 + c.objectIds().size() * 8;
            }
            case MetadataCommand.PUT_KV -> {
                MetadataCommand.PutKV c = (MetadataCommand.PutKV) command;

                yield 16 + stringSize(c.key()) + bytesSize(c.value());
            }
            case MetadataCommand.PUT_KV_IF_ABSENT -> {
                MetadataCommand.PutKVIfAbsent c = (MetadataCommand.PutKVIfAbsent) command;

                yield 16 + stringSize(c.key()) + bytesSize(c.value());
            }
            case MetadataCommand.DELETE_KV -> {
                MetadataCommand.DeleteKV c = (MetadataCommand.DeleteKV) command;

                yield 16 + stringSize(c.key());
            }
            default -> 256;
        };
    }

    private static int commitRequestSize(CommitStreamSetObjectRequest request) {
        return 64
            + request.getStreamRanges().size() * 40
            + request.getStreamObjects().size() * 48
            + request.getCompactedObjectIds().size() * 8;
    }

    private static int compactRequestSize(CompactStreamObjectRequest request) {
        return 64
            + request.getSourceObjectIds().size() * 8
            + request.getOperations().size();
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

    private static int bytesSize(byte[] value) {
        return 4 + value.length;
    }

    private static void writeCommitRequest(ByteBuffer buf, CommitStreamSetObjectRequest request) {
        buf.putLong(request.getObjectId());
        buf.putLong(request.getOrderId());
        buf.putLong(request.getObjectSize());
        buf.putInt(request.getAttributes());
        List<ObjectStreamRange> ranges = request.getStreamRanges();

        buf.putInt(ranges.size());

        for (ObjectStreamRange range : ranges) {
            buf.putLong(range.getStreamId());
            buf.putLong(range.getEpoch());
            buf.putLong(range.getStartOffset());
            buf.putLong(range.getEndOffset());
            buf.putInt(range.getSize());
        }

        List<StreamObject> streamObjects = request.getStreamObjects();

        buf.putInt(streamObjects.size());

        for (StreamObject object : streamObjects) {
            buf.putLong(object.getObjectId());
            buf.putLong(object.getObjectSize());
            buf.putLong(object.getStreamId());
            buf.putLong(object.getStartOffset());
            buf.putLong(object.getEndOffset());
            buf.putInt(object.getAttributes());
        }

        List<Long> compacted = request.getCompactedObjectIds();

        buf.putInt(compacted.size());

        for (Long id : compacted) {
            buf.putLong(id);
        }
    }

    private static CommitStreamSetObjectRequest readCommitRequest(ByteBuffer buf) {
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        request.setObjectId(buf.getLong());
        request.setOrderId(buf.getLong());
        request.setObjectSize(buf.getLong());
        request.setAttributes(buf.getInt());
        int rangeCount = buf.getInt();
        List<ObjectStreamRange> ranges = new ArrayList<>(rangeCount);

        for (int i = 0; i < rangeCount; i++) {
            ObjectStreamRange range = new ObjectStreamRange();

            range.setStreamId(buf.getLong());
            range.setEpoch(buf.getLong());
            range.setStartOffset(buf.getLong());
            range.setEndOffset(buf.getLong());
            range.setSize(buf.getInt());
            ranges.add(range);
        }

        request.setStreamRanges(ranges);
        int objectCount = buf.getInt();
        List<StreamObject> objects = new ArrayList<>(objectCount);

        for (int i = 0; i < objectCount; i++) {
            StreamObject object = new StreamObject();

            object.setObjectId(buf.getLong());
            object.setObjectSize(buf.getLong());
            object.setStreamId(buf.getLong());
            object.setStartOffset(buf.getLong());
            object.setEndOffset(buf.getLong());
            object.setAttributes(buf.getInt());
            objects.add(object);
        }

        request.setStreamObjects(objects);
        int compactedCount = buf.getInt();
        List<Long> compacted = new ArrayList<>(compactedCount);

        for (int i = 0; i < compactedCount; i++) {
            compacted.add(buf.getLong());
        }

        request.setCompactedObjectIds(compacted);

        return request;
    }

    private static void writeCompactRequest(ByteBuffer buf, CompactStreamObjectRequest request) {
        buf.putLong(request.getObjectId());
        buf.putLong(request.getObjectSize());
        buf.putLong(request.getStreamId());
        buf.putLong(request.getStreamEpoch());
        buf.putLong(request.getStartOffset());
        buf.putLong(request.getEndOffset());
        buf.putInt(request.getAttributes());
        List<Long> sourceIds = request.getSourceObjectIds();

        buf.putInt(sourceIds.size());

        for (Long id : sourceIds) {
            buf.putLong(id);
        }

        List<CompactOperations> operations = request.getOperations();

        buf.putInt(operations.size());

        for (CompactOperations op : operations) {
            buf.put(op.value());
        }
    }

    private static CompactStreamObjectRequest readCompactRequest(ByteBuffer buf) {
        long objectId = buf.getLong();
        long objectSize = buf.getLong();
        long streamId = buf.getLong();
        long streamEpoch = buf.getLong();
        long startOffset = buf.getLong();
        long endOffset = buf.getLong();
        int attributes = buf.getInt();
        int sourceCount = buf.getInt();
        List<Long> sourceIds = new ArrayList<>(sourceCount);

        for (int i = 0; i < sourceCount; i++) {
            sourceIds.add(buf.getLong());
        }

        int opCount = buf.getInt();
        List<CompactOperations> operations = new ArrayList<>(opCount);

        for (int i = 0; i < opCount; i++) {
            operations.add(CompactOperations.fromValue(buf.get()));
        }

        return new CompactStreamObjectRequest(
            objectId, objectSize, streamId, streamEpoch, startOffset, endOffset, sourceIds, operations, attributes);
    }
}
