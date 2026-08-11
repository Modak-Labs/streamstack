package io.streamstack.metadata.codec;

import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.s3.objects.CommitStreamSetObjectResponse;

import java.nio.ByteBuffer;

public final class MetadataResultCodec {
    private static final byte NULL = 0;
    private static final byte LONG = 1;
    private static final byte INT = 2;
    private static final byte STREAM_METADATA = 3;
    private static final byte COMMIT_RESPONSE = 4;
    private static final byte BYTES = 5;

    private MetadataResultCodec() {
    }

    public static byte[] encode(Object result) {
        if (result == null) {
            return new byte[] {NULL};
        }
        if (result instanceof Long value) {
            ByteBuffer buf = ByteBuffer.allocate(9);
            buf.put(LONG);
            buf.putLong(value);
            return buf.array();
        }
        if (result instanceof Integer value) {
            ByteBuffer buf = ByteBuffer.allocate(5);
            buf.put(INT);
            buf.putInt(value);
            return buf.array();
        }
        if (result instanceof StreamMetadata metadata) {
            ByteBuffer buf = ByteBuffer.allocate(1 + 8 * 4 + 1 + 4);
            buf.put(STREAM_METADATA);
            buf.putLong(metadata.streamId());
            buf.putLong(metadata.epoch());
            buf.putLong(metadata.startOffset());
            buf.putLong(metadata.endOffset());
            buf.put(metadata.state().toByte());
            buf.putInt(metadata.nodeId());
            return buf.array();
        }
        if (result instanceof CommitStreamSetObjectResponse) {
            return new byte[] {COMMIT_RESPONSE};
        }
        if (result instanceof byte[] value) {
            ByteBuffer buf = ByteBuffer.allocate(5 + value.length);
            buf.put(BYTES);
            buf.putInt(value.length);
            buf.put(value);
            return buf.array();
        }
        throw new IllegalArgumentException("unsupported metadata result type " + result.getClass().getName());
    }

    public static Object decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte type = buf.get();
        return switch (type) {
            case NULL -> null;
            case LONG -> buf.getLong();
            case INT -> buf.getInt();
            case STREAM_METADATA -> {
                StreamMetadata metadata = new StreamMetadata(
                    buf.getLong(), buf.getLong(), buf.getLong(), buf.getLong(), StreamState.fromByte(buf.get()));
                metadata.nodeId(buf.getInt());
                yield metadata;
            }
            case COMMIT_RESPONSE -> new CommitStreamSetObjectResponse();
            case BYTES -> {
                int length = buf.getInt();
                byte[] value = new byte[length];
                buf.get(value);
                yield value;
            }
            default -> throw new IllegalArgumentException("unknown metadata result type " + type);
        };
    }
}
