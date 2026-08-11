package io.streamstack.metadata.command;

import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public sealed interface MetadataCommand
    permits MetadataCommand.RegisterNode,
    MetadataCommand.CreateStream,
    MetadataCommand.OpenStream,
    MetadataCommand.TrimStream,
    MetadataCommand.CloseStream,
    MetadataCommand.DeleteStream,
    MetadataCommand.PrepareObject,
    MetadataCommand.CommitStreamSetObject,
    MetadataCommand.CompactStreamObject,
    MetadataCommand.ExpirePreparedObjects,
    MetadataCommand.CleanDestroyedObjects,
    MetadataCommand.PutKV,
    MetadataCommand.PutKVIfAbsent,
    MetadataCommand.DeleteKV {

    byte CREATE_STREAM = 1;
    byte OPEN_STREAM = 2;
    byte TRIM_STREAM = 3;
    byte CLOSE_STREAM = 4;
    byte DELETE_STREAM = 5;
    byte PREPARE_OBJECT = 6;
    byte COMMIT_STREAM_SET_OBJECT = 7;
    byte COMPACT_STREAM_OBJECT = 8;
    byte EXPIRE_PREPARED_OBJECTS = 9;
    byte REGISTER_NODE = 10;
    byte CLEAN_DESTROYED_OBJECTS = 11;
    byte PUT_KV = 12;
    byte PUT_KV_IF_ABSENT = 13;
    byte DELETE_KV = 14;

    byte type();

    record RegisterNode(int nodeId, long nodeEpoch, String httpAddress) implements MetadataCommand {
        public RegisterNode {
            httpAddress = httpAddress == null ? "" : httpAddress;
        }

        public RegisterNode(int nodeId, long nodeEpoch) {
            this(nodeId, nodeEpoch, "");
        }

        @Override
        public byte type() {
            return REGISTER_NODE;
        }
    }

    record CreateStream(int nodeId, long nodeEpoch) implements MetadataCommand {
        @Override
        public byte type() {
            return CREATE_STREAM;
        }
    }

    record OpenStream(int nodeId, long nodeEpoch, long streamId, long epoch) implements MetadataCommand {
        @Override
        public byte type() {
            return OPEN_STREAM;
        }
    }

    record TrimStream(int nodeId, long nodeEpoch, long streamId, long epoch, long newStartOffset)
        implements MetadataCommand {
        @Override
        public byte type() {
            return TRIM_STREAM;
        }
    }

    record CloseStream(int nodeId, long nodeEpoch, long streamId, long epoch) implements MetadataCommand {
        @Override
        public byte type() {
            return CLOSE_STREAM;
        }
    }

    record DeleteStream(int nodeId, long nodeEpoch, long streamId, long epoch) implements MetadataCommand {
        @Override
        public byte type() {
            return DELETE_STREAM;
        }
    }

    record PrepareObject(int nodeId, long nodeEpoch, int count, long ttlMs, long nowMs) implements MetadataCommand {
        @Override
        public byte type() {
            return PREPARE_OBJECT;
        }
    }

    record CommitStreamSetObject(int nodeId, long nodeEpoch, CommitStreamSetObjectRequest request, long nowMs)
        implements MetadataCommand {
        @Override
        public byte type() {
            return COMMIT_STREAM_SET_OBJECT;
        }
    }

    record CompactStreamObject(int nodeId, long nodeEpoch, CompactStreamObjectRequest request, long nowMs)
        implements MetadataCommand {
        @Override
        public byte type() {
            return COMPACT_STREAM_OBJECT;
        }
    }

    record ExpirePreparedObjects(long nowMs) implements MetadataCommand {
        @Override
        public byte type() {
            return EXPIRE_PREPARED_OBJECTS;
        }
    }

    record CleanDestroyedObjects(List<Long> objectIds) implements MetadataCommand {
        @Override
        public byte type() {
            return CLEAN_DESTROYED_OBJECTS;
        }
    }

    record PutKV(String key, byte[] value) implements MetadataCommand {
        public PutKV {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            value = Arrays.copyOf(value, value.length);
        }

        @Override
        public byte type() {
            return PUT_KV;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PutKV that)) {
                return false;
            }
            return Objects.equals(key, that.key) && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(key) + Arrays.hashCode(value);
        }
    }

    record PutKVIfAbsent(String key, byte[] value) implements MetadataCommand {
        public PutKVIfAbsent {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            value = Arrays.copyOf(value, value.length);
        }

        @Override
        public byte type() {
            return PUT_KV_IF_ABSENT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PutKVIfAbsent that)) {
                return false;
            }
            return Objects.equals(key, that.key) && Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(key) + Arrays.hashCode(value);
        }
    }

    record DeleteKV(String key) implements MetadataCommand {
        public DeleteKV {
            Objects.requireNonNull(key, "key");
        }

        @Override
        public byte type() {
            return DELETE_KV;
        }
    }
}
