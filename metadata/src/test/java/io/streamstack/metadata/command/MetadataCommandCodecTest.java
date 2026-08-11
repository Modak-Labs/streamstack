package io.streamstack.metadata.command;

import io.streamstack.s3.compact.CompactOperations;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectStreamRange;
import io.streamstack.s3.objects.StreamObject;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataCommandCodecTest {
    @Test
    void roundTripCommands() {
        assertRoundTrip(new MetadataCommand.CreateStream(7, 100));
        assertRoundTrip(new MetadataCommand.OpenStream(7, 100, 3, 9));
        assertRoundTrip(new MetadataCommand.TrimStream(7, 100, 3, 9, 4));
        assertRoundTrip(new MetadataCommand.CloseStream(7, 100, 3, 9));
        assertRoundTrip(new MetadataCommand.DeleteStream(7, 100, 3, 9));
        assertRoundTrip(new MetadataCommand.PrepareObject(7, 100, 2, 1000, 50));
        assertRoundTrip(new MetadataCommand.ExpirePreparedObjects(99));
        assertRoundTrip(new MetadataCommand.RegisterNode(7, 100));
        assertRoundTrip(new MetadataCommand.RegisterNode(7, 100, "http://127.0.0.1:8080"));
        assertRoundTrip(new MetadataCommand.CleanDestroyedObjects(List.of(1L, 2L, 3L)));
        assertRoundTrip(new MetadataCommand.PutKV("path/a", new byte[] {1, 2, 3}));
        assertRoundTrip(new MetadataCommand.PutKVIfAbsent("path/b", new byte[] {4, 5}));
        assertRoundTrip(new MetadataCommand.DeleteKV("path/c"));

        CommitStreamSetObjectRequest commit = new CommitStreamSetObjectRequest();
        commit.setObjectId(11);
        commit.setOrderId(11);
        commit.setObjectSize(128);
        commit.setAttributes(1);
        ObjectStreamRange range = new ObjectStreamRange(3, 1, 0, 10, 128);
        commit.setStreamRanges(List.of(range));
        StreamObject streamObject = new StreamObject();
        streamObject.setObjectId(12);
        streamObject.setObjectSize(64);
        streamObject.setStreamId(3);
        streamObject.setStartOffset(10);
        streamObject.setEndOffset(20);
        streamObject.setAttributes(2);
        commit.setStreamObjects(List.of(streamObject));
        commit.setCompactedObjectIds(List.of(1L, 2L));
        assertStableEncoding(new MetadataCommand.CommitStreamSetObject(7, 100, commit, 123));

        CompactStreamObjectRequest compact = new CompactStreamObjectRequest(
            20, 64, 3, 9, 0, 20, List.of(12L), List.of(CompactOperations.DELETE), 3);
        assertStableEncoding(new MetadataCommand.CompactStreamObject(7, 100, compact, 456));
    }

    private static void assertRoundTrip(MetadataCommand command) {
        MetadataCommand decoded = MetadataCommandCodec.decode(MetadataCommandCodec.encode(command));
        assertEquals(command, decoded);
    }

    private static void assertStableEncoding(MetadataCommand command) {
        byte[] encoded = MetadataCommandCodec.encode(command);
        byte[] reencoded = MetadataCommandCodec.encode(MetadataCommandCodec.decode(encoded));
        assertArrayEquals(encoded, reencoded);
    }
}
