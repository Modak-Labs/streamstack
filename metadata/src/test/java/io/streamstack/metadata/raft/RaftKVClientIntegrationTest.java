package io.streamstack.metadata.raft;

import io.streamstack.api.KeyValue;
import io.streamstack.api.KeyValue.Key;
import io.streamstack.api.KeyValue.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RaftKVClientIntegrationTest {
    @TempDir
    Path tempDir;

    @Test
    void putGetDeleteRoundTrip() throws Exception {
        int port = MetadataTestSupport.freePort();
        File dataDir = tempDir.resolve("meta").toFile();
        List<String> peers = MetadataNode.singlePeer("127.0.0.1", port);
        try (MetadataNode node = new MetadataNode(1, "127.0.0.1", port, dataDir, peers, 1L)) {
            node.awaitLeader(15, TimeUnit.SECONDS);
            node.awaitRegistered(15, TimeUnit.SECONDS);
            RaftKVClient kv = new RaftKVClient(node);

            byte[] value = "v1".getBytes(StandardCharsets.UTF_8);
            Value put = kv.putKV(KeyValue.of("/streams/a", ByteBuffer.wrap(value)))
                .get(15, TimeUnit.SECONDS);
            assertArrayEquals(value, toBytes(put));

            byte[] other = "v2".getBytes(StandardCharsets.UTF_8);
            Value absent = kv.putKVIfAbsent(KeyValue.of("/streams/a", ByteBuffer.wrap(other)))
                .get(15, TimeUnit.SECONDS);
            assertArrayEquals(value, toBytes(absent));

            Value got = kv.getKV(Key.of("/streams/a")).get(15, TimeUnit.SECONDS);
            assertArrayEquals(value, toBytes(got));

            Value deleted = kv.delKV(Key.of("/streams/a")).get(15, TimeUnit.SECONDS);
            assertArrayEquals(value, toBytes(deleted));
            assertNull(kv.getKV(Key.of("/streams/a")).get(15, TimeUnit.SECONDS));
        }
    }

    private static byte[] toBytes(Value value) {
        ByteBuffer buffer = value.get().duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
