package io.streamstack.metadata.stream;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVControlManagerTest {

    @Test
    void putGetDeleteAndPutIfAbsent() {
        KVControlManager kv = new KVControlManager();
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(value, kv.put("k", value));
        assertArrayEquals(value, kv.get("k"));
        byte[] other = "world".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(value, kv.putIfAbsent("k", other));
        assertArrayEquals(other, kv.putIfAbsent("k2", other));
        assertArrayEquals(other, kv.get("k2"));
        assertArrayEquals(value, kv.delete("k"));
        assertNull(kv.get("k"));
        assertNull(kv.delete("missing"));
    }

    @Test
    void snapshotIsDefensiveCopy() {
        KVControlManager kv = new KVControlManager();
        byte[] value = new byte[] {1, 2, 3};
        kv.put("a", value);
        value[0] = 9;
        assertArrayEquals(new byte[] {1, 2, 3}, kv.get("a"));
        var snapshot = kv.snapshot();
        snapshot.get("a")[0] = 7;
        assertArrayEquals(new byte[] {1, 2, 3}, kv.get("a"));
        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, kv.snapshot().get("a")));
    }
}
