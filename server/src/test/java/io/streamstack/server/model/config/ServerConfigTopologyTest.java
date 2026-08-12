package io.streamstack.server.model.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServerConfigTopologyTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsTopoAndAppliesStreamConfig() throws Exception {
        Path topo = tempDir.resolve("topo.yaml");
        Files.writeString(topo, """
            global:
              clusterName: poc
              storage: 0@s3://streams-data?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true
              wal: 0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true
              config:
                walCacheSize: 1048576
                blockCacheSize: 524288
                splitSize: 16777216
                objectCompaction:
                  intervalMinutes: 30
                  maxSizeBytes: 1073741824
                streamSetCompaction:
                  interval: 5
                  cacheSize: 104857600
                routing: LOCAL_ALWAYS
              envs:
                - name: AWS_ACCESS_KEY_ID
                  value: minioadmin
                - name: AWS_SECRET_ACCESS_KEY
                  value: minioadmin
            nodes:
              - host: 127.0.0.1
                nodeId: 1
                httpPort: 4437
                raftPort: 8091
                dataDir: /tmp/streamstack/n1
              - host: 127.0.0.1
                nodeId: 2
                httpPort: 4438
                raftPort: 8092
                dataDir: /tmp/streamstack/n2
            """);
        ServerConfig config = ServerConfig.fromArgs(new String[]{
            "--topo", topo.toString(),
            "--node-id", "1",
            "--http-port", "9999"
        });
        assertEquals(1, config.nodeId());
        assertEquals("poc", config.clusterId());
        assertEquals(9999, config.httpPort());
        assertEquals(8091, config.raftPort());
        assertTrue(config.storageUri().startsWith("0@s3://streams-data"));
        assertEquals(
            "0@s3://streams-wal?region=us-east-1&endpoint=http://127.0.0.1:9000&pathStyle=true",
            config.resolveWalUri());
        assertEquals(2, config.raftPeers().size());
        assertEquals("minioadmin", config.envs().get("AWS_ACCESS_KEY_ID"));
        assertEquals(RoutingMode.LOCAL_ALWAYS, config.routingMode());
        assertEquals(30, config.streamConfig().objectCompaction().intervalMinutes().orElseThrow());
        assertEquals(5, config.streamConfig().streamSetCompaction().interval().orElseThrow());
    }

    @Test
    void objectDirSugarAndWalDefaultMemoryForFileStorage() {
        Path objects = tempDir.resolve("objects");
        ServerConfig config = ServerConfig.builder()
            .objectDir(objects.toFile())
            .build();
        assertTrue(config.storageUri().startsWith("-2@file://"));
        assertEquals("memory", config.resolveWalUri());
        assertEquals(objects.toFile().getAbsolutePath(), config.objectDir().getAbsolutePath());
    }

    @Test
    void walDefaultsToStorageWhenS3() {
        String storage = "0@s3://bucket?region=us-east-1";
        ServerConfig config = ServerConfig.builder().storageUri(storage).build();
        assertEquals(storage, config.resolveWalUri());
    }
}
