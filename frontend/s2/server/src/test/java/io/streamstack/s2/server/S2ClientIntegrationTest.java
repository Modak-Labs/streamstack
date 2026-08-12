package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.client.Basin;
import io.streamstack.s2.client.S2;
import io.streamstack.s2.client.Stream;
import io.streamstack.s2.model.RecordHeader;
import io.streamstack.s2.model.exception.SeqNumMismatchException;
import io.streamstack.s2.model.exception.StreamNotFoundException;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.request.ReadRequest;
import io.streamstack.s2.model.response.AppendResponse;
import io.streamstack.s2.model.response.ReadResponse;
import io.streamstack.s2.model.response.SequencedRecord;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S2ClientIntegrationTest {

    @TempDir
    Path tempDir;

    @Test
    void clientRoundTrip() throws Exception {
        int httpPort = freePort();
        int raftPort = freePort();
        ServerConfig config = ServerConfig.builder()
            .nodeId(1)
            .nodeEpoch(1)
            .httpHost("127.0.0.1")
            .httpPort(httpPort)
            .raftHost("127.0.0.1")
            .raftPort(raftPort)
            .raftPeers(List.of("127.0.0.1:" + raftPort))
            .dataDir(tempDir.resolve("data").toFile())
            .objectDir(tempDir.resolve("objects").toFile())
            .routingMode(RoutingMode.LOCAL_ALWAYS)
            .build();
        try (S2Server server = new S2Server(config);
             S2 s2 = S2.builder(server.baseUrl()).build()) {
            server.start();
            Basin basin = s2.basin("client-basin");

            s2.createBasin("client-basin");
            basin.createStream("events");
            Stream stream = basin.stream("events");
            AppendResponse ack = stream.append(new AppendRequest(
                List.of(
                    new AppendRecord(null, List.of(), "hello".getBytes(StandardCharsets.UTF_8)),
                    new AppendRecord(null,
                        List.of(new RecordHeader(
                            "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8))),
                        "world".getBytes(StandardCharsets.UTF_8))),
                null, null));
            assertEquals(0, ack.start().seqNum());
            assertEquals(2, ack.end().seqNum());
            ReadResponse batch = stream.read(new ReadRequest(0L, null, null, false, null, null, null, null));

            assertEquals(2, batch.records().size());
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), batch.records().get(0).body());
            assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), batch.records().get(1).headers().get(0).value());
            assertEquals(2, stream.checkTail().tail().seqNum());

            try (var session = stream.readSession(new ReadRequest(0L, null, null, false, 2L, null, null, null))) {
                List<SequencedRecord> records = new ArrayList<>();

                for (SequencedRecord record : session) {
                    records.add(record);
                }

                assertEquals(2, records.size());
                assertEquals("hello", new String(records.get(0).body(), StandardCharsets.UTF_8));
                assertTrue(Objects.nonNull(session.lastEventId()) && session.lastEventId().startsWith("1,"));
            }

            try (var producer = stream.producer()) {
                producer.submit(new AppendRecord(null, List.of(), "three".getBytes(StandardCharsets.UTF_8)));
                AppendResponse flushed = producer.flush();

                assertEquals(3, flushed.end().seqNum());
            }

            SeqNumMismatchException mismatch = assertThrows(SeqNumMismatchException.class, () ->
                stream.append(new AppendRequest(
                    List.of(new AppendRecord(null, List.of(), "x".getBytes(StandardCharsets.UTF_8))), 0L, null)));
            assertEquals(3, mismatch.actualSeqNum());
            StreamNotFoundException missing = assertThrows(StreamNotFoundException.class,
                () -> basin.stream("nope").checkTail());
            assertEquals("nope", missing.stream());
            basin.deleteStream("events");
            s2.deleteBasin("client-basin");
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
