package io.streamstack.s2.server;

import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.model.config.ServerConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S2ServerIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BASIN = "test-basin-one";

    @TempDir
    Path tempDir;

    @Test
    void basinsStreamsAndRecordsEndToEnd() throws Exception {
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
        try (S2Server server = new S2Server(config)) {
            server.start();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            String base = server.baseUrl();
            basins(client, base);
            streams(client, base);
            records(client, base);
            concurrencyControl(client, base);
            selectorsAndLimits(client, base);
            sse(client, base);
            trimAndAutoCreate(client, base);
            deletes(client, base);
        }
    }

    private void basins(HttpClient client, String base) throws Exception {
        HttpResponse<String> created = send(client, post(base + "/v1/basins",
            "{\"basin\":\"" + BASIN + "\"}"));
        assertEquals(201, created.statusCode(), created.body());
        JsonNode info = MAPPER.readTree(created.body());
        assertEquals(BASIN, info.get("name").asText());
        assertEquals("active", info.get("state").asText());
        HttpResponse<String> duplicate = send(client, post(base + "/v1/basins",
            "{\"basin\":\"" + BASIN + "\"}"));
        assertEquals(409, duplicate.statusCode());
        assertEquals("resource_already_exists", MAPPER.readTree(duplicate.body()).get("code").asText());
        HttpResponse<String> invalidName = send(client, post(base + "/v1/basins", "{\"basin\":\"UPPER\"}"));
        assertEquals(422, invalidName.statusCode());
        HttpResponse<String> list = send(client, get(base + "/v1/basins"));
        assertEquals(200, list.statusCode());
        JsonNode basins = MAPPER.readTree(list.body());
        assertEquals(1, basins.get("basins").size());
        assertFalse(basins.get("has_more").asBoolean());
        HttpResponse<String> reconfigured = send(client, patch(base + "/v1/basins/" + BASIN,
            "{\"default_stream_config\":{\"storage_class\":\"standard\"}}"));
        assertEquals(200, reconfigured.statusCode(), reconfigured.body());
        assertEquals("standard",
            MAPPER.readTree(reconfigured.body()).get("default_stream_config").get("storage_class").asText());
    }

    private void streams(HttpClient client, String base) throws Exception {
        HttpResponse<String> created = send(client, withBasin(post(base + "/v1/streams",
            "{\"stream\":\"logs/app-1\",\"config\":{\"timestamping\":{\"mode\":\"client-prefer\"}}}")));
        assertEquals(201, created.statusCode(), created.body());
        assertEquals("logs/app-1", MAPPER.readTree(created.body()).get("name").asText());
        HttpResponse<String> list = send(client, withBasin(get(base + "/v1/streams")));
        assertEquals(200, list.statusCode());
        assertEquals(1, MAPPER.readTree(list.body()).get("streams").size());
        HttpResponse<String> resolved = send(client, withBasin(get(base + "/v1/streams/logs/app-1")));
        assertEquals(200, resolved.statusCode(), resolved.body());
        JsonNode cfg = MAPPER.readTree(resolved.body());
        assertEquals("standard", cfg.get("storage_class").asText());
        assertEquals("client-prefer", cfg.get("timestamping").get("mode").asText());
        assertEquals(7 * 24 * 60 * 60, cfg.get("retention_policy").get("age").asLong());
        HttpResponse<String> missing = send(client, withBasin(get(base + "/v1/streams/nope")));
        assertEquals(404, missing.statusCode());
        assertEquals("stream_not_found", MAPPER.readTree(missing.body()).get("code").asText());
    }

    private void records(HttpClient client, String base) throws Exception {
        String records = base + "/v1/streams/logs/app-1/records";
        HttpResponse<String> appended = send(client, withBasin(post(records,
            "{\"records\":[{\"body\":\"one\"},{\"body\":\"two\",\"headers\":[[\"k\",\"v\"]]}]}")));
        assertEquals(200, appended.statusCode(), appended.body());
        JsonNode ack = MAPPER.readTree(appended.body());
        assertEquals(0, ack.get("start").get("seq_num").asLong());
        assertEquals(2, ack.get("end").get("seq_num").asLong());
        assertEquals(2, ack.get("tail").get("seq_num").asLong());
        assertTrue(ack.get("end").get("timestamp").asLong() > 0);
        HttpResponse<String> read = send(client, withBasin(get(records + "?seq_num=0")));
        assertEquals(200, read.statusCode(), read.body());
        JsonNode batch = MAPPER.readTree(read.body());
        assertEquals(2, batch.get("records").size());
        assertEquals("one", batch.get("records").get(0).get("body").asText());
        assertNull(batch.get("records").get(0).get("headers"));
        assertEquals("k", batch.get("records").get(1).get("headers").get(0).get(0).asText());
        assertEquals(2, batch.get("tail").get("seq_num").asLong());
        HttpResponse<String> tail = send(client, withBasin(get(records + "/tail")));
        assertEquals(200, tail.statusCode());
        assertEquals(2, MAPPER.readTree(tail.body()).get("tail").get("seq_num").asLong());
    }

    private void concurrencyControl(HttpClient client, String base) throws Exception {
        String records = base + "/v1/streams/logs/app-1/records";
        HttpResponse<String> mismatch = send(client, withBasin(post(records,
            "{\"records\":[{\"body\":\"x\"}],\"match_seq_num\":0}")));
        assertEquals(412, mismatch.statusCode());
        assertEquals(2, MAPPER.readTree(mismatch.body()).get("seq_num_mismatch").asLong());
        HttpResponse<String> matched = send(client, withBasin(post(records,
            "{\"records\":[{\"body\":\"three\"}],\"match_seq_num\":2}")));
        assertEquals(200, matched.statusCode(), matched.body());
        HttpResponse<String> fenced = send(client, withBasin(post(records,
            "{\"records\":[{\"headers\":[[\"\",\"fence\"]],\"body\":\"tok-1\"}]}")));
        assertEquals(200, fenced.statusCode(), fenced.body());
        HttpResponse<String> badToken = send(client, withBasin(post(records,
            "{\"records\":[{\"body\":\"y\"}],\"fencing_token\":\"bad\"}")));
        assertEquals(412, badToken.statusCode());
        assertEquals("tok-1", MAPPER.readTree(badToken.body()).get("fencing_token_mismatch").asText());
        HttpResponse<String> goodToken = send(client, withBasin(post(records,
            "{\"records\":[{\"body\":\"four\"}],\"fencing_token\":\"tok-1\"}")));
        assertEquals(200, goodToken.statusCode(), goodToken.body());
        HttpResponse<String> readCommand = send(client, withBasin(get(records + "?seq_num=3&count=1")));
        JsonNode command = MAPPER.readTree(readCommand.body()).get("records").get(0);
        assertEquals("", command.get("headers").get(0).get(0).asText());
        assertEquals("fence", command.get("headers").get(0).get(1).asText());
        assertEquals("tok-1", command.get("body").asText());
    }

    private void selectorsAndLimits(HttpClient client, String base) throws Exception {
        String records = base + "/v1/streams/logs/app-1/records";
        HttpResponse<String> tailOffset = send(client, withBasin(get(records + "?tail_offset=1")));
        JsonNode last = MAPPER.readTree(tailOffset.body());
        assertEquals(1, last.get("records").size());
        assertEquals("four", last.get("records").get(0).get("body").asText());
        HttpResponse<String> limited = send(client, withBasin(get(records + "?seq_num=0&count=2")));
        assertEquals(2, MAPPER.readTree(limited.body()).get("records").size());
        HttpResponse<String> tail = send(client, withBasin(get(records + "/tail")));
        long tailTs = MAPPER.readTree(tail.body()).get("tail").get("timestamp").asLong();
        HttpResponse<String> byTimestamp = send(client, withBasin(get(records + "?timestamp=" + tailTs)));
        assertEquals(200, byTimestamp.statusCode(), byTimestamp.body());
        JsonNode tsBatch = MAPPER.readTree(byTimestamp.body());
        assertTrue(tsBatch.get("records").size() >= 1);
        assertEquals(tailTs, tsBatch.get("records").get(0).get("timestamp").asLong());
        HttpResponse<String> beyond = send(client, withBasin(get(records + "?seq_num=999")));
        assertEquals(416, beyond.statusCode());
        assertEquals(5, MAPPER.readTree(beyond.body()).get("tail").get("seq_num").asLong());
        HttpResponse<String> clamped = send(client, withBasin(get(records + "?seq_num=999&clamp=true&wait=0")));
        assertEquals(200, clamped.statusCode());
        assertEquals(0, MAPPER.readTree(clamped.body()).get("records").size());
    }

    private void sse(HttpClient client, String base) throws Exception {
        String records = base + "/v1/streams/logs/app-1/records";
        HttpResponse<java.io.InputStream> stream = client.send(
            withBasin(get(records + "?seq_num=0&count=2"))
                .header("Accept", "text/event-stream")
                .build(),
            HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, stream.statusCode());
        assertEquals("text/event-stream",
            stream.headers().firstValue("Content-Type").orElse(""));
        String event = null;
        String id = null;
        String data = null;
        boolean done = false;
        try (var reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(stream.body(), StandardCharsets.UTF_8))) {
            String line;
            while (Objects.nonNull((line = reader.readLine()))) {
                if (line.startsWith("event: ")) {
                    event = line.substring(7);
                } else if (line.startsWith("id: ")) {
                    id = line.substring(4);
                } else if (line.startsWith("data: ")) {
                    data = line.substring(6);
                    if ("[DONE]".equals(data)) {
                        done = true;
                        break;
                    }
                    if ("batch".equals(event)) {
                        JsonNode batch = MAPPER.readTree(data);
                        assertEquals(2, batch.get("records").size());
                        assertEquals("one", batch.get("records").get(0).get("body").asText());
                        assertTrue(Objects.nonNull(id) && id.startsWith("1,2,"));
                    }
                }
            }
        }
        assertTrue(done, "expected [DONE] after count limit");
    }

    private void trimAndAutoCreate(HttpClient client, String base) throws Exception {
        String records = base + "/v1/streams/logs/app-1/records";
        byte[] trimPoint = ByteBuffer.allocate(8).putLong(3).array();
        String body = Base64.getEncoder().encodeToString(trimPoint);
        HttpResponse<String> trimmed = send(client, withFormat(withBasin(post(records,
            "{\"records\":[{\"headers\":[[\"\",\"" + Base64.getEncoder().encodeToString(
                "trim".getBytes(StandardCharsets.UTF_8)) + "\"]],\"body\":\"" + body + "\"}]}")), "base64"));
        assertEquals(200, trimmed.statusCode(), trimmed.body());
        HttpResponse<String> read = send(client, withBasin(get(records + "?seq_num=0")));
        JsonNode batch = MAPPER.readTree(read.body());
        assertEquals(3, batch.get("records").get(0).get("seq_num").asLong());
        HttpResponse<String> enableAutoCreate = send(client, patch(base + "/v1/basins/" + BASIN,
            "{\"create_stream_on_append\":true}"));
        assertEquals(200, enableAutoCreate.statusCode(), enableAutoCreate.body());
        HttpResponse<String> autoAppend = send(client, withBasin(post(
            base + "/v1/streams/auto/records", "{\"records\":[{\"body\":\"hi\"}]}")));
        assertEquals(200, autoAppend.statusCode(), autoAppend.body());
        HttpResponse<String> autoConfig = send(client, withBasin(get(base + "/v1/streams/auto")));
        assertEquals(200, autoConfig.statusCode());
    }

    private void deletes(HttpClient client, String base) throws Exception {
        HttpResponse<String> deleteStream = send(client, withBasin(delete(base + "/v1/streams/auto")));
        assertEquals(204, deleteStream.statusCode());
        HttpResponse<String> gone = send(client, withBasin(get(base + "/v1/streams/auto")));
        assertEquals(404, gone.statusCode());
        HttpResponse<String> deleteBasin = send(client, delete(base + "/v1/basins/" + BASIN));
        assertEquals(204, deleteBasin.statusCode());
        HttpResponse<String> basinGone = send(client, get(base + "/v1/basins/" + BASIN));
        assertEquals(404, basinGone.statusCode());
    }

    private static HttpRequest.Builder get(String uri) {
        return HttpRequest.newBuilder(URI.create(uri)).GET();
    }

    private static HttpRequest.Builder post(String uri, String body) {
        return HttpRequest.newBuilder(URI.create(uri))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));
    }

    private static HttpRequest.Builder patch(String uri, String body) {
        return HttpRequest.newBuilder(URI.create(uri))
            .header("Content-Type", "application/json")
            .method("PATCH", HttpRequest.BodyPublishers.ofString(body));
    }

    private static HttpRequest.Builder delete(String uri) {
        return HttpRequest.newBuilder(URI.create(uri)).DELETE();
    }

    private static HttpRequest.Builder withBasin(HttpRequest.Builder builder) {
        return builder.header("s2-basin", BASIN);
    }

    private static HttpRequest.Builder withFormat(HttpRequest.Builder builder, String format) {
        return builder.header("s2-format", format);
    }

    private static HttpResponse<String> send(HttpClient client, HttpRequest.Builder builder) throws Exception {
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
