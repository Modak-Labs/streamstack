package io.streamstack.s2.client;

import com.fasterxml.jackson.databind.JsonNode;

import io.streamstack.s2.client.helper.HttpTransport;
import io.streamstack.s2.model.response.ListStreamsResponse;
import io.streamstack.s2.model.response.StreamResponse;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class Basin {

    private final HttpTransport transport;
    private final String name;

    Basin(HttpTransport transport, String name) {
        this.transport = transport;
        this.name = Objects.requireNonNull(name, "name");
    }

    public String name() {
        return name;
    }

    public Stream stream(String stream) {
        return new Stream(transport, name, stream);
    }

    public ListStreamsResponse listStreams() {
        return listStreams(null, null, null);
    }

    public ListStreamsResponse listStreams(String prefix, String startAfter, Integer limit) {
        StringBuilder query = new StringBuilder();

        HttpTransport.appendParam(query, "prefix", prefix);
        HttpTransport.appendParam(query, "start_after", startAfter);
        HttpTransport.appendParam(query, "limit", limit);
        String path = "/v1/streams" + (query.isEmpty() ? "" : "?" + query);

        return transport.execute(transport.withBasin(transport.request(path), name).GET(),
            ListStreamsResponse.class, 200);
    }

    public StreamResponse createStream(String stream) {
        return createStream(stream, null, null);
    }

    public StreamResponse createStream(String stream, JsonNode config, String requestToken) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("stream", stream);

        if (Objects.nonNull(config)) {
            body.put("config", config);
        }

        var builder = transport.withBasin(transport.request("/v1/streams"), name)
            .POST(transport.jsonBody(body));
        if (Objects.nonNull(requestToken)) {
            builder.header("s2-request-token", requestToken);
        }

        return transport.execute(builder, StreamResponse.class, 201, 200);
    }

    public StreamResponse ensureStream(String stream, JsonNode config) {
        Map<String, Object> body = new LinkedHashMap<>();

        if (Objects.nonNull(config)) {
            body.put("config", config);
        }

        return transport.execute(
            transport.withBasin(transport.request("/v1/streams/" + HttpTransport.encodePath(stream)), name)
                .PUT(transport.jsonBody(body)),
            StreamResponse.class,
            201, 200);
    }

    public JsonNode getStreamConfig(String stream) {
        return transport.execute(
            transport.withBasin(transport.request("/v1/streams/" + HttpTransport.encodePath(stream)), name).GET(),
            JsonNode.class,
            200);
    }

    public JsonNode reconfigureStream(String stream, JsonNode patch) {
        return transport.execute(
            transport.withBasin(transport.request("/v1/streams/" + HttpTransport.encodePath(stream)), name)
                .method("PATCH", transport.jsonBody(patch)),
            JsonNode.class,
            200);
    }

    public void deleteStream(String stream) {
        transport.execute(
            transport.withBasin(transport.request("/v1/streams/" + HttpTransport.encodePath(stream)), name).DELETE(),
            204);
    }
}
