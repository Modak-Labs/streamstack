package io.streamstack.s2.client;

import com.fasterxml.jackson.databind.JsonNode;

import io.streamstack.s2.client.helper.HttpTransport;
import io.streamstack.s2.client.helper.RetryPolicy;
import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.response.BasinResponse;
import io.streamstack.s2.model.response.ListBasinsResponse;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class S2 implements AutoCloseable {

    private final HttpTransport transport;

    S2(HttpTransport transport) {
        this.transport = transport;
    }

    public static Builder builder(String endpoint) {
        return new Builder(endpoint);
    }

    public Basin basin(String name) {
        return new Basin(transport, name);
    }

    public ListBasinsResponse listBasins() {
        return listBasins(null, null, null);
    }

    public ListBasinsResponse listBasins(String prefix, String startAfter, Integer limit) {
        StringBuilder query = new StringBuilder();

        HttpTransport.appendParam(query, "prefix", prefix);
        HttpTransport.appendParam(query, "start_after", startAfter);
        HttpTransport.appendParam(query, "limit", limit);
        String path = "/v1/basins" + (query.isEmpty() ? "" : "?" + query);

        return transport.execute(transport.request(path).GET(), ListBasinsResponse.class, 200);
    }

    public BasinResponse createBasin(String name) {
        return createBasin(name, null, null);
    }

    public BasinResponse createBasin(String name, JsonNode config, String requestToken) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("basin", name);

        if (Objects.nonNull(config)) {
            body.put("config", config);
        }

        var builder = transport.request("/v1/basins").POST(transport.jsonBody(body));

        if (Objects.nonNull(requestToken)) {
            builder.header("s2-request-token", requestToken);
        }

        return transport.execute(builder, BasinResponse.class, 201, 200);
    }

    public BasinResponse ensureBasin(String name, JsonNode config) {
        Map<String, Object> body = new LinkedHashMap<>();

        if (Objects.nonNull(config)) {
            body.put("config", config);
        }

        return transport.execute(
            transport.request("/v1/basins/" + HttpTransport.encodePath(name)).PUT(transport.jsonBody(body)),
            BasinResponse.class,
            201, 200);
    }

    public JsonNode getBasinConfig(String name) {
        return transport.execute(
            transport.request("/v1/basins/" + HttpTransport.encodePath(name)).GET(),
            JsonNode.class,
            200);
    }

    public JsonNode reconfigureBasin(String name, JsonNode patch) {
        return transport.execute(
            transport.request("/v1/basins/" + HttpTransport.encodePath(name))
                .method("PATCH", transport.jsonBody(patch)),
            JsonNode.class,
            200);
    }

    public void deleteBasin(String name) {
        transport.execute(
            transport.request("/v1/basins/" + HttpTransport.encodePath(name)).DELETE(),
            204);
    }

    @Override
    public void close() {
        transport.close();
    }

    public static final class Builder {
        private final String endpoint;
        private Format format = Format.RAW;
        private HttpClient httpClient;
        private RetryPolicy retryPolicy = RetryPolicy.defaults();
        private Builder(String endpoint) {
            this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        }

        public Builder format(Format format) {
            this.format = format;
            return this;
        }

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public S2 build() {
            boolean owns = Objects.isNull(httpClient);
            HttpClient client = Objects.nonNull(httpClient)
                ? httpClient
                : HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            return new S2(new HttpTransport(client, owns, endpoint, format, retryPolicy));
        }
    }
}
