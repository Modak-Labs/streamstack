package io.streamstack.ss.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.streamstack.ss.model.BatchCodec;
import io.streamstack.ss.model.Protocol;
import io.streamstack.ss.model.request.AppendRequest;
import io.streamstack.ss.model.response.AppendResponse;
import io.streamstack.ss.model.response.HeadResponse;
import io.streamstack.ss.model.response.ListResponse;
import io.streamstack.ss.model.response.ReadResponse;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class StreamStack implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpClient http;
    private final String baseUrl;
    private final Duration requestTimeout;

    private StreamStack(Builder builder) {
        this.baseUrl = trimSlash(Objects.requireNonNull(builder.baseUrl, "baseUrl"));
        this.requestTimeout = builder.requestTimeout;
        this.http = Objects.nonNull(builder.httpClient) ? builder.httpClient : HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean create(String stream, String contentType) {
        HttpRequest request = request(stream)
            .header(Protocol.H_CONTENT_TYPE, contentType)
            .PUT(HttpRequest.BodyPublishers.noBody())
            .build();
        HttpResponse<byte[]> response = send(request);

        check(response, 200, 201);

        return response.statusCode() == 201;
    }

    public Optional<HeadResponse> head(String stream) {
        HttpResponse<byte[]> response = send(request(stream)
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build());

        if (response.statusCode() == 404) {
            return Optional.empty();
        }

        check(response, 200);

        return Optional.of(new HeadResponse(
            stream,
            header(response, Protocol.H_CONTENT_TYPE),
            longHeader(response, Protocol.H_START_SEQ, 0),
            longHeader(response, Protocol.H_NEXT_SEQ, 0),
            boolHeader(response, Protocol.H_CLOSED),
            nullableLongHeader(response, Protocol.H_TTL),
            instantHeader(response, Protocol.H_EXPIRES_AT)));
    }

    public AppendResponse append(String stream, AppendRequest request) {
        return join(appendAsync(stream, request));
    }

    public AppendResponse appendRaw(String stream, byte[] body, String contentType) {
        HttpRequest request = request(stream)
            .header(Protocol.H_CONTENT_TYPE, contentType)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();

        return appendResponse(checked(send(request), 200));
    }

    public CompletableFuture<AppendResponse> appendAsync(String stream, AppendRequest request) {
        HttpRequest.Builder builder = request(stream)
            .header(Protocol.H_CONTENT_TYPE, Protocol.CT_BATCH_BINARY);

        if (Objects.nonNull(request.matchSeq())) {
            builder.header(Protocol.H_MATCH_SEQ, Long.toString(request.matchSeq()));
        }

        if (Objects.nonNull(request.producerId())) {
            builder.header(Protocol.H_PRODUCER_ID, request.producerId());
            builder.header(Protocol.H_PRODUCER_EPOCH, Long.toString(request.producerEpoch()));
            builder.header(Protocol.H_PRODUCER_SEQ, Long.toString(request.producerSeq()));
        }

        HttpRequest httpRequest = builder
            .POST(HttpRequest.BodyPublishers.ofByteArray(BatchCodec.encodeAppend(request.records())))
            .build();

        return http.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
            .thenApply(response -> appendResponse(checked(response, 200)));
    }

    public ReadResponse read(String stream, long seq, int count, int bytes) {
        StringBuilder query = new StringBuilder("?").append(Protocol.Q_FORMAT).append('=')
            .append(Protocol.FORMAT_BINARY).append('&').append(Protocol.Q_SEQ).append('=').append(seq);

        if (count > 0) {
            query.append('&').append(Protocol.Q_COUNT).append('=').append(count);
        }

        if (bytes > 0) {
            query.append('&').append(Protocol.Q_BYTES).append('=').append(bytes);
        }

        HttpResponse<byte[]> response = checked(send(request(stream, query.toString()).GET().build()), 200);

        return new ReadResponse(
            BatchCodec.decodeRead(response.body()),
            longHeader(response, Protocol.H_NEXT_SEQ, seq),
            boolHeader(response, Protocol.H_UP_TO_DATE),
            boolHeader(response, Protocol.H_CLOSED));
    }

    public byte[] readRaw(String stream, long seq, int bytes) {
        String query = "?" + Protocol.Q_FORMAT + "=" + Protocol.FORMAT_RAW + "&" + Protocol.Q_SEQ + "=" + seq
            + (bytes > 0 ? "&" + Protocol.Q_BYTES + "=" + bytes : "");

        return checked(send(request(stream, query).GET().build()), 200).body();
    }

    public ListResponse list(String prefix, String startAfter, int limit) {
        StringBuilder query = new StringBuilder("?").append(Protocol.Q_PREFIX).append('=').append(encode(prefix));

        if (Objects.nonNull(startAfter) && !startAfter.isEmpty()) {
            query.append('&').append(Protocol.Q_START_AFTER).append('=').append(encode(startAfter));
        }

        if (limit > 0) {
            query.append('&').append(Protocol.Q_LIMIT).append('=').append(limit);
        }

        HttpResponse<byte[]> response = checked(send(request("/", query.toString()).GET().build()), 200);
        JsonNode root = json(response.body());
        List<HeadResponse> streams = new ArrayList<>();

        for (JsonNode node : root.path("streams")) {
            streams.add(new HeadResponse(
                node.path("name").asText(),
                node.path("content_type").asText(null),
                node.path("start_seq").asLong(),
                node.path("next_seq").asLong(),
                node.path("closed").asBoolean(),
                node.has("ttl") ? node.path("ttl").asLong() : null,
                node.has("expires_at") ? Instant.parse(node.path("expires_at").asText()) : null));
        }

        return new ListResponse(streams, root.path("has_more").asBoolean());
    }

    public long trim(String stream, long seq) {
        HttpRequest request = request(stream)
            .header(Protocol.H_TRIM_SEQ, Long.toString(seq))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

        return longHeader(checked(send(request), 200), Protocol.H_START_SEQ, 0);
    }

    public long closeStream(String stream) {
        HttpRequest request = request(stream)
            .header(Protocol.H_CLOSED, Protocol.BOOL_TRUE)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();

        return longHeader(checked(send(request), 200), Protocol.H_NEXT_SEQ, 0);
    }

    public boolean delete(String stream) {
        HttpResponse<byte[]> response = send(request(stream).DELETE().build());

        if (response.statusCode() == 404) {
            return false;
        }

        check(response, 204);

        return true;
    }

    public Producer producer(String stream, String producerId) {
        return new Producer(this, stream, producerId);
    }

    @Override
    public void close() {
    }

    private static AppendResponse appendResponse(HttpResponse<byte[]> response) {
        long nextSeq = longHeader(response, Protocol.H_NEXT_SEQ, 0);

        return new AppendResponse(
            longHeader(response, Protocol.H_START_SEQ, nextSeq),
            nextSeq,
            nullableLongHeader(response, Protocol.H_TIMESTAMP),
            nullableLongHeader(response, Protocol.H_PRODUCER_EPOCH),
            nullableLongHeader(response, Protocol.H_PRODUCER_SEQ));
    }

    private HttpRequest.Builder request(String stream) {
        return request(stream, "");
    }

    private HttpRequest.Builder request(String stream, String query) {
        String path = stream.startsWith("/") ? stream : "/" + stream;

        return HttpRequest.newBuilder(URI.create(baseUrl + path + query)).timeout(requestTimeout);
    }

    private HttpResponse<byte[]> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new StreamStackException(0, "transport", e.getMessage(), null);
        }
    }

    private static HttpResponse<byte[]> checked(HttpResponse<byte[]> response, int... expected) {
        check(response, expected);
        return response;
    }

    private static void check(HttpResponse<byte[]> response, int... expected) {
        for (int status : expected) {
            if (response.statusCode() == status) {
                return;
            }
        }

        String code = "http_" + response.statusCode();
        String message = null;
        Long nextSeq = null;

        try {
            JsonNode node = MAPPER.readTree(response.body());

            code = node.path("error").asText(code);
            message = node.path("message").asText(null);
            nextSeq = node.has("next_seq") ? node.path("next_seq").asLong() : null;
        } catch (Exception ignored) {
        }

        throw new StreamStackException(response.statusCode(), code, message, nextSeq);
    }

    private static JsonNode json(byte[] body) {
        try {
            return MAPPER.readTree(body);
        } catch (Exception e) {
            throw new StreamStackException(0, "invalid_response", e.getMessage(), null);
        }
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.join();
        } catch (Exception e) {
            Throwable cause = Objects.isNull(e.getCause()) ? e : e.getCause();

            if (cause instanceof StreamStackException se) {
                throw se;
            }

            throw new StreamStackException(0, "transport", cause.getMessage(), null);
        }
    }

    private static String header(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).orElse(null);
    }

    private static long longHeader(HttpResponse<?> response, String name, long fallback) {
        Long value = nullableLongHeader(response, name);
        return Objects.isNull(value) ? fallback : value;
    }

    private static Long nullableLongHeader(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).map(Long::parseLong).orElse(null);
    }

    private static boolean boolHeader(HttpResponse<?> response, String name) {
        return Protocol.BOOL_TRUE.equalsIgnoreCase(header(response, name));
    }

    private static Instant instantHeader(HttpResponse<?> response, String name) {
        String value = header(response, name);
        return Objects.isNull(value) ? null : Instant.parse(value);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String trimSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    public static final class Builder {

        private String baseUrl;
        private HttpClient httpClient;
        private Duration requestTimeout = Duration.ofSeconds(30);

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public StreamStack build() {
            return new StreamStack(this);
        }
    }
}
