package io.streamstack.client;

import java.util.Objects;

import io.streamstack.client.internal.ErrorMapper;
import io.streamstack.client.internal.RetryPolicy;
import io.streamstack.client.internal.SseStreamingReader;
import io.streamstack.client.model.Chunk;
import io.streamstack.client.model.CloseResult;
import io.streamstack.client.model.ProducerConfig;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.Protocol;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.request.AppendRequest;
import io.streamstack.model.request.CreateRequest;
import io.streamstack.model.request.ReadRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.CreateResponse;
import io.streamstack.model.response.HeadResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public final class DurableStream implements AutoCloseable {

    private final HttpClient httpClient;
    private final ExecutorService ownedExecutor;
    private final RetryPolicy retryPolicy;
    private final Map<String, String> defaultHeaders;
    private final Map<String, Supplier<String>> dynamicHeaders;
    private final Map<String, String> defaultParams;
    private final Map<String, Supplier<String>> dynamicParams;
    private final Map<String, String> contentTypeCache;

    private DurableStream(Builder builder) {
        if (Objects.nonNull(builder.httpClient)) {
            this.httpClient = builder.httpClient;
            this.ownedExecutor = null;
        } else {
            this.ownedExecutor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "durable-streams-http");

                t.setDaemon(true);

                return t;
            });

            this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .executor(ownedExecutor)
                .build();
        }

        this.retryPolicy = Objects.nonNull(builder.retryPolicy) ? builder.retryPolicy : RetryPolicy.defaults();
        this.defaultHeaders = new HashMap<>(builder.defaultHeaders);
        this.dynamicHeaders = new ConcurrentHashMap<>(builder.dynamicHeaders);
        this.defaultParams = new HashMap<>(builder.defaultParams);
        this.dynamicParams = new ConcurrentHashMap<>(builder.dynamicParams);
        this.contentTypeCache = new ConcurrentHashMap<>();
    }

    public static DurableStream create() {
        return new DurableStream(new Builder());
    }

    public static Builder builder() {
        return new Builder();
    }

    public CreateResponse create(String url) {
        return create(url, new CreateRequest("application/octet-stream", null, null, false, null));
    }

    public CreateResponse create(String url, String contentType) {
        return create(url, new CreateRequest(contentType, null, null, false, null));
    }

    public CreateResponse create(String url, CreateRequest request) {
        HttpRequest http = buildCreateRequest(url, request);
        return executeWithRetry(http, true, response -> parseCreate(response, url, request));
    }

    public AppendResponse append(String url, byte[] data) {
        return append(url, new AppendRequest(cachedContentType(url), data, null, null, null, null, false));
    }

    public AppendResponse append(String url, AppendRequest request) {
        if ((Objects.isNull(request.body()) || request.body().length == 0) && !request.close()) {
            throw new DurableStreamException("Cannot append empty data");
        }

        HttpRequest http = buildAppendRequest(url, request);
        boolean producer = Objects.nonNull(request.producerId());
        boolean hadBody = Objects.nonNull(request.body()) && request.body().length > 0;

        return executeWithRetry(http, producer, response -> parseAppend(response, url, producer, hadBody));
    }

    public CompletableFuture<AppendResponse> appendAsync(String url, byte[] data) {
        try {
            AppendRequest request = new AppendRequest(cachedContentType(url), data, null, null, null, null, false);

            if (request.body().length == 0) {
                return CompletableFuture.failedFuture(new DurableStreamException("Cannot append empty data"));
            }

            HttpRequest http = buildAppendRequest(url, request);

            return executeWithRetryAsync(http, false,
                response -> parseAppend(response, url, false, true), 0);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(wrap(e));
        }
    }

    public HeadResponse head(String url) {
        HttpRequest http = buildHeadRequest(url);
        return executeWithRetry(http, true, response -> parseHead(response, url));
    }

    public void delete(String url) {
        HttpRequest http = buildDeleteRequest(url);

        executeWithRetry(http, true, response -> {
            int status = response.statusCode();

            if (status == 200 || status == 204) {
                return null;
            }

            throw ErrorMapper.map(url, response);
        });
    }

    public CloseResult close(String url) {
        return close(url, null, null);
    }

    public CloseResult close(String url, byte[] data, String contentType) {
        boolean wasClosed = head(url).closed();
        AppendRequest request = new AppendRequest(
            Objects.nonNull(contentType) ? contentType : cachedContentType(url),
            data,
            null,
            null,
            null,
            null,
            true);
        AppendResponse response = append(url, request);

        return new CloseResult(response.nextOffset(), wasClosed);
    }

    public ChunkIterator read(String url) {
        return read(url, new ReadRequest(Offset.beginning(), null, null), null);
    }

    public ChunkIterator read(String url, ReadRequest request) {
        return read(url, request, null);
    }

    public ChunkIterator read(String url, ReadRequest request, Duration timeout) {
        return new ChunkIterator(this, url, request, timeout);
    }

    public <T> JsonIterator<T> readJson(String url, Function<String, List<T>> parser) {
        return readJson(url, parser, new ReadRequest(Offset.beginning(), null, null), null);
    }

    public <T> JsonIterator<T> readJson(String url, Function<String, List<T>> parser, ReadRequest request) {
        return readJson(url, parser, request, null);
    }

    public <T> JsonIterator<T> readJson(
        String url,
        Function<String, List<T>> parser,
        ReadRequest request,
        Duration timeout) {
        return new JsonIterator<>(read(url, request, timeout), parser);
    }

    public IdempotentProducer producer(String url, String producerId) {
        return producer(url, producerId, ProducerConfig.defaults());
    }

    public IdempotentProducer producer(String url, String producerId, ProducerConfig config) {
        return new IdempotentProducer(this, url, producerId, config);
    }

    HttpClient httpClient() {
        return httpClient;
    }

    Map<String, String> resolveHeaders() {
        Map<String, String> headers = new HashMap<>(defaultHeaders);
        dynamicHeaders.forEach((name, supplier) -> headers.put(name, supplier.get()));

        return headers;
    }

    String cachedContentType(String url) {
        return contentTypeCache.get(url);
    }

    void cacheContentType(String url, String contentType) {
        if (Objects.nonNull(contentType)) {
            contentTypeCache.put(url, contentType);
        }
    }

    Chunk readOnce(String url, ReadRequest request, Duration timeout) {
        HttpRequest http = buildReadRequest(url, request, timeout);
        return executeWithRetry(http, true, response -> parseRead(response, url));
    }

    SseStreamingReader openSseStream(String url, Offset offset, String cursor) {
        HttpRequest http = buildSseRequest(url, offset, cursor);
        return new SseStreamingReader(httpClient, http, offset);
    }

    AppendResponse appendProducer(
        String url,
        byte[] data,
        String contentType,
        String producerId,
        long epoch,
        long seq,
        boolean close) {
        AppendRequest request = new AppendRequest(contentType, data, null, producerId, epoch, seq, close);
        HttpRequest http = buildAppendRequest(url, request);

        return executeWithRetry(http, true,
            response -> parseAppend(response, url, true, Objects.nonNull(data) && data.length > 0));
    }

    CompletableFuture<HttpResponse<byte[]>> sendAsync(HttpRequest request) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    @Override
    public void close() {
        if (Objects.nonNull(ownedExecutor)) {
            ownedExecutor.shutdown();

            try {
                if (!ownedExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    ownedExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                ownedExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private HttpRequest buildCreateRequest(String url, CreateRequest request) {
        byte[] body = request.initialBody();
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.noBody();

        if (body.length > 0) {
            if (Objects.nonNull(request.contentType()) && request.contentType().toLowerCase().contains("json")) {
                body = wrapInJsonArray(body);
            }

            publisher = HttpRequest.BodyPublishers.ofByteArray(body);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(withParams(url)))
            .method("PUT", publisher)
            .timeout(Duration.ofSeconds(30));
        resolveHeaders().forEach(builder::header);

        if (Objects.nonNull(request.contentType())) {
            builder.header(Protocol.H_CONTENT_TYPE, request.contentType());
        }

        if (Objects.nonNull(request.ttlSeconds())) {
            builder.header(Protocol.H_STREAM_TTL, Long.toString(request.ttlSeconds()));
        }

        if (Objects.nonNull(request.expiresAt())) {
            builder.header(Protocol.H_STREAM_EXPIRES_AT, request.expiresAt().toString());
        }

        if (request.closed()) {
            builder.header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        }

        return builder.build();
    }

    private HttpRequest buildAppendRequest(String url, AppendRequest request) {
        byte[] body = request.body();
        String contentType = Objects.nonNull(request.contentType()) ? request.contentType() : cachedContentType(url);

        if (Objects.isNull(contentType)) {
            contentType = "application/octet-stream";
        }

        if (body.length > 0 && contentType.toLowerCase().contains("json") && !looksLikeJsonArray(body)) {
            body = wrapInJsonArray(body);
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(withParams(url)))
            .POST(body.length == 0
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(body))
            .timeout(Duration.ofSeconds(30));
        resolveHeaders().forEach(builder::header);
        builder.header(Protocol.H_CONTENT_TYPE, contentType);

        if (Objects.nonNull(request.streamSeq())) {
            builder.header(Protocol.H_STREAM_SEQ, request.streamSeq());
        }

        if (Objects.nonNull(request.producerId())) {
            builder.header(Protocol.H_PRODUCER_ID, request.producerId());
            builder.header(Protocol.H_PRODUCER_EPOCH, Long.toString(request.producerEpoch()));
            builder.header(Protocol.H_PRODUCER_SEQ, Long.toString(request.producerSeq()));
        }

        if (request.close()) {
            builder.header(Protocol.H_STREAM_CLOSED, Protocol.BOOL_TRUE);
        }

        return builder.build();
    }

    private HttpRequest buildHeadRequest(String url) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(withParams(url)))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(30));
        resolveHeaders().forEach(builder::header);

        return builder.build();
    }

    private HttpRequest buildDeleteRequest(String url) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(withParams(url)))
            .DELETE()
            .timeout(Duration.ofSeconds(30));
        resolveHeaders().forEach(builder::header);

        return builder.build();
    }

    private HttpRequest buildReadRequest(String url, ReadRequest request, Duration timeout) {
        List<String> params = new ArrayList<>();

        resolveParams().forEach((k, v) -> params.add(encode(k) + "=" + encode(v)));
        Offset offset = Objects.isNull(request.offset()) ? Offset.beginning() : request.offset();

        params.add(Protocol.Q_OFFSET + "=" + encode(offset.value()));

        if (Objects.nonNull(request.live())) {
            params.add(Protocol.Q_LIVE + "=" + encode(request.live().wire()));
        }

        if (Objects.nonNull(request.cursor())) {
            params.add(Protocol.Q_CURSOR + "=" + encode(request.cursor()));
        }

        Collections.sort(params);
        String full = url + "?" + String.join("&", params);
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(full)).GET();

        if (Objects.nonNull(timeout)) {
            builder.timeout(timeout);
        } else if (request.live() == LiveMode.LONG_POLL) {
            builder.timeout(Duration.ofSeconds(65));
        } else {
            builder.timeout(Duration.ofSeconds(30));
        }

        resolveHeaders().forEach(builder::header);

        if (request.live() == LiveMode.SSE) {
            builder.header("Accept", Protocol.CT_EVENT_STREAM);
        }

        return builder.build();
    }

    private HttpRequest buildSseRequest(String url, Offset offset, String cursor) {
        List<String> params = new ArrayList<>();

        resolveParams().forEach((k, v) -> params.add(encode(k) + "=" + encode(v)));

        if (Objects.nonNull(offset)) {
            params.add(Protocol.Q_OFFSET + "=" + encode(offset.value()));
        }

        params.add(Protocol.Q_LIVE + "=" + Protocol.LIVE_SSE);

        if (Objects.nonNull(cursor)) {
            params.add(Protocol.Q_CURSOR + "=" + encode(cursor));
        }

        Collections.sort(params);
        String full = url + "?" + String.join("&", params);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(full))
            .GET()
            .header("Accept", Protocol.CT_EVENT_STREAM);
        resolveHeaders().forEach(builder::header);

        return builder.build();
    }

    private CreateResponse parseCreate(HttpResponse<byte[]> response, String url, CreateRequest request) {
        int status = response.statusCode();

        if (status == 201 || status == 200) {
            String contentType = response.headers().firstValue(Protocol.H_CONTENT_TYPE)
                .orElse(request.contentType());
            cacheContentType(url, contentType);
            Offset next = response.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).map(Offset::of).orElse(null);
            boolean closed = Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null));
            return new CreateResponse(status == 201, contentType, next, closed);
        }

        throw ErrorMapper.mapCreate(url, response);
    }

    private AppendResponse parseAppend(HttpResponse<byte[]> response, String url, boolean producer, boolean hadBody) {
        int status = response.statusCode();

        response.headers().firstValue(Protocol.H_CONTENT_TYPE).ifPresent(ct -> cacheContentType(url, ct));

        if (status == 200 || status == 201 || status == 204) {
            Offset next = response.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).map(Offset::of).orElse(null);
            boolean closed = Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null));
            Long epoch = response.headers().firstValue(Protocol.H_PRODUCER_EPOCH).map(Long::parseLong).orElse(null);
            Long seq = response.headers().firstValue(Protocol.H_PRODUCER_SEQ).map(Long::parseLong).orElse(null);
            boolean appended = producer ? status != 204 : hadBody;

            return new AppendResponse(next, appended, closed, epoch, seq);
        }

        throw ErrorMapper.map(url, response);
    }

    private HeadResponse parseHead(HttpResponse<byte[]> response, String url) {
        if (response.statusCode() != 200) {
            throw ErrorMapper.map(url, response);
        }

        String contentType = response.headers().firstValue(Protocol.H_CONTENT_TYPE).orElse(null);

        cacheContentType(url, contentType);
        Long ttl = response.headers().firstValue(Protocol.H_STREAM_TTL).map(Long::parseLong).orElse(null);
        Instant expiresAt = response.headers().firstValue(Protocol.H_STREAM_EXPIRES_AT).map(Instant::parse).orElse(null);
        Offset next = response.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).map(Offset::of).orElse(null);
        boolean closed = Protocol.BOOL_TRUE.equalsIgnoreCase(
            response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null));
        return new HeadResponse(contentType, ttl, expiresAt, next, closed);
    }

    private Chunk parseRead(HttpResponse<byte[]> response, String url) {
        int status = response.statusCode();

        if (status == 200 || status == 204) {
            response.headers().firstValue(Protocol.H_CONTENT_TYPE).ifPresent(ct -> cacheContentType(url, ct));
            Offset next = response.headers().firstValue(Protocol.H_STREAM_NEXT_OFFSET).map(Offset::of).orElse(null);
            boolean upToDate = Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_UP_TO_DATE).orElse(null));
            boolean closed = Protocol.BOOL_TRUE.equalsIgnoreCase(
                response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null));
            String cursor = response.headers().firstValue(Protocol.H_STREAM_CURSOR).orElse(null);

            Map<String, String> headers = new HashMap<>();
            response.headers().map().forEach((k, v) -> {
                if (!v.isEmpty()) {
                    headers.put(k.toLowerCase(), v.get(0));
                }
            });

            byte[] body = status == 204 || Objects.isNull(response.body()) ? new byte[0] : response.body();

            return new Chunk(body, next, upToDate || status == 204, closed, cursor, status, headers);
        }

        throw ErrorMapper.map(url, response);
    }

    private <T> T executeWithRetry(HttpRequest request, boolean idempotent, ResponseHandler<T> handler) {
        int attempt = 0;

        while (true) {
            try {
                HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
                return handler.handle(response);
            } catch (DurableStreamException e) {
                if (idempotent && e.statusCode().isPresent()
                    && retryPolicy.shouldRetry(e.statusCode().get(), attempt)) {
                    attempt++;
                    sleep(retryPolicy.delay(attempt), e);
                    continue;
                }

                throw e;
            } catch (IOException e) {
                if (idempotent && attempt < retryPolicy.maxRetries()) {
                    attempt++;
                    sleep(retryPolicy.delay(attempt), new DurableStreamException(e.getMessage(), e));
                    continue;
                }

                throw new DurableStreamException(e.getMessage(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DurableStreamException("Request interrupted", e);
            }
        }
    }

    private <T> CompletableFuture<T> executeWithRetryAsync(
        HttpRequest request,
        boolean idempotent,
        ResponseHandler<T> handler,
        int attempt) {
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
            .handle((response, error) -> {
                if (Objects.isNull(error)) {
                    try {
                        return CompletableFuture.completedFuture(handler.handle(response));
                    } catch (DurableStreamException e) {
                        if (idempotent && e.statusCode().isPresent()
                            && retryPolicy.shouldRetry(e.statusCode().get(), attempt)) {
                            return retryAsync(request, idempotent, handler, attempt + 1);
                        }

                        return CompletableFuture.<T>failedFuture(e);
                    }
                }

                Throwable cause = error instanceof CompletionException && Objects.nonNull(error.getCause())
                    ? error.getCause()
                    : error;
                if (idempotent && cause instanceof IOException && attempt < retryPolicy.maxRetries()) {
                    return retryAsync(request, idempotent, handler, attempt + 1);
                }

                return CompletableFuture.<T>failedFuture(cause instanceof DurableStreamException dse
                    ? dse
                    : new DurableStreamException(cause.getMessage(), cause));
            })

            .thenCompose(Function.identity());
    }

    private <T> CompletableFuture<T> retryAsync(
        HttpRequest request,
        boolean idempotent,
        ResponseHandler<T> handler,
        int attempt) {
        Executor delayed = CompletableFuture.delayedExecutor(
            retryPolicy.delay(attempt).toMillis(), TimeUnit.MILLISECONDS);
        return CompletableFuture.supplyAsync(() -> null, delayed)
            .thenCompose(ignored -> executeWithRetryAsync(request, idempotent, handler, attempt));
    }

    private void sleep(Duration delay, DurableStreamException fallback) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw fallback;
        }
    }

    private String withParams(String url) {
        Map<String, String> params = resolveParams();

        if (params.isEmpty()) {
            return url;
        }

        List<String> parts = new ArrayList<>();

        params.forEach((k, v) -> parts.add(encode(k) + "=" + encode(v)));
        Collections.sort(parts);

        return url + "?" + String.join("&", parts);
    }

    private Map<String, String> resolveParams() {
        Map<String, String> params = new HashMap<>(defaultParams);
        dynamicParams.forEach((name, supplier) -> params.put(name, supplier.get()));

        return params;
    }

    static boolean looksLikeJsonArray(byte[] body) {
        for (byte b : body) {
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
                continue;
            }

            return b == '[';
        }

        return false;
    }

    static byte[] wrapInJsonArray(byte[] data) {
        byte[] prefix = {'['};
        byte[] suffix = {']'};
        byte[] result = new byte[prefix.length + data.length + suffix.length];

        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(data, 0, result, prefix.length, data.length);
        System.arraycopy(suffix, 0, result, prefix.length + data.length, suffix.length);

        return result;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static DurableStreamException wrap(Exception e) {
        return e instanceof DurableStreamException dse ? dse : new DurableStreamException("Failed to build request", e);
    }

    @FunctionalInterface
    private interface ResponseHandler<T> {

        T handle(HttpResponse<byte[]> response);
    }

    public static final class Builder {
        private HttpClient httpClient;
        private RetryPolicy retryPolicy;
        private final Map<String, String> defaultHeaders = new HashMap<>();
        private final Map<String, Supplier<String>> dynamicHeaders = new HashMap<>();
        private final Map<String, String> defaultParams = new HashMap<>();
        private final Map<String, Supplier<String>> dynamicParams = new HashMap<>();
        public Builder httpClient(HttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder header(String name, String value) {
            defaultHeaders.put(name, value);
            return this;
        }

        public Builder header(String name, Supplier<String> supplier) {
            dynamicHeaders.put(name, supplier);
            return this;
        }

        public Builder param(String name, String value) {
            defaultParams.put(name, value);
            return this;
        }

        public Builder param(String name, Supplier<String> supplier) {
            dynamicParams.put(name, supplier);
            return this;
        }

        public DurableStream build() {
            return new DurableStream(this);
        }
    }
}
