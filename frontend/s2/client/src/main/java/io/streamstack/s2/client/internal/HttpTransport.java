package io.streamstack.s2.client.internal;

import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.exception.S2Exception;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

public final class HttpTransport implements AutoCloseable {

    private final HttpClient httpClient;
    private final String endpoint;
    private final Format format;
    private final RetryPolicy retryPolicy;

    public HttpTransport(HttpClient httpClient, boolean ownsClient, String endpoint, Format format, RetryPolicy retryPolicy) {
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.format = Objects.isNull(format) ? Format.RAW : format;
        this.retryPolicy = Objects.isNull(retryPolicy) ? RetryPolicy.defaults() : retryPolicy;
    }

    public Format format() {
        return format;
    }

    public HttpRequest.Builder request(String path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + path))
            .timeout(Duration.ofSeconds(60))
            .header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
        if (format == Format.BASE64) {
            builder.header(Protocol.H_FORMAT, "base64");
        }

        return builder;
    }

    public HttpRequest.Builder withBasin(HttpRequest.Builder builder, String basin) {
        return builder.header(Protocol.H_BASIN, Objects.requireNonNull(basin, "basin"));
    }

    public HttpRequest.BodyPublisher jsonBody(Object body) {
        return HttpRequest.BodyPublishers.ofByteArray(S2Json.write(body, format));
    }

    public void execute(HttpRequest.Builder builder, int... expected) {
        execute(builder, Void.class, expected);
    }

    public <T> T execute(HttpRequest.Builder builder, Class<T> type, int... expected) {
        HttpRequest request = builder.build();
        boolean idempotent = !"POST".equals(request.method());
        int attempt = 0;

        while (true) {
            try {
                HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());

                for (int status : expected) {
                    if (response.statusCode() == status) {
                        if (type == Void.class || response.body().length == 0) {
                            return null;
                        }

                        return S2Json.read(response.body(), type, format);
                    }
                }

                if (idempotent && retryPolicy.retryable(response.statusCode()) && attempt < retryPolicy.maxRetries()) {
                    sleep(retryPolicy.delayForAttempt(++attempt));
                    continue;
                }

                throw ErrorMapper.map(response.statusCode(), response.body());
            } catch (S2Exception e) {
                throw e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw S2Exception.unavailable(e.getMessage());
            } catch (Exception e) {
                if (idempotent && attempt < retryPolicy.maxRetries()) {
                    try {
                        sleep(retryPolicy.delayForAttempt(++attempt));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw S2Exception.unavailable(ie.getMessage());
                    }

                    continue;
                }

                throw S2Exception.unavailable(e.getMessage());
            }
        }
    }

    public InputStream executeStream(HttpRequest.Builder builder) {
        try {
            HttpResponse<InputStream> response = httpClient.send(
                builder.build(), HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            }

            byte[] body = response.body().readAllBytes();

            throw ErrorMapper.map(response.statusCode(), body);
        } catch (S2Exception e) {
            throw e;
        } catch (Exception e) {
            throw S2Exception.unavailable(e.getMessage());
        }
    }

    public static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    public static String encodePath(String segment) {
        return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
    }

    public static void appendParam(StringBuilder query, String name, Object value) {
        if (Objects.isNull(value)) {
            return;
        }

        if (!query.isEmpty()) {
            query.append('&');
        }

        query.append(name).append('=').append(encode(String.valueOf(value)));
    }

    private static void sleep(Duration delay) throws InterruptedException {
        if (!delay.isZero() && !delay.isNegative()) {
            Thread.sleep(delay.toMillis());
        }
    }

    @Override
    public void close() {
    }
}
