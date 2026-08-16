package io.streamstack.client;

import io.streamstack.client.helper.RetryPolicy;

import java.io.Serializable;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class StreamStackConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String baseUrl;
    private final Duration connectTimeout;
    private final Duration requestTimeout;
    private final Duration longPollTimeout;
    private final Map<String, String> headers;
    private final RetryPolicy retryPolicy;

    private StreamStackConfig(Builder builder) {
        this.baseUrl = Objects.requireNonNull(builder.baseUrl, "baseUrl");
        this.connectTimeout = Objects.requireNonNull(builder.connectTimeout, "connectTimeout");
        this.requestTimeout = Objects.requireNonNull(builder.requestTimeout, "requestTimeout");
        this.longPollTimeout = Objects.requireNonNull(builder.longPollTimeout, "longPollTimeout");
        this.headers = Map.copyOf(builder.headers);
        this.retryPolicy = Objects.requireNonNull(builder.retryPolicy, "retryPolicy");
    }

    public static Builder builder() {
        return new Builder();
    }

    public String baseUrl() {
        return baseUrl;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public Duration longPollTimeout() {
        return longPollTimeout;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public StreamStack build() {
        StreamStack.Builder builder = StreamStack.builder()
            .baseUrl(baseUrl)
            .connectTimeout(connectTimeout)
            .requestTimeout(requestTimeout)
            .longPollTimeout(longPollTimeout)
            .retryPolicy(retryPolicy);

        headers.forEach(builder::header);

        return builder.build();
    }

    public static final class Builder {

        private String baseUrl;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration requestTimeout = Duration.ofSeconds(30);
        private Duration longPollTimeout = Duration.ofSeconds(65);
        private final Map<String, String> headers = new LinkedHashMap<>();
        private RetryPolicy retryPolicy = RetryPolicy.none();

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder longPollTimeout(Duration longPollTimeout) {
            this.longPollTimeout = longPollTimeout;
            return this;
        }

        public Builder header(String name, String value) {
            headers.put(
                Objects.requireNonNull(name, "name"),
                Objects.requireNonNull(value, "value"));
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            Objects.requireNonNull(headers, "headers").forEach(this::header);
            return this;
        }

        public Builder retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public StreamStackConfig build() {
            return new StreamStackConfig(this);
        }
    }
}
