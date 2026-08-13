package io.streamstack.client.helper;

import java.util.Objects;
import java.time.Duration;
import java.util.Set;

public final class RetryPolicy {

    private final int maxRetries;
    private final Duration initialDelay;
    private final Duration maxDelay;
    private final double multiplier;
    private final Set<Integer> retryableStatuses;

    public RetryPolicy(
        int maxRetries,
        Duration initialDelay,
        Duration maxDelay,
        double multiplier,
        Set<Integer> retryableStatuses) {
        this.maxRetries = maxRetries;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
        this.multiplier = multiplier;
        this.retryableStatuses = Objects.isNull(retryableStatuses) ? Set.of() : Set.copyOf(retryableStatuses);
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(30), 2.0,
            Set.of(429, 500, 502, 503, 504));
    }

    public static RetryPolicy none() {
        return new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 1.0, Set.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxRetries() {
        return maxRetries;
    }

    public boolean shouldRetry(int statusCode, int attempt) {
        if (attempt >= maxRetries) {
            return false;
        }

        return retryableStatuses.contains(statusCode);
    }

    public Duration delay(int attempt) {
        if (attempt <= 1) {
            return initialDelay;
        }

        double delay = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);

        return Duration.ofMillis(Math.min((long) delay, maxDelay.toMillis()));
    }

    public static final class Builder {
        private int maxRetries = 3;
        private Duration initialDelay = Duration.ofMillis(100);
        private Duration maxDelay = Duration.ofSeconds(30);
        private double multiplier = 2.0;
        private Set<Integer> retryableStatuses = Set.of(429, 500, 502, 503, 504);
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder initialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        public Builder maxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
            return this;
        }

        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        public Builder retryableStatuses(Set<Integer> statuses) {
            this.retryableStatuses = statuses;
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(maxRetries, initialDelay, maxDelay, multiplier, retryableStatuses);
        }
    }
}
