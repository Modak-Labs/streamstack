package io.streamstack.client.helper;

import io.streamstack.client.StreamStackException;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

public final class RetryPolicy implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final double multiplier;
    private final Set<Integer> retryableStatuses;

    public RetryPolicy(
        int maxAttempts,
        Duration initialBackoff,
        Duration maxBackoff,
        double multiplier,
        Set<Integer> retryableStatuses) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1.0");
        }

        this.maxAttempts = maxAttempts;
        this.initialBackoff = Objects.requireNonNull(initialBackoff, "initialBackoff");
        this.maxBackoff = Objects.requireNonNull(maxBackoff, "maxBackoff");
        this.multiplier = multiplier;
        this.retryableStatuses = Objects.isNull(retryableStatuses)
            ? Set.of(429)
            : Set.copyOf(retryableStatuses);
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0, Set.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration initialBackoff() {
        return initialBackoff;
    }

    public Duration maxBackoff() {
        return maxBackoff;
    }

    public double multiplier() {
        return multiplier;
    }

    public Set<Integer> retryableStatuses() {
        return retryableStatuses;
    }

    public boolean shouldRetry(StreamStackException exception, int attempt) {
        if (attempt >= maxAttempts || Thread.currentThread().isInterrupted()) {
            return false;
        }

        int status = exception.status();

        return "transport".equals(exception.code())
            || retryableStatuses.contains(status)
            || status >= 500 && status <= 599;
    }

    public Duration backoff(int attempt) {
        if (initialBackoff.isZero() || attempt <= 0) {
            return Duration.ZERO;
        }

        double delay = initialBackoff.toMillis() * Math.pow(multiplier, attempt - 1);

        return Duration.ofMillis(Math.min((long) delay, maxBackoff.toMillis()));
    }

    public static final class Builder {

        private int maxAttempts = 1;
        private Duration initialBackoff = Duration.ofMillis(100);
        private Duration maxBackoff = Duration.ofSeconds(30);
        private double multiplier = 2.0;
        private Set<Integer> retryableStatuses = Set.of(429);

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder initialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
            return this;
        }

        public Builder maxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
            return this;
        }

        public Builder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        public Builder retryableStatuses(Set<Integer> retryableStatuses) {
            this.retryableStatuses = retryableStatuses;
            return this;
        }

        public RetryPolicy build() {
            return new RetryPolicy(maxAttempts, initialBackoff, maxBackoff, multiplier, retryableStatuses);
        }
    }
}
