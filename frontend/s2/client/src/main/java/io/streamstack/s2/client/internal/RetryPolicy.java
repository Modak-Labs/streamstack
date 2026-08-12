package io.streamstack.s2.client.internal;

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
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0,
            Set.of(429, 500, 502, 503, 504));
    }

    public static RetryPolicy none() {
        return new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 1.0, Set.of());
    }

    public int maxRetries() {
        return maxRetries;
    }

    public boolean retryable(int status) {
        return retryableStatuses.contains(status);
    }

    public Duration delayForAttempt(int attempt) {
        if (attempt <= 0 || initialDelay.isZero()) {
            return Duration.ZERO;
        }

        double millis = initialDelay.toMillis() * Math.pow(multiplier, attempt - 1);

        return Duration.ofMillis(Math.min((long) millis, maxDelay.toMillis()));
    }
}
