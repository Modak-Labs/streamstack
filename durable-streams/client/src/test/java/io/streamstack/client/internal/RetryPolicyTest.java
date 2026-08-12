package io.streamstack.client.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RetryPolicyTest {

    @Test
    void shouldRetryRespectsConfiguredStatusSet() {
        RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(10), Duration.ofSeconds(1), 2.0, Set.of(503));
        assertTrue(policy.shouldRetry(503, 0));
        assertFalse(policy.shouldRetry(500, 0), "5xx outside the configured set must not be retried");
        assertFalse(policy.shouldRetry(502, 0));
        assertFalse(policy.shouldRetry(503, 3), "attempts are capped at maxRetries");
    }

    @Test
    void noneNeverRetries() {
        RetryPolicy policy = RetryPolicy.none();
        assertFalse(policy.shouldRetry(503, 0));
        assertFalse(policy.shouldRetry(500, 0));
    }

    @Test
    void firstBackoffIsInitialDelay() {
        RetryPolicy policy = new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(30), 2.0, Set.of(503));
        assertEquals(Duration.ofMillis(100), policy.delay(1));
        assertEquals(Duration.ofMillis(200), policy.delay(2));
        assertEquals(Duration.ofMillis(400), policy.delay(3));
    }

    @Test
    void delayIsCappedAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(10, Duration.ofMillis(100), Duration.ofMillis(150), 2.0, Set.of(503));
        assertEquals(Duration.ofMillis(150), policy.delay(5));
    }
}
