package io.streamstack.server.model;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public record SubmittedAppendResult(AppendResult result, CompletableFuture<AppendResult> durable) {

    public static SubmittedAppendResult completed(AppendResult result) {
        return new SubmittedAppendResult(result, CompletableFuture.completedFuture(result));
    }

    public AppendResult await(Duration timeout) throws StreamServiceException {
        try {
            return durable.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw StreamServiceException.durability(e.getCause());
        } catch (TimeoutException e) {
            throw StreamServiceException.durability(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw StreamServiceException.durability(e);
        }
    }
}
