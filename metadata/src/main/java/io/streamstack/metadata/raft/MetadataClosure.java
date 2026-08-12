package io.streamstack.metadata.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import io.streamstack.metadata.model.MetadataCommand;

import java.util.concurrent.CompletableFuture;

public final class MetadataClosure implements Closure {

    private final MetadataCommand command;

    private final CompletableFuture<Object> future = new CompletableFuture<>();

    public MetadataClosure(MetadataCommand command) {
        this.command = command;
    }

    public MetadataCommand command() {
        return command;
    }

    public CompletableFuture<Object> future() {
        return future;
    }

    public void success(Object result) {
        future.complete(result);
    }

    public void failure(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void run(Status status) {
        if (!status.isOk()) {
            future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
        }
    }
}
