package io.streamstack.metadata.raft;

import java.util.Objects;

import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

import io.streamstack.metadata.MetadataException;
import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.metadata.codec.MetadataCommandCodec;
import io.streamstack.metadata.model.MetadataCommandRequest;
import io.streamstack.metadata.model.MetadataCommandResponse;
import io.streamstack.metadata.codec.MetadataResultCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class MetadataClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataClient.class);

    private final MetadataNode node;
    private final String groupId;
    private final CliClientServiceImpl cliClientService;
    private final int maxRetries;
    private final long retrySleepMs;
    private final long rpcTimeoutMs;
    private final ScheduledExecutorService scheduler;

    public MetadataClient(MetadataNode node, Configuration configuration) {
        this(node, configuration, 60, 200L, 5_000L);
    }

    public MetadataClient(MetadataNode node, Configuration configuration, int maxRetries, long retrySleepMs,
        long rpcTimeoutMs) {
        this.node = node;
        this.groupId = node.groupId();
        this.maxRetries = maxRetries;
        this.retrySleepMs = retrySleepMs;
        this.rpcTimeoutMs = rpcTimeoutMs;
        this.cliClientService = new CliClientServiceImpl();
        this.cliClientService.init(new CliOptions());
        RouteTable.getInstance().updateConfiguration(groupId, configuration);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metadata-client-" + node.nodeId());

            t.setDaemon(true);

            return t;
        });
    }

    public MetadataNode node() {
        return node;
    }

    public CompletableFuture<Object> propose(MetadataCommand command) {
        CompletableFuture<Object> future = new CompletableFuture<>();

        attempt(command, 0, future);

        return future;
    }

    private void attempt(MetadataCommand command, int attempt, CompletableFuture<Object> future) {
        if (node.isLeader()) {
            node.propose(command).whenComplete((result, error) -> {
                if (Objects.isNull(error)) {
                    future.complete(result);
                    return;
                }

                Throwable cause = unwrap(error);

                if (cause instanceof MetadataException metadataException) {
                    future.completeExceptionally(MetadataException.toStreamClientException(metadataException));
                } else {
                    retryOrFail(command, attempt, future, cause);
                }
            });

            return;
        }

        PeerId leader = discoverLeader();

        if (Objects.isNull(leader)) {
            retryOrFail(command, attempt, future,
                new MetadataNode.NotLeaderException(null));
            return;
        }

        MetadataCommandRequest request = new MetadataCommandRequest(MetadataCommandCodec.encode(command));

        try {
            cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, (result, err) -> {
                if (Objects.nonNull(err)) {
                    RouteTable.getInstance().updateLeader(groupId, (PeerId) null);
                    retryOrFail(command, attempt, future, err);

                    return;
                }

                MetadataCommandResponse response = (MetadataCommandResponse) result;

                switch (response.getStatus()) {
                    case MetadataCommandResponse.OK ->
                        future.complete(MetadataResultCodec.decode(response.getResult()));
                    case MetadataCommandResponse.NOT_LEADER -> {
                        if (Objects.nonNull(response.getLeaderId())) {
                            RouteTable.getInstance().updateLeader(groupId, response.getLeaderId());
                        } else {
                            RouteTable.getInstance().updateLeader(groupId, (PeerId) null);
                        }

                        retryOrFail(command, attempt, future,
                            new MetadataNode.NotLeaderException(response.getLeaderId()));
                    }
                    case MetadataCommandResponse.METADATA_ERROR ->
                        future.completeExceptionally(MetadataException.toStreamClientException(
                            new MetadataException(response.getErrorCode(), response.getErrorMessage())));
                    default ->
                        retryOrFail(command, attempt, future,
                            new IllegalStateException(response.getErrorMessage()));
                }
            }, rpcTimeoutMs);
        } catch (Throwable t) {
            RouteTable.getInstance().updateLeader(groupId, (PeerId) null);
            retryOrFail(command, attempt, future, t);
        }
    }

    private void retryOrFail(MetadataCommand command, int attempt, CompletableFuture<Object> future, Throwable cause) {
        if (attempt >= maxRetries) {
            future.completeExceptionally(cause);
            return;
        }

        scheduler.schedule(() -> attempt(command, attempt + 1, future), retrySleepMs, TimeUnit.MILLISECONDS);
    }

    private PeerId discoverLeader() {
        RouteTable routeTable = RouteTable.getInstance();
        PeerId leader = routeTable.selectLeader(groupId);

        if (Objects.nonNull(leader)) {
            return leader;
        }

        try {
            Status status = routeTable.refreshLeader(cliClientService, groupId, (int) rpcTimeoutMs);

            if (!status.isOk()) {
                LOGGER.debug("refresh leader failed: {}", status.getErrorMsg());
            }
        } catch (Exception e) {
            LOGGER.debug("refresh leader failed", e);
        }

        return routeTable.selectLeader(groupId);
    }

    public CompletableFuture<Void> readIndex(Runnable read) {
        return readIndex(() -> {
            read.run();
            return null;
        });
    }

    public <T> CompletableFuture<T> readIndex(Supplier<T> read) {
        CompletableFuture<T> future = new CompletableFuture<>();
        attemptReadIndex(read, 0, future);
        return future;
    }

    private <T> void attemptReadIndex(Supplier<T> read, int attempt, CompletableFuture<T> future) {
        if (future.isDone()) {
            return;
        }

        try {
            node.raftNode().readIndex(null, new ReadIndexClosure() {
                @Override
                public void run(Status status, long index, byte[] reqCtx) {
                    if (!status.isOk()) {
                        if (status.getRaftError() == RaftError.EAGAIN && attempt < maxRetries) {
                            scheduleReadIndexRetry(read, attempt, future);
                        } else {
                            future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
                        }
                        return;
                    }

                    node.stateMachine().awaitApplied(index).whenCompleteAsync((ignored, error) -> {
                        if (Objects.nonNull(error)) {
                            future.completeExceptionally(unwrap(error));
                            return;
                        }

                        try {
                            future.complete(node.stateMachine().read(read));
                        } catch (Throwable t) {
                            Throwable cause = unwrap(t);

                            if (cause instanceof MetadataException metadataException) {
                                future.completeExceptionally(MetadataException.toStreamClientException(metadataException));
                            } else {
                                future.completeExceptionally(cause);
                            }
                        }
                    }, scheduler);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(unwrap(t));
        }
    }

    private <T> void scheduleReadIndexRetry(Supplier<T> read, int attempt, CompletableFuture<T> future) {
        try {
            scheduler.schedule(
                () -> attemptReadIndex(read, attempt + 1, future),
                retrySleepMs,
                TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        cliClientService.shutdown();
    }

    private static Throwable unwrap(Throwable t) {
        Throwable cur = t;

        while (cur instanceof CompletionException && Objects.nonNull(cur.getCause())) {
            cur = cur.getCause();
        }

        return cur;
    }
}
