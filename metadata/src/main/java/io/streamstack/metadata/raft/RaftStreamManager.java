package io.streamstack.metadata.raft;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.streams.StreamCloseHook;
import io.streamstack.s3.streams.StreamManager;
import io.streamstack.s3.streams.StreamMetadataListener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public final class RaftStreamManager implements StreamManager {
    private final MetadataNode metadataNode;
    private final MetadataClient client;
    private StreamCloseHook streamCloseHook = streamId -> CompletableFuture.completedFuture(null);

    public RaftStreamManager(MetadataNode metadataNode) {
        this.metadataNode = metadataNode;
        this.client = metadataNode.client();
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getOpeningStreams() {
        return client.readIndex(() ->
            metadataNode.stateMachine().streamControlManager().getOpeningStreams(metadataNode.nodeId()));
    }

    @Override
    public CompletableFuture<List<StreamMetadata>> getStreams(List<Long> streamIds) {
        return client.readIndex(() ->
            metadataNode.stateMachine().streamControlManager().getStreams(streamIds));
    }

    @Override
    public StreamMetadataListener.Handle addMetadataListener(long streamId, StreamMetadataListener listener) {
        return metadataNode.stateMachine().streamControlManager().addMetadataListener(streamId, listener);
    }

    @Override
    public CompletableFuture<Long> createStream(Map<String, String> tags) {
        return client.propose(new MetadataCommand.CreateStream(metadataNode.nodeId(), metadataNode.nodeEpoch()))
            .thenApply(result -> (Long) result);
    }

    @Override
    public CompletableFuture<StreamMetadata> openStream(long streamId, long epoch, Map<String, String> tags) {
        return client.propose(
                new MetadataCommand.OpenStream(metadataNode.nodeId(), metadataNode.nodeEpoch(), streamId, epoch))
            .thenApply(result -> (StreamMetadata) result);
    }

    @Override
    public CompletableFuture<Void> trimStream(long streamId, long epoch, long newStartOffset) {
        return client.propose(new MetadataCommand.TrimStream(
                metadataNode.nodeId(), metadataNode.nodeEpoch(), streamId, epoch, newStartOffset))
            .thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> closeStream(long streamId, long epoch) {
        return streamCloseHook.beforeStreamClose(streamId)
            .thenCompose(ignored -> client.propose(new MetadataCommand.CloseStream(
                metadataNode.nodeId(), metadataNode.nodeEpoch(), streamId, epoch)))
            .thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> deleteStream(long streamId, long epoch) {
        return client.propose(new MetadataCommand.DeleteStream(
                metadataNode.nodeId(), metadataNode.nodeEpoch(), streamId, epoch))
            .thenApply(ignored -> null);
    }

    @Override
    public void setStreamCloseHook(StreamCloseHook hook) {
        this.streamCloseHook = hook == null ? streamId -> CompletableFuture.completedFuture(null) : hook;
    }
}
