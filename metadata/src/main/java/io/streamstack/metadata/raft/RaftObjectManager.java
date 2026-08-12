package io.streamstack.metadata.raft;

import java.util.Objects;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.s3.metadata.S3ObjectMetadata;
import io.streamstack.s3.objects.CommitStreamSetObjectHook;
import io.streamstack.s3.objects.CommitStreamSetObjectRequest;
import io.streamstack.s3.objects.CommitStreamSetObjectResponse;
import io.streamstack.s3.objects.CompactStreamObjectRequest;
import io.streamstack.s3.objects.ObjectManager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class RaftObjectManager implements ObjectManager {

    private final MetadataNode metadataNode;
    private final MetadataClient client;

    private CommitStreamSetObjectHook commitStreamSetObjectHook =
        request -> CompletableFuture.completedFuture(null);

    public RaftObjectManager(MetadataNode metadataNode) {
        this.metadataNode = metadataNode;
        this.client = metadataNode.client();
    }

    @Override
    public CompletableFuture<Long> prepareObject(int count, long ttl) {
        return client.propose(new MetadataCommand.PrepareObject(
                metadataNode.nodeId(), metadataNode.nodeEpoch(), count, ttl, System.currentTimeMillis()))
            .thenApply(result -> (Long) result);
    }

    @Override
    public CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject(CommitStreamSetObjectRequest request) {
        return client.propose(
                new MetadataCommand.CommitStreamSetObject(
                    metadataNode.nodeId(), metadataNode.nodeEpoch(), request, System.currentTimeMillis()))
            .thenCompose(result -> commitStreamSetObjectHook.onCommitSuccess(request)
                .thenApply(ignored -> (CommitStreamSetObjectResponse) result));
    }

    @Override
    public CompletableFuture<Void> compactStreamObject(CompactStreamObjectRequest request) {
        return client.propose(new MetadataCommand.CompactStreamObject(
                metadataNode.nodeId(), metadataNode.nodeEpoch(), request, System.currentTimeMillis()))
            .thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return client.readIndex(() ->
            metadataNode.stateMachine().objectControlManager().getObjects(streamId, startOffset, endOffset, limit));
    }

    @Override
    public boolean isObjectExist(long objectId) {
        return metadataNode.stateMachine().read(() ->
            metadataNode.stateMachine().objectControlManager().isObjectExist(objectId));
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
        return client.readIndex(() ->
            metadataNode.stateMachine().objectControlManager().getServerObjects(metadataNode.nodeId()));
    }

    @Override
    public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset,
        int limit) {
        return client.readIndex(() ->
            metadataNode.stateMachine().objectControlManager().getStreamObjects(streamId, startOffset, endOffset, limit));
    }

    @Override
    public CompletableFuture<Integer> getObjectsCount() {
        return client.readIndex(() -> metadataNode.stateMachine().objectControlManager().getObjectsCount());
    }

    @Override
    public void setCommitStreamSetObjectHook(CommitStreamSetObjectHook hook) {
        this.commitStreamSetObjectHook =
            Objects.isNull(hook) ? request -> CompletableFuture.completedFuture(null) : hook;
    }
}
