package io.streamstack.metadata.raft;

import com.alipay.sofa.jraft.entity.PeerId;

public final class MetadataHealth {
    private final MetadataNode node;

    public MetadataHealth(MetadataNode node) {
        this.node = node;
    }

    public boolean isLeader() {
        return node.isLeader();
    }

    public String leaderId() {
        PeerId leader = node.leaderId();
        return leader == null ? null : leader.toString();
    }

    public int nodeId() {
        return node.nodeId();
    }

    public long nodeEpoch() {
        return node.nodeEpoch();
    }

    public boolean isRegistered() {
        return node.isRegistered();
    }

    public long appliedIndex() {
        return node.stateMachine().appliedIndex();
    }

    public int streamCount() {
        return node.stateMachine().read(() ->
            node.stateMachine().streamControlManager().streamsMetadata().size());
    }

    public int objectCount() {
        return node.stateMachine().read(() ->
            node.stateMachine().objectControlManager().getObjectsCount());
    }

    public int destroyedBacklog() {
        return node.stateMachine().read(() ->
            node.stateMachine().objectControlManager().markDestroyedObjects().size());
    }

    public long applySuccessCount() {
        return node.stateMachine().applySuccessCount();
    }

    public long applyFailCount() {
        return node.stateMachine().applyFailCount();
    }
}
