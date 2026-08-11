package io.streamstack.server.service;

import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public final class RaftOwnershipService implements OwnershipService {
    private static final long OP_TIMEOUT_SEC = 10;

    private final MetadataNode metadataNode;
    private final S3StreamService streamService;

    public RaftOwnershipService(MetadataNode metadataNode, S3StreamService streamService) {
        this.metadataNode = Objects.requireNonNull(metadataNode, "metadataNode");
        this.streamService = Objects.requireNonNull(streamService, "streamService");
    }

    @Override
    public Owner ownerOf(String name) throws StreamServiceException {
        try {
            OptionalLong streamId = streamService.lookupStreamId(name);
            if (streamId.isEmpty()) {
                return Owner.local(OptionalLong.empty());
            }
            long id = streamId.getAsLong();
            return metadataNode.client().readIndex(() -> {
                List<StreamMetadata> streams =
                    metadataNode.stateMachine().streamControlManager().getStreams(List.of(id));
                if (streams.isEmpty() || streams.get(0).state() != StreamState.OPENED) {
                    return Owner.local(OptionalLong.of(id));
                }
                int ownerId = streams.get(0).nodeId();
                if (ownerId == metadataNode.nodeId()) {
                    return Owner.local(OptionalLong.of(id));
                }
                String address = metadataNode.stateMachine().streamControlManager().getNodeAddress(ownerId);
                if (address == null || address.isEmpty()) {
                    return Owner.local(OptionalLong.of(id));
                }
                return Owner.remote(id, ownerId, address);
            }).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (StreamServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamServiceException(
                StreamServiceException.Kind.BAD_REQUEST, null, false,
                e.getMessage() == null ? "ownership lookup failed" : e.getMessage());
        }
    }

    @Override
    public NodeMeta localNode() {
        return new NodeMeta(metadataNode.nodeId(), metadataNode.httpAddress());
    }
}
