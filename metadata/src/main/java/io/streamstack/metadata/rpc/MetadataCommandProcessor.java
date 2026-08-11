package io.streamstack.metadata.rpc;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import io.streamstack.metadata.MetadataException;
import io.streamstack.metadata.codec.MetadataCommandCodec;
import io.streamstack.metadata.codec.MetadataResultCodec;
import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.metadata.model.MetadataCommandRequest;
import io.streamstack.metadata.model.MetadataCommandResponse;
import io.streamstack.metadata.raft.MetadataNode;

import java.util.concurrent.CompletionException;

public final class MetadataCommandProcessor implements RpcProcessor<MetadataCommandRequest> {
    private final MetadataNode node;

    public MetadataCommandProcessor(MetadataNode node) {
        this.node = node;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, MetadataCommandRequest request) {
        MetadataCommand command;
        try {
            command = MetadataCommandCodec.decode(request.getCommand());
        } catch (Throwable t) {
            rpcCtx.sendResponse(MetadataCommandResponse.raftError("invalid command: " + t.getMessage()));
            return;
        }
        node.propose(command).whenComplete((result, error) -> {
            if (error == null) {
                rpcCtx.sendResponse(MetadataCommandResponse.ok(MetadataResultCodec.encode(result)));
                return;
            }
            Throwable cause = unwrap(error);
            if (cause instanceof MetadataNode.NotLeaderException notLeader) {
                rpcCtx.sendResponse(MetadataCommandResponse.notLeader(notLeader.leaderId()));
            } else if (cause instanceof MetadataException metadataException) {
                rpcCtx.sendResponse(MetadataCommandResponse.metadataError(
                    metadataException.code(), metadataException.getMessage()));
            } else {
                rpcCtx.sendResponse(MetadataCommandResponse.raftError(String.valueOf(cause.getMessage())));
            }
        });
    }

    @Override
    public String interest() {
        return MetadataCommandRequest.class.getName();
    }

    private static Throwable unwrap(Throwable error) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
