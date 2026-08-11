package io.streamstack.server.http;

import io.javalin.http.Context;
import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.s3.metadata.StreamMetadata;
import io.streamstack.s3.metadata.StreamState;
import io.streamstack.server.store.S3StreamStore;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public final class OwnershipRouter {
    public enum Mode { REDIRECT, LOCAL_ALWAYS }

    private final MetadataNode metadataNode;
    private final S3StreamStore store;
    private final DurableStreamsHandler handler;
    private final Mode mode;

    public OwnershipRouter(
        MetadataNode metadataNode,
        S3StreamStore store,
        DurableStreamsHandler handler,
        Mode mode) {
        this.metadataNode = Objects.requireNonNull(metadataNode, "metadataNode");
        this.store = Objects.requireNonNull(store, "store");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.mode = mode == null ? Mode.REDIRECT : mode;
    }

    public void handle(Context ctx) {
        if (mode == Mode.LOCAL_ALWAYS || "PUT".equals(ctx.method().name())) {
            handler.handle(ctx);
            return;
        }
        try {
            String ownerHttp = ownerHttpAddress(ctx.path());
            if (ownerHttp == null || ownerHttp.equals(metadataNode.httpAddress())) {
                handler.handle(ctx);
                return;
            }
            String location = ownerHttp.endsWith("/") ? ownerHttp.substring(0, ownerHttp.length() - 1) : ownerHttp;
            location += ctx.path().startsWith("/") ? ctx.path() : "/" + ctx.path();
            if (ctx.queryString() != null && !ctx.queryString().isEmpty()) {
                location += "?" + ctx.queryString();
            }
            ctx.status(307);
            ctx.header(Protocol.H_LOCATION, location);
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        } catch (Exception e) {
            ctx.status(503);
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
            ctx.header("X-Error", e.getMessage() == null ? "routing failed" : e.getMessage());
        }
    }

    private String ownerHttpAddress(String path) throws Exception {
        if (path == null || path.isEmpty()) {
            path = "/";
        }
        OptionalLong streamId = store.lookupStreamId(path);
        if (streamId.isEmpty()) {
            return null;
        }
        long id = streamId.getAsLong();
        return metadataNode.client().readIndex(() -> {
            List<StreamMetadata> streams =
                metadataNode.stateMachine().streamControlManager().getStreams(List.of(id));
            if (streams.isEmpty() || streams.get(0).state() != StreamState.OPENED) {
                return null;
            }
            int ownerId = streams.get(0).nodeId();
            if (ownerId == metadataNode.nodeId()) {
                return null;
            }
            String address = metadataNode.stateMachine().streamControlManager().getNodeAddress(ownerId);
            return address == null || address.isEmpty() ? null : address;
        }).get(10, TimeUnit.SECONDS);
    }
}
