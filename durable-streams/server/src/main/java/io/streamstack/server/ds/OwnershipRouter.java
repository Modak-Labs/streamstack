package io.streamstack.server.ds;

import io.javalin.http.Context;
import io.streamstack.model.Protocol;
import io.streamstack.server.service.OwnershipService;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;
import io.streamstack.server.model.config.RoutingMode;

import java.util.Objects;

public final class OwnershipRouter {

    private final OwnershipService ownership;
    private final DurableStreamsHandler handler;
    private final RoutingMode mode;

    public OwnershipRouter(StreamService service, DurableStreamsHandler handler, RoutingMode mode) {
        this(service.ownership(), handler, mode);
    }

    public OwnershipRouter(OwnershipService ownership, DurableStreamsHandler handler, RoutingMode mode) {
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.mode = Objects.isNull(mode) ? RoutingMode.REDIRECT : mode;
    }

    public void handle(Context ctx) {
        if (mode == RoutingMode.LOCAL_ALWAYS
            || "PUT".equals(ctx.method().name())
            || "OPTIONS".equals(ctx.method().name())) {
            handler.handle(ctx);
            return;
        }
        try {
            String name = streamName(ctx);
            Owner owner = ownership.ownerOf(name);
            NodeMeta local = ownership.localNode();
            if (owner.local()
                || Objects.isNull(owner.ownerAdvertisedAddress())
                || owner.ownerAdvertisedAddress().equals(local.advertisedAddress())) {
                handler.handle(ctx);
                return;
            }
            String location = owner.ownerAdvertisedAddress();
            if (location.endsWith("/")) {
                location = location.substring(0, location.length() - 1);
            }
            location += ctx.path().startsWith("/") ? ctx.path() : "/" + ctx.path();
            if (Objects.nonNull(ctx.queryString()) && !ctx.queryString().isEmpty()) {
                location += "?" + ctx.queryString();
            }
            ctx.status(307);
            ctx.header(Protocol.H_LOCATION, location);
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
        } catch (Exception e) {
            ctx.status(503);
            ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
            ctx.header("X-Error", Objects.isNull(e.getMessage()) ? "routing failed" : e.getMessage());
        }
    }

    private static String streamName(Context ctx) {
        String path = ctx.path();
        return Objects.isNull(path) || path.isEmpty() ? "/" : path;
    }
}
