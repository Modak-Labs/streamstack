package io.streamstack.s2.server;

import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.streamstack.s2.model.Protocol;
import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;
import io.streamstack.server.model.config.RoutingMode;
import io.streamstack.server.service.OwnershipService;
import io.streamstack.server.service.StreamService;

import java.util.Objects;

public final class OwnershipRouter {

    private final OwnershipService ownership;
    private final RoutingMode mode;

    public OwnershipRouter(StreamService service, RoutingMode mode) {
        this.ownership = Objects.requireNonNull(service, "service").ownership();
        this.mode = Objects.isNull(mode) ? RoutingMode.REDIRECT : mode;
    }

    public Handler route(Handler local) {
        return ctx -> {
            if (mode == RoutingMode.LOCAL_ALWAYS) {
                local.handle(ctx);
                return;
            }
            String basin = ctx.header(Protocol.H_BASIN);
            String stream = ctx.pathParamMap().get("stream");
            if (Objects.isNull(basin) || Objects.isNull(stream)) {
                local.handle(ctx);
                return;
            }
            Owner owner = ownership.ownerOf(BasinRegistry.coreStreamName(basin, stream));
            NodeMeta localNode = ownership.localNode();
            if (owner.local()
                || Objects.isNull(owner.ownerAdvertisedAddress())
                || owner.ownerAdvertisedAddress().equals(localNode.advertisedAddress())) {
                local.handle(ctx);
                return;
            }
            redirect(ctx, owner.ownerAdvertisedAddress());
        };
    }

    private static void redirect(Context ctx, String address) {
        String location = address.endsWith("/") ? address.substring(0, address.length() - 1) : address;
        location += ctx.path().startsWith("/") ? ctx.path() : "/" + ctx.path();
        if (Objects.nonNull(ctx.queryString()) && !ctx.queryString().isEmpty()) {
            location += "?" + ctx.queryString();
        }
        ctx.status(307);
        ctx.header("Location", location);
        ctx.header(Protocol.H_CACHE_CONTROL, "no-store");
    }
}
