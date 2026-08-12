package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.http.Context;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.response.ListBasinsResponse;
import io.streamstack.s2.model.response.BasinResponse;

import java.util.ArrayList;
import java.util.List;

public final class BasinHandler {

    private final BasinRegistry registry;
    private final ObjectMapper mapper;
    private final StreamHandler streams;

    public BasinHandler(BasinRegistry registry, ObjectMapper mapper, StreamHandler streams) {
        this.registry = registry;
        this.mapper = mapper;
        this.streams = streams;
    }

    public void list(Context ctx) {
        Requests.ListQuery query = Requests.ListQuery.of(ctx);
        List<BasinResponse> basins = new ArrayList<>();
        boolean hasMore = false;

        for (BasinRegistry.Entry entry : registry.listBasins()) {
            if (!query.matches(entry.name())) {
                continue;
            }

            if (basins.size() >= query.limit()) {
                hasMore = true;
                break;
            }

            basins.add(ProtocolConverter.toBasinResponse(entry.name(), entry.doc()));
        }

        Requests.json(ctx, 200, new ListBasinsResponse(basins, hasMore));
    }

    public void create(Context ctx) {
        JsonNode body = Requests.parseBody(mapper, ctx);
        String basin = Requests.requireText(body, "basin");

        BasinRegistry.validateBasinName(basin);
        JsonNode config = body.get("config");

        ConfigJson.validateBasinConfig(config);
        String requestToken = ctx.header(Protocol.H_REQUEST_TOKEN);
        ObjectNode existing = registry.getBasin(basin);

        if (Objects.nonNull(existing)) {
            if (Objects.nonNull(requestToken) && requestToken.equals(existing.path("request_token").asText(null))) {
                Requests.json(ctx, 200, ProtocolConverter.toBasinResponse(basin, existing));
                return;
            }

            throw S2Exception.alreadyExists("basin " + basin);
        }

        ObjectNode doc = basinDoc(config, requestToken);

        registry.putBasin(basin, doc);
        Requests.json(ctx, 201, ProtocolConverter.toBasinResponse(basin, doc));
    }

    public void ensure(Context ctx) {
        String basin = ctx.pathParam("basin");

        BasinRegistry.validateBasinName(basin);
        JsonNode body = Requests.parseBody(mapper, ctx);
        JsonNode config = body.get("config");

        ConfigJson.validateBasinConfig(config);
        ObjectNode existing = registry.getBasin(basin);

        if (Objects.isNull(existing)) {
            ObjectNode doc = basinDoc(config, ctx.header(Protocol.H_REQUEST_TOKEN));

            registry.putBasin(basin, doc);
            Requests.json(ctx, 201, ProtocolConverter.toBasinResponse(basin, doc));

            return;
        }

        if (Objects.nonNull(config) && !config.isNull()) {
            existing.set("config", config.deepCopy());
            registry.putBasin(basin, existing);
        }

        Requests.json(ctx, 200, ProtocolConverter.toBasinResponse(basin, existing));
    }

    public void getConfig(Context ctx) {
        ObjectNode doc = registry.requireBasin(ctx.pathParam("basin"));
        JsonNode config = doc.get("config");

        Requests.jsonNode(mapper, ctx, 200,
            Objects.nonNull(config) && config.isObject() ? config : mapper.createObjectNode());
    }

    public void reconfigure(Context ctx) {
        String basin = ctx.pathParam("basin");
        ObjectNode doc = registry.requireBasin(basin);
        ObjectNode updated = ConfigJson.reconfigureBasin(mapper, doc.get("config"), Requests.parseBody(mapper, ctx));

        doc.set("config", updated);
        registry.putBasin(basin, doc);
        Requests.jsonNode(mapper, ctx, 200, updated);
    }

    public void delete(Context ctx) throws Exception {
        String basin = ctx.pathParam("basin");

        if (Objects.isNull(registry.getBasin(basin))) {
            throw S2Exception.basinNotFound(basin);
        }

        for (BasinRegistry.Entry entry : registry.listStreams(basin)) {
            streams.remove(basin, entry.name());
        }

        registry.deleteBasin(basin);
        ctx.status(204);
    }

    private ObjectNode basinDoc(JsonNode config, String requestToken) {
        return ConfigJson.newDoc(mapper, config, requestToken);
    }
}
