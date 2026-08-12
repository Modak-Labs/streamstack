package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.javalin.http.Context;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.response.ListStreamsResponse;
import io.streamstack.s2.model.response.StreamResponse;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.service.StreamService;
import io.streamstack.server.service.StreamServiceException;

import java.util.ArrayList;
import java.util.List;

public final class StreamHandler {

    static final String CORE_CONTENT_TYPE = "application/x-s2";

    private final StreamService service;
    private final BasinRegistry registry;
    private final ObjectMapper mapper;
    private final StreamState state;

    public StreamHandler(StreamService service, BasinRegistry registry, ObjectMapper mapper, StreamState state) {
        this.service = service;
        this.registry = registry;
        this.mapper = mapper;
        this.state = state;
    }

    public void list(Context ctx) {
        String basin = Requests.basin(ctx);
        registry.requireBasin(basin);
        Requests.ListQuery query = Requests.ListQuery.of(ctx);
        List<StreamResponse> streams = new ArrayList<>();
        boolean hasMore = false;
        for (BasinRegistry.Entry entry : registry.listStreams(basin)) {
            if (!query.matches(entry.name())) {
                continue;
            }
            if (streams.size() >= query.limit()) {
                hasMore = true;
                break;
            }
            streams.add(ProtocolConverter.toStreamResponse(entry.name(), entry.doc()));
        }
        Requests.json(ctx, 200, new ListStreamsResponse(streams, hasMore));
    }

    public void create(Context ctx) throws Exception {
        String basin = Requests.basin(ctx);
        registry.requireBasin(basin);
        JsonNode body = Requests.parseBody(mapper, ctx);
        String stream = Requests.requireText(body, "stream");
        BasinRegistry.validateStreamName(stream);
        JsonNode config = body.get("config");
        ConfigJson.validateStreamConfig(config);
        String requestToken = ctx.header(Protocol.H_REQUEST_TOKEN);
        ObjectNode existing = registry.getStream(basin, stream);
        if (Objects.nonNull(existing)) {
            if (Objects.nonNull(requestToken) && requestToken.equals(existing.path("request_token").asText(null))) {
                Requests.json(ctx, 200, ProtocolConverter.toStreamResponse(stream, existing));
                return;
            }
            throw S2Exception.alreadyExists("stream " + stream);
        }
        ObjectNode doc = provision(basin, stream, config, requestToken);
        Requests.json(ctx, 201, ProtocolConverter.toStreamResponse(stream, doc));
    }

    public void ensure(Context ctx) throws Exception {
        String basin = Requests.basin(ctx);
        registry.requireBasin(basin);
        String stream = ctx.pathParam("stream");
        BasinRegistry.validateStreamName(stream);
        JsonNode body = Requests.parseBody(mapper, ctx);
        JsonNode config = body.get("config");
        ConfigJson.validateStreamConfig(config);
        ObjectNode existing = registry.getStream(basin, stream);
        if (Objects.isNull(existing)) {
            ObjectNode doc = provision(basin, stream, config, ctx.header(Protocol.H_REQUEST_TOKEN));
            Requests.json(ctx, 201, ProtocolConverter.toStreamResponse(stream, doc));
            return;
        }
        if (Objects.nonNull(config) && !config.isNull()) {
            existing.set("config", config.deepCopy());
            registry.putStream(basin, stream, existing);
        }
        Requests.json(ctx, 200, ProtocolConverter.toStreamResponse(stream, existing));
    }

    public void getConfig(Context ctx) {
        String basin = Requests.basin(ctx);
        ObjectNode basinDoc = registry.requireBasin(basin);
        ObjectNode doc = registry.requireStream(basin, ctx.pathParam("stream"));
        Requests.jsonNode(mapper, ctx, 200,
            ConfigJson.resolveStreamConfig(mapper, doc.get("config"), basinDoc.get("config")));
    }

    public void reconfigure(Context ctx) {
        String basin = Requests.basin(ctx);
        ObjectNode basinDoc = registry.requireBasin(basin);
        String stream = ctx.pathParam("stream");
        ObjectNode doc = registry.requireStream(basin, stream);
        ObjectNode updated = ConfigJson.reconfigureStream(mapper, doc.get("config"), Requests.parseBody(mapper, ctx));
        doc.set("config", updated);
        registry.putStream(basin, stream, doc);
        Requests.jsonNode(mapper, ctx, 200,
            ConfigJson.resolveStreamConfig(mapper, updated, basinDoc.get("config")));
    }

    public void delete(Context ctx) throws Exception {
        String basin = Requests.basin(ctx);
        registry.requireBasin(basin);
        String stream = ctx.pathParam("stream");
        if (Objects.isNull(registry.getStream(basin, stream))) {
            throw S2Exception.streamNotFound(stream);
        }
        remove(basin, stream);
        ctx.status(204);
    }

    ObjectNode provision(String basin, String stream, JsonNode config, String requestToken) throws Exception {
        service.lifecycle().create(new CreateCommand(
            BasinRegistry.coreStreamName(basin, stream), CORE_CONTENT_TYPE, null, null, false, new byte[0]));
        ObjectNode doc = ConfigJson.newDoc(mapper, config, requestToken);
        doc.put("fencing_token", "");
        doc.put("trim_point", 0);
        registry.putStream(basin, stream, doc);
        return doc;
    }

    void remove(String basin, String stream) throws Exception {
        String coreName = BasinRegistry.coreStreamName(basin, stream);
        try {
            service.lifecycle().delete(coreName);
        } catch (StreamServiceException e) {
            if (e.kind() != StreamServiceException.Kind.NOT_FOUND) {
                throw e;
            }
        }
        registry.deleteStream(basin, stream);
        state.invalidate(coreName);
    }

    StreamContext resolve(Context ctx, boolean forAppend) throws Exception {
        String basin = Requests.basin(ctx);
        ObjectNode basinDoc = registry.requireBasin(basin);
        String stream = ctx.pathParam("stream");
        BasinRegistry.validateStreamName(stream);
        ObjectNode streamDoc = registry.getStream(basin, stream);
        if (Objects.isNull(streamDoc)) {
            JsonNode config = basinDoc.get("config");
            boolean autoCreate = Objects.nonNull(config) && config.path(
                forAppend ? "create_stream_on_append" : "create_stream_on_read").asBoolean(false);
            if (!autoCreate) {
                throw S2Exception.streamNotFound(stream);
            }
            streamDoc = provision(basin, stream, null, null);
        }
        return new StreamContext(basin, stream, basinDoc, streamDoc);
    }
}
