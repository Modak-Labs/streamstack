package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.http.Context;
import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.request.ReadRequest;

final class Requests {

    private Requests() {
    }

    static String basin(Context ctx) {
        String basin = ctx.header(Protocol.H_BASIN);

        if (Objects.isNull(basin) || basin.isEmpty()) {
            throw S2Exception.badHeader("missing " + Protocol.H_BASIN + " header");
        }

        return basin;
    }

    static JsonNode parseBody(ObjectMapper mapper, Context ctx) {
        byte[] body = ctx.bodyAsBytes();

        if (Objects.isNull(body) || body.length == 0) {
            return mapper.createObjectNode();
        }

        try {
            return mapper.readTree(body);
        } catch (Exception e) {
            throw S2Exception.badJson(e.getMessage());
        }
    }

    static String requireText(JsonNode node, String field) {
        if (!node.hasNonNull(field) || !node.get(field).isTextual()) {
            throw S2Exception.badJson("`" + field + "` is required");
        }

        return node.get(field).asText();
    }

    static ReadRequest readRequest(Context ctx) {
        return new ReadRequest(
            queryLong(ctx, Protocol.Q_SEQ_NUM),
            queryLong(ctx, Protocol.Q_TIMESTAMP),
            queryLong(ctx, Protocol.Q_TAIL_OFFSET),
            "true".equalsIgnoreCase(ctx.queryParam(Protocol.Q_CLAMP)),
            queryLong(ctx, Protocol.Q_COUNT),
            queryLong(ctx, Protocol.Q_BYTES),
            queryLong(ctx, Protocol.Q_UNTIL),
            queryLong(ctx, Protocol.Q_WAIT));
    }

    static Long queryLong(Context ctx, String name) {
        String raw = ctx.queryParam(name);

        if (Objects.isNull(raw) || raw.isEmpty()) {
            return null;
        }

        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw S2Exception.badQuery("invalid " + name);
        }
    }

    static boolean acceptsEventStream(Context ctx) {
        String accept = ctx.header("Accept");
        return Objects.nonNull(accept) && accept.contains(Protocol.CT_EVENT_STREAM);
    }

    static void json(Context ctx, int status, Object body, Format format) {
        ctx.status(status);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
        ctx.result(S2Json.write(body, format));
    }

    static void json(Context ctx, int status, Object body) {
        json(ctx, status, body, Format.RAW);
    }

    static void jsonNode(ObjectMapper mapper, Context ctx, int status, JsonNode body) {
        try {
            ctx.status(status);
            ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
            ctx.result(mapper.writeValueAsBytes(body));
        } catch (Exception e) {
            throw new IllegalStateException("failed to serialize response", e);
        }
    }

    record ListQuery(String prefix, String startAfter, int limit) {
        static ListQuery of(Context ctx) {
            String prefix = ctx.queryParam(Protocol.Q_PREFIX);
            String startAfter = ctx.queryParam(Protocol.Q_START_AFTER);
            String limitRaw = ctx.queryParam(Protocol.Q_LIMIT);
            int limit = Protocol.LIST_LIMIT_MAX;

            if (Objects.nonNull(limitRaw) && !limitRaw.isEmpty()) {
                try {
                    limit = Math.min(Integer.parseInt(limitRaw), Protocol.LIST_LIMIT_MAX);
                } catch (NumberFormatException e) {
                    throw S2Exception.badQuery("invalid limit");
                }

                if (limit < 0) {
                    throw S2Exception.badQuery("invalid limit");
                }
            }

            return new ListQuery(Objects.isNull(prefix) ? "" : prefix, Objects.isNull(startAfter) ? "" : startAfter, limit);
        }

        boolean matches(String name) {
            return name.startsWith(prefix) && name.compareTo(startAfter) > 0;
        }
    }
}
