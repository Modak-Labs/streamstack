package io.streamstack.s2.server;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public final class ConfigJson {

    private static final Set<String> STORAGE_CLASSES = Set.of("standard", "express");

    private static final Set<String> TIMESTAMPING_MODES = Set.of("client-prefer", "client-require", "arrival");

    private static final Set<String> NESTED_STREAM_FIELDS = Set.of("timestamping", "delete_on_empty");

    private static final Set<String> NESTED_BASIN_FIELDS =
        Set.of("default_stream_config", "timestamping", "delete_on_empty");
    private static final long DEFAULT_RETENTION_AGE_SEC = 7L * 24 * 60 * 60;

    private ConfigJson() {
    }

    public static ObjectNode newDoc(ObjectMapper mapper, JsonNode config, String requestToken) {
        ObjectNode doc = mapper.createObjectNode();

        if (Objects.nonNull(config) && !config.isNull()) {
            doc.set("config", config.deepCopy());
        }

        doc.put("created_at", System.currentTimeMillis());

        if (Objects.nonNull(requestToken)) {
            doc.put("request_token", requestToken);
        }

        return doc;
    }

    public static ObjectNode reconfigureBasin(ObjectMapper mapper, JsonNode current, JsonNode patch) {
        ObjectNode updated = reconfigure(mapper, current, patch, NESTED_BASIN_FIELDS);

        validateBasinConfig(updated);

        return updated;
    }

    public static ObjectNode reconfigureStream(ObjectMapper mapper, JsonNode current, JsonNode patch) {
        ObjectNode updated = reconfigure(mapper, current, patch, NESTED_STREAM_FIELDS);

        validateStreamConfig(updated);

        return updated;
    }

    public static void validateBasinConfig(JsonNode config) {
        if (Objects.isNull(config) || config.isNull()) {
            return;
        }

        if (!config.isObject()) {
            throw S2Exception.invalid("basin config must be an object");
        }

        if (config.hasNonNull("stream_cipher")) {
            throw S2Exception.invalid("stream_cipher is not supported");
        }

        checkBoolean(config, "create_stream_on_append");
        checkBoolean(config, "create_stream_on_read");

        if (config.hasNonNull("default_stream_config")) {
            validateStreamConfig(config.get("default_stream_config"));
        }
    }

    public static void validateStreamConfig(JsonNode config) {
        if (Objects.isNull(config) || config.isNull()) {
            return;
        }

        if (!config.isObject()) {
            throw S2Exception.invalid("stream config must be an object");
        }

        if (config.hasNonNull("storage_class")
            && !STORAGE_CLASSES.contains(config.get("storage_class").asText())) {
            throw S2Exception.invalid("invalid storage_class");
        }

        if (config.hasNonNull("retention_policy")) {
            JsonNode policy = config.get("retention_policy");

            if (!policy.isObject() || (!policy.has("age") && !policy.has("infinite"))) {
                throw S2Exception.invalid("retention_policy must be {\"age\": seconds} or {\"infinite\": {}}");
            }

            if (policy.has("age") && policy.get("age").asLong() <= 0) {
                throw S2Exception.invalid("age must be greater than 0 seconds");
            }
        }

        if (config.hasNonNull("timestamping")) {
            JsonNode ts = config.get("timestamping");

            if (!ts.isObject()) {
                throw S2Exception.invalid("timestamping must be an object");
            }

            if (ts.hasNonNull("mode") && !TIMESTAMPING_MODES.contains(ts.get("mode").asText())) {
                throw S2Exception.invalid("invalid timestamping mode");
            }

            checkBoolean(ts, "uncapped");
        }

        if (config.hasNonNull("delete_on_empty")) {
            JsonNode doe = config.get("delete_on_empty");

            if (!doe.isObject() || (doe.has("min_age_secs") && doe.get("min_age_secs").asLong() < 0)) {
                throw S2Exception.invalid("invalid delete_on_empty");
            }
        }
    }

    public static ObjectNode resolveStreamConfig(ObjectMapper mapper, JsonNode streamConfig, JsonNode basinConfig) {
        JsonNode basinDefaults = Objects.nonNull(basinConfig) && basinConfig.hasNonNull("default_stream_config")
            ? basinConfig.get("default_stream_config")
            : null;
        ObjectNode resolved = mapper.createObjectNode();

        resolved.put("storage_class",
            firstText(streamConfig, basinDefaults, "storage_class", "express"));
        JsonNode retention = firstNode(streamConfig, basinDefaults, "retention_policy");

        if (Objects.nonNull(retention)) {
            resolved.set("retention_policy", retention.deepCopy());
        } else {
            resolved.putObject("retention_policy").put("age", DEFAULT_RETENTION_AGE_SEC);
        }

        JsonNode streamTs = Objects.isNull(streamConfig) ? null : streamConfig.get("timestamping");
        JsonNode basinTs = Objects.isNull(basinDefaults) ? null : basinDefaults.get("timestamping");
        ObjectNode ts = resolved.putObject("timestamping");

        ts.put("mode", firstText(streamTs, basinTs, "mode", "client-prefer"));
        ts.put("uncapped", firstBoolean(streamTs, basinTs, "uncapped", false));
        JsonNode streamDoe = Objects.isNull(streamConfig) ? null : streamConfig.get("delete_on_empty");
        JsonNode basinDoe = Objects.isNull(basinDefaults) ? null : basinDefaults.get("delete_on_empty");
        ObjectNode doe = resolved.putObject("delete_on_empty");

        doe.put("min_age_secs", firstLong(streamDoe, basinDoe, "min_age_secs", 0L));

        return resolved;
    }

    private static ObjectNode reconfigure(ObjectMapper mapper, JsonNode current, JsonNode patch, Set<String> nested) {
        ObjectNode target = Objects.nonNull(current) && current.isObject()
            ? ((ObjectNode) current).deepCopy()
            : mapper.createObjectNode();
        if (Objects.isNull(patch) || !patch.isObject()) {
            return target;
        }

        Iterator<Map.Entry<String, JsonNode>> fields = patch.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();

            if (field.getValue().isNull()) {
                target.remove(field.getKey());
            } else if (nested.contains(field.getKey()) && field.getValue().isObject()) {
                target.set(field.getKey(),
                    reconfigure(mapper, target.get(field.getKey()), field.getValue(), nested));
            } else {
                target.set(field.getKey(), field.getValue().deepCopy());
            }
        }

        return target;
    }

    private static void checkBoolean(JsonNode node, String field) {
        if (node.hasNonNull(field) && !node.get(field).isBoolean()) {
            throw S2Exception.invalid(field + " must be a boolean");
        }
    }

    private static String firstText(JsonNode primary, JsonNode fallback, String field, String defaultValue) {
        if (Objects.nonNull(primary) && primary.hasNonNull(field)) {
            return primary.get(field).asText();
        }

        if (Objects.nonNull(fallback) && fallback.hasNonNull(field)) {
            return fallback.get(field).asText();
        }

        return defaultValue;
    }

    private static boolean firstBoolean(JsonNode primary, JsonNode fallback, String field, boolean defaultValue) {
        if (Objects.nonNull(primary) && primary.hasNonNull(field)) {
            return primary.get(field).asBoolean();
        }

        if (Objects.nonNull(fallback) && fallback.hasNonNull(field)) {
            return fallback.get(field).asBoolean();
        }

        return defaultValue;
    }

    private static long firstLong(JsonNode primary, JsonNode fallback, String field, long defaultValue) {
        if (Objects.nonNull(primary) && primary.hasNonNull(field)) {
            return primary.get(field).asLong();
        }

        if (Objects.nonNull(fallback) && fallback.hasNonNull(field)) {
            return fallback.get(field).asLong();
        }

        return defaultValue;
    }

    private static JsonNode firstNode(JsonNode primary, JsonNode fallback, String field) {
        if (Objects.nonNull(primary) && primary.hasNonNull(field)) {
            return primary.get(field);
        }

        if (Objects.nonNull(fallback) && fallback.hasNonNull(field)) {
            return fallback.get(field);
        }

        return null;
    }
}
