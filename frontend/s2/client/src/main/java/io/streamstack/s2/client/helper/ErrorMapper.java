package io.streamstack.s2.client.helper;

import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;

import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.exception.FencingTokenMismatchException;
import io.streamstack.s2.model.exception.S2Exception;
import io.streamstack.s2.model.exception.SeqNumMismatchException;
import io.streamstack.s2.model.response.ErrorResponse;

import java.nio.charset.StandardCharsets;

public final class ErrorMapper {

    private ErrorMapper() {
    }

    public static S2Exception map(int status, byte[] body) {
        if (status == 412) {
            return mapPrecondition(body);
        }

        String code = "other";
        String message = Objects.isNull(body) || body.length == 0
            ? "request failed with status " + status
            : new String(body, StandardCharsets.UTF_8);
        String resource = null;

        try {
            ErrorResponse error = S2Json.read(body, ErrorResponse.class, Format.RAW);

            if (Objects.nonNull(error.code())) {
                code = error.code();
            }

            if (Objects.nonNull(error.message())) {
                message = error.message();
            }

            resource = error.resource();
        } catch (Exception ignored) {
        }

        String name = Objects.isNull(resource) ? message : resource;

        return switch (code) {
            case "basin_not_found" -> S2Exception.basinNotFound(name);
            case "stream_not_found" -> S2Exception.streamNotFound(name);
            case "resource_already_exists" -> S2Exception.alreadyExists(name);
            case "invalid" -> S2Exception.invalid(message);
            case "bad_json" -> S2Exception.badJson(message);
            case "bad_query" -> S2Exception.badQuery(message);
            case "bad_header" -> S2Exception.badHeader(message);
            default -> new S2Exception(status, code, message);
        };
    }

    private static S2Exception mapPrecondition(byte[] body) {
        try {
            JsonNode node = S2Json.read(body, JsonNode.class, Format.RAW);

            if (node.has("seq_num_mismatch")) {
                return new SeqNumMismatchException(node.get("seq_num_mismatch").asLong());
            }

            if (node.has("fencing_token_mismatch")) {
                return new FencingTokenMismatchException(node.get("fencing_token_mismatch").asText(""));
            }
        } catch (Exception ignored) {
        }

        return new S2Exception(412, "append_condition_failed", "append condition failed");
    }
}
