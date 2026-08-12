package io.streamstack.s2.model;

import java.util.Objects;

import io.streamstack.s2.model.exception.S2Exception;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public enum Format {

    RAW,
    BASE64;

    public static Format parse(String value) {
        if (Objects.isNull(value) || value.isEmpty() || "raw".equalsIgnoreCase(value)) {
            return RAW;
        }

        if ("base64".equalsIgnoreCase(value)) {
            return BASE64;
        }

        throw S2Exception.badHeader("invalid s2-format: " + value);
    }

    public String encode(byte[] bytes) {
        return switch (this) {
            case RAW -> new String(bytes, StandardCharsets.UTF_8);
            case BASE64 -> Base64.getEncoder().encodeToString(bytes);
        };
    }

    public byte[] decode(String value) {
        if (Objects.isNull(value)) {
            return new byte[0];
        }

        return switch (this) {
            case RAW -> value.getBytes(StandardCharsets.UTF_8);
            case BASE64 -> Base64.getDecoder().decode(value);
        };
    }
}
