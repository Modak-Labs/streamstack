package io.streamstack.client.internal;

import java.util.Objects;

import io.streamstack.model.Protocol;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.exception.OffsetGoneException;
import io.streamstack.model.exception.SequenceConflictException;
import io.streamstack.model.exception.StaleEpochException;
import io.streamstack.model.exception.StreamClosedException;
import io.streamstack.model.exception.StreamExistsException;
import io.streamstack.model.exception.StreamNotFoundException;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public final class ErrorMapper {

    private ErrorMapper() {
    }

    public static DurableStreamException map(String url, HttpResponse<?> response) {
        int status = response.statusCode();
        String body = bodyMessage(response);

        return switch (status) {
            case 404 -> new StreamNotFoundException(url, Objects.isNull(body) ? "stream not found: " + url : body);
            case 403 -> new StaleEpochException(headerLong(response, Protocol.H_PRODUCER_EPOCH),
                Objects.isNull(body) ? "Stale producer epoch" : body);
            case 410 -> new OffsetGoneException(url, null, Objects.isNull(body) ? "offset gone" : body);
            case 409 -> mapConflict(url, response, body);
            default -> new DurableStreamException(Objects.isNull(body) ? "request failed: " + status : body, status);
        };
    }

    public static DurableStreamException mapCreate(String url, HttpResponse<?> response) {
        if (response.statusCode() == 409) {
            return new StreamExistsException(url);
        }

        return map(url, response);
    }

    private static DurableStreamException mapConflict(String url, HttpResponse<?> response, String body) {
        if (Protocol.BOOL_TRUE.equalsIgnoreCase(response.headers().firstValue(Protocol.H_STREAM_CLOSED).orElse(null))) {
            return new StreamClosedException(url, Objects.isNull(body) ? "stream closed: " + url : body);
        }

        Long expected = headerLong(response, Protocol.H_PRODUCER_EXPECTED_SEQ);
        Long received = headerLong(response, Protocol.H_PRODUCER_RECEIVED_SEQ);

        if (Objects.nonNull(expected) || Objects.nonNull(received)) {
            return new SequenceConflictException(expected, received, Objects.isNull(body) ? "Producer sequence gap" : body);
        }

        return new SequenceConflictException(null, null, Objects.isNull(body) ? "conflict" : body);
    }

    private static Long headerLong(HttpResponse<?> response, String name) {
        return response.headers().firstValue(name).map(raw -> {
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException e) {
                return null;
            }
        }).orElse(null);
    }

    private static String bodyMessage(HttpResponse<?> response) {
        Object body = response.body();

        if (body instanceof byte[] bytes && bytes.length > 0) {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        if (body instanceof String s && !s.isEmpty()) {
            return s;
        }

        return null;
    }
}
