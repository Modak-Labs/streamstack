package io.streamstack.client.internal;

import io.streamstack.model.Protocol;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.exception.OffsetGoneException;
import io.streamstack.model.exception.SequenceConflictException;
import io.streamstack.model.exception.StaleEpochException;
import io.streamstack.model.exception.StreamClosedException;
import io.streamstack.model.exception.StreamExistsException;
import io.streamstack.model.exception.StreamNotFoundException;

import java.net.http.HttpResponse;

public final class ErrorMapper {
    private ErrorMapper() {
    }

    public static DurableStreamException map(String url, HttpResponse<?> response) {
        int status = response.statusCode();
        String body = bodyMessage(response);
        return switch (status) {
            case 404 -> new StreamNotFoundException(url, body == null ? "stream not found: " + url : body);
            case 403 -> new StaleEpochException(headerLong(response, Protocol.H_PRODUCER_EPOCH),
                body == null ? "Stale producer epoch" : body);
            case 410 -> new OffsetGoneException(url, null, body == null ? "offset gone" : body);
            case 409 -> mapConflict(url, response, body);
            default -> new DurableStreamException(body == null ? "request failed: " + status : body, status);
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
            return new StreamClosedException(url, body == null ? "stream closed: " + url : body);
        }
        Long expected = headerLong(response, Protocol.H_PRODUCER_EXPECTED_SEQ);
        Long received = headerLong(response, Protocol.H_PRODUCER_RECEIVED_SEQ);
        if (expected != null || received != null) {
            return new SequenceConflictException(expected, received, body == null ? "Producer sequence gap" : body);
        }
        return new SequenceConflictException(null, null, body == null ? "conflict" : body);
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
            return new String(bytes);
        }
        if (body instanceof String s && !s.isEmpty()) {
            return s;
        }
        return null;
    }
}
