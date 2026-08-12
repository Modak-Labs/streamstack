package io.streamstack.s2.client;

import java.util.Objects;

import io.streamstack.s2.client.internal.SseParser;
import io.streamstack.s2.model.Format;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.exception.S2Exception;
import io.streamstack.s2.model.response.ReadResponse;
import io.streamstack.s2.model.response.SequencedRecord;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public final class ReadSession implements Iterable<SequencedRecord>, AutoCloseable {

    private final SseParser parser;
    private final Format format;

    private final Deque<SequencedRecord> buffer = new ArrayDeque<>();

    private boolean done;
    private String lastEventId;

    ReadSession(InputStream stream, Format format) {
        this.parser = new SseParser(stream);
        this.format = format;
    }

    public String lastEventId() {
        return lastEventId;
    }

    public Optional<ReadResponse> nextBatch() {
        while (!done) {
            SseParser.Event event;

            try {
                Optional<SseParser.Event> next = parser.next();

                if (next.isEmpty()) {
                    done = true;
                    return Optional.empty();
                }

                event = next.get();
            } catch (Exception e) {
                throw S2Exception.unavailable(e.getMessage());
            }

            if (event.done()) {
                done = true;
                return Optional.empty();
            }

            if (Objects.nonNull(event.id())) {
                lastEventId = event.id();
            }

            if ("batch".equals(event.event()) || (Objects.isNull(event.event()) && Objects.nonNull(event.data()))) {
                try {
                    ReadResponse batch = S2Json.read(
                        event.data().getBytes(StandardCharsets.UTF_8), ReadResponse.class, format);
                    return Optional.of(batch);
                } catch (Exception e) {
                    throw S2Exception.badJson(e.getMessage());
                }
            }
        }

        return Optional.empty();
    }

    @Override
    public Iterator<SequencedRecord> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                fill();
                return !buffer.isEmpty();
            }

            @Override
            public SequencedRecord next() {
                fill();

                if (buffer.isEmpty()) {
                    throw new NoSuchElementException();
                }

                return buffer.removeFirst();
            }

            private void fill() {
                while (buffer.isEmpty() && !done) {
                    Optional<ReadResponse> batch = nextBatch();

                    batch.ifPresent(readResponse -> buffer.addAll(readResponse.records()));
                }
            }
        };
    }

    @Override
    public void close() {
        try {
            parser.close();
        } catch (Exception ignored) {
        }
    }
}
