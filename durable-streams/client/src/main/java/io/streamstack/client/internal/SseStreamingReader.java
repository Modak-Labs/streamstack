package io.streamstack.client.internal;

import io.streamstack.client.ParseErrorException;
import io.streamstack.client.model.Chunk;
import io.streamstack.model.Offset;
import io.streamstack.model.Protocol;
import io.streamstack.model.exception.DurableStreamException;
import io.streamstack.model.exception.StreamNotFoundException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SseStreamingReader implements AutoCloseable {
    private final HttpClient httpClient;
    private final HttpRequest request;
    private final BlockingQueue<ChunkOrError> chunkQueue = new LinkedBlockingQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile Thread readerThread;
    private volatile InputStream inputStream;
    private volatile Offset currentOffset;
    private volatile String currentCursor;
    private volatile boolean upToDate;
    private volatile String encoding;

    public SseStreamingReader(HttpClient httpClient, HttpRequest request, Offset initialOffset) {
        this.httpClient = httpClient;
        this.request = request;
        this.currentOffset = initialOffset;
    }

    public void start() throws DurableStreamException {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            HttpResponse<InputStream> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
            int status = response.statusCode();
            if (status == 404) {
                throw new StreamNotFoundException(request.uri().toString());
            }
            if (status != 200) {
                throw new DurableStreamException("SSE connection failed with status: " + status, status);
            }
            encoding = response.headers().firstValue(Protocol.H_STREAM_SSE_DATA_ENCODING).orElse(null);
            inputStream = response.body();
            readerThread = new Thread(this::readLoop, "sse-reader");
            readerThread.setDaemon(true);
            readerThread.start();
        } catch (IOException e) {
            throw new DurableStreamException("Failed to open SSE connection: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DurableStreamException("SSE connection interrupted", e);
        }
    }

    public Chunk poll(long timeoutMs) throws DurableStreamException {
        try {
            ChunkOrError result = chunkQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            if (result == null) {
                return null;
            }
            if (result.error != null) {
                throw result.error;
            }
            return result.chunk;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public Offset currentOffset() {
        return currentOffset;
    }

    public String currentCursor() {
        return currentCursor;
    }

    public boolean upToDate() {
        return upToDate;
    }

    public boolean closed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (readerThread != null) {
            readerThread.interrupt();
        }
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void readLoop() {
        SseParser parser = new SseParser(inputStream);
        List<String> pendingData = new ArrayList<>();
        try {
            while (!closed.get() && !Thread.currentThread().isInterrupted()) {
                SseParser.SseEvent event = parser.nextEvent();
                if (event == null) {
                    break;
                }
                if ("data".equals(event.event())) {
                    pendingData.add(event.data());
                } else if ("control".equals(event.event())) {
                    try {
                        Chunk chunk = createChunk(pendingData, event.data());
                        if (chunk != null) {
                            chunkQueue.offer(new ChunkOrError(chunk));
                            if (chunk.nextOffset() != null) {
                                currentOffset = chunk.nextOffset();
                            }
                            currentCursor = chunk.cursor().orElse(null);
                            upToDate = chunk.upToDate();
                        }
                    } catch (ParseErrorException e) {
                        chunkQueue.offer(new ChunkOrError(e));
                        break;
                    }
                    pendingData.clear();
                }
            }
        } catch (IOException e) {
            if (!closed.get()) {
                chunkQueue.offer(new ChunkOrError(
                    new DurableStreamException("SSE read error: " + e.getMessage(), e)));
            }
        } finally {
            closed.set(true);
        }
    }

    private Chunk createChunk(List<String> dataParts, String controlJson) {
        if (controlJson == null || controlJson.trim().isEmpty()) {
            throw new ParseErrorException("Empty control event data");
        }
        String trimmed = controlJson.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            throw new ParseErrorException("Malformed control event JSON: " + controlJson);
        }
        String nextOffset = extractJsonString(controlJson, "streamNextOffset");
        String cursor = extractJsonString(controlJson, "streamCursor");
        boolean isUpToDate = extractJsonBoolean(controlJson, "upToDate");
        byte[] dataBytes;
        if (dataParts.isEmpty()) {
            dataBytes = new byte[0];
        } else if ("base64".equals(encoding)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (String part : dataParts) {
                String cleaned = part.replace("\n", "").replace("\r", "");
                if (!cleaned.isEmpty()) {
                    try {
                        out.write(Base64.getDecoder().decode(cleaned));
                    } catch (IllegalArgumentException e) {
                        throw new ParseErrorException("Invalid base64 data in SSE event: " + e.getMessage());
                    } catch (IOException e) {
                        throw new ParseErrorException("Failed to concatenate decoded data: " + e.getMessage());
                    }
                }
            }
            dataBytes = out.toByteArray();
        } else {
            dataBytes = String.join("", dataParts).getBytes(StandardCharsets.UTF_8);
        }
        Map<String, String> headers = new HashMap<>();
        if (nextOffset != null) {
            headers.put(Protocol.H_STREAM_NEXT_OFFSET.toLowerCase(), nextOffset);
        }
        if (cursor != null) {
            headers.put(Protocol.H_STREAM_CURSOR.toLowerCase(), cursor);
        }
        return new Chunk(
            dataBytes,
            nextOffset != null ? Offset.of(nextOffset) : null,
            isUpToDate,
            cursor,
            200,
            headers);
    }

    private static String extractJsonString(String json, String key) {
        int keyStart = json.indexOf('"' + key + '"');
        if (keyStart < 0) {
            return null;
        }
        int colonPos = json.indexOf(':', keyStart);
        if (colonPos < 0) {
            return null;
        }
        int valueStart = json.indexOf('"', colonPos + 1);
        if (valueStart < 0) {
            return null;
        }
        int valueEnd = json.indexOf('"', valueStart + 1);
        if (valueEnd < 0) {
            return null;
        }
        return json.substring(valueStart + 1, valueEnd);
    }

    private static boolean extractJsonBoolean(String json, String key) {
        int keyStart = json.indexOf('"' + key + '"');
        if (keyStart < 0) {
            return false;
        }
        int colonPos = json.indexOf(':', keyStart);
        if (colonPos < 0) {
            return false;
        }
        return json.substring(colonPos + 1).trim().startsWith("true");
    }

    private static final class ChunkOrError {
        final Chunk chunk;
        final DurableStreamException error;

        ChunkOrError(Chunk chunk) {
            this.chunk = chunk;
            this.error = null;
        }

        ChunkOrError(DurableStreamException error) {
            this.chunk = null;
            this.error = error;
        }
    }
}
