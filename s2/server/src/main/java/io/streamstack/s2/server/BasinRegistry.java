package io.streamstack.s2.server;

import io.streamstack.s2.model.exception.S2Exception;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.streamstack.api.KVClient;
import io.streamstack.api.KeyValue;
import io.streamstack.api.KeyValue.Key;
import io.streamstack.api.KeyValue.Value;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class BasinRegistry {

    private static final String BASIN_PREFIX = "s2b!";
    private static final String STREAM_PREFIX = "s2s!";

    private static final Pattern BASIN_NAME = Pattern.compile("[a-z0-9]([a-z0-9-]{6,46})[a-z0-9]");

    private static final int OP_TIMEOUT_SEC = 15;
    private final KVClient kvClient;
    private final ObjectMapper mapper;

    public BasinRegistry(KVClient kvClient, ObjectMapper mapper) {
        this.kvClient = Objects.requireNonNull(kvClient, "kvClient");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    public static void validateBasinName(String basin) {
        if (Objects.isNull(basin) || !BASIN_NAME.matcher(basin).matches()) {
            throw S2Exception.invalid(
                "basin name must be 8-48 lowercase letters, numbers and hyphens, not starting or ending with a hyphen");
        }
    }

    public static void validateStreamName(String stream) {
        if (Objects.isNull(stream) || stream.isEmpty() || stream.getBytes(StandardCharsets.UTF_8).length > 512) {
            throw S2Exception.invalid("stream name must be between 1 and 512 bytes");
        }
    }

    public static String coreStreamName(String basin, String stream) {
        return "s2!" + basin + "!" + stream;
    }

    public ObjectNode getBasin(String basin) {
        return getDoc(BASIN_PREFIX + basin);
    }

    public ObjectNode requireBasin(String basin) {
        ObjectNode doc = getBasin(basin);
        if (Objects.isNull(doc)) {
            throw S2Exception.basinNotFound(basin);
        }
        return doc;
    }

    public ObjectNode getStream(String basin, String stream) {
        return getDoc(streamKey(basin, stream));
    }

    public ObjectNode requireStream(String basin, String stream) {
        ObjectNode doc = getStream(basin, stream);
        if (Objects.isNull(doc)) {
            throw S2Exception.streamNotFound(stream);
        }
        return doc;
    }

    public void putBasin(String basin, ObjectNode doc) {
        putDoc(BASIN_PREFIX + basin, doc);
    }

    public void putStream(String basin, String stream, ObjectNode doc) {
        putDoc(streamKey(basin, stream), doc);
    }

    public boolean deleteBasin(String basin) {
        return deleteDoc(BASIN_PREFIX + basin);
    }

    public boolean deleteStream(String basin, String stream) {
        return deleteDoc(streamKey(basin, stream));
    }

    public List<Entry> listBasins() {
        return list(BASIN_PREFIX, BASIN_PREFIX.length());
    }

    public List<Entry> listStreams(String basin) {
        String prefix = STREAM_PREFIX + basin + "!";
        return list(prefix, prefix.length());
    }

    public record Entry(String name, ObjectNode doc) {
    }

    private List<Entry> list(String prefix, int trim) {
        try {
            List<KeyValue> entries = kvClient.listKV(Key.of(prefix)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            List<Entry> out = new ArrayList<>(entries.size());
            for (KeyValue entry : entries) {
                out.add(new Entry(entry.key().get().substring(trim), parse(entry.value())));
            }
            out.sort(Comparator.comparing(Entry::name));
            return out;
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    private ObjectNode getDoc(String key) {
        try {
            Value value = kvClient.getKV(Key.of(key)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
            return Objects.isNull(value) || value.isNull() ? null : parse(value);
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    private void putDoc(String key, ObjectNode doc) {
        try {
            byte[] bytes = mapper.writeValueAsBytes(doc);
            kvClient.putKV(KeyValue.of(key, ByteBuffer.wrap(bytes))).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    private boolean deleteDoc(String key) {
        try {
            return Objects.nonNull(kvClient.delKV(Key.of(key)).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    private ObjectNode parse(Value value) {
        try {
            ByteBuffer buffer = value.get().duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            JsonNode node = mapper.readTree(bytes);
            return node instanceof ObjectNode obj ? obj : mapper.createObjectNode();
        } catch (Exception e) {
            throw wrap(e);
        }
    }

    private static String streamKey(String basin, String stream) {
        return STREAM_PREFIX + basin + "!" + stream;
    }

    private static S2Exception wrap(Exception e) {
        if (e instanceof S2Exception api) {
            return api;
        }
        return S2Exception.other(S2Exception.rootMessage(e));
    }
}
