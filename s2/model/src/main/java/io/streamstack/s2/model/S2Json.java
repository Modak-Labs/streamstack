package io.streamstack.s2.model;

import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;

public final class S2Json {

    private static final String FORMAT_ATTRIBUTE = "s2.format";

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
        .serializationInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .addModule(new SimpleModule()
            .addSerializer(byte[].class, new BytesSerializer())
            .addDeserializer(byte[].class, new BytesDeserializer())
            .addSerializer(RecordHeader.class, new HeaderSerializer())
            .addDeserializer(RecordHeader.class, new HeaderDeserializer()))
        .build();

    private S2Json() {
    }

    public static byte[] write(Object value, Format format) {
        try {
            return MAPPER.writer(attributes(format)).writeValueAsBytes(value);
        } catch (IOException e) {
            throw new IllegalStateException("failed to encode " + value.getClass().getSimpleName(), e);
        }
    }

    public static <T> T read(byte[] body, Class<T> type, Format format) throws IOException {
        return MAPPER.readerFor(type).with(attributes(format)).readValue(body);
    }

    private static ContextAttributes attributes(Format format) {
        return ContextAttributes.getEmpty()
            .withSharedAttribute(FORMAT_ATTRIBUTE, Objects.isNull(format) ? Format.RAW : format);
    }

    private static Format format(Object attribute) {
        return attribute instanceof Format format ? format : Format.RAW;
    }

    private static final class BytesSerializer extends JsonSerializer<byte[]> {
        @Override
        public void serialize(byte[] value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeString(format(provider.getAttribute(FORMAT_ATTRIBUTE)).encode(value));
        }
        @Override
        public boolean isEmpty(SerializerProvider provider, byte[] value) {
            return Objects.isNull(value) || value.length == 0;
        }
    }

    private static final class BytesDeserializer extends JsonDeserializer<byte[]> {
        @Override
        public byte[] deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return format(context.getAttribute(FORMAT_ATTRIBUTE)).decode(parser.getValueAsString());
        }
    }

    private static final class HeaderSerializer extends JsonSerializer<RecordHeader> {
        @Override
        public void serialize(RecordHeader value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            Format format = format(provider.getAttribute(FORMAT_ATTRIBUTE));
            gen.writeStartArray();
            gen.writeString(format.encode(value.name()));
            gen.writeString(format.encode(value.value()));
            gen.writeEndArray();
        }
    }

    private static final class HeaderDeserializer extends JsonDeserializer<RecordHeader> {
        @Override
        public RecordHeader deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (parser.currentToken() != JsonToken.START_ARRAY) {
                throw new IOException("each header must be a [name, value] pair");
            }
            Format format = format(context.getAttribute(FORMAT_ATTRIBUTE));
            String name = parser.nextTextValue();
            String value = parser.nextTextValue();
            if (Objects.isNull(name) || Objects.isNull(value) || parser.nextToken() != JsonToken.END_ARRAY) {
                throw new IOException("each header must be a [name, value] pair");
            }
            return new RecordHeader(format.decode(name), format.decode(value));
        }
    }
}
