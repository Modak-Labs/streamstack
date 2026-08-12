package io.streamstack.s2.model.request;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import io.streamstack.s2.model.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record AppendRecord(Long timestamp, List<RecordHeader> headers, byte[] body) {

    public AppendRecord {
        headers = Objects.isNull(headers) ? List.of() : List.copyOf(headers);
        body = Objects.isNull(body) ? new byte[0] : body;
    }

    @JsonIgnore
    public boolean isCommand() {
        return headers.size() == 1 && headers.get(0).name().length == 0;
    }

    @JsonIgnore
    public String commandName() {
        return isCommand() ? new String(headers.get(0).value(), StandardCharsets.UTF_8) : null;
    }
}
