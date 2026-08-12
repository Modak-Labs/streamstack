package io.streamstack.s2.model;

import java.util.List;

public final class Protocol {

    private Protocol() {
    }

    public static final String H_BASIN = "s2-basin";
    public static final String H_FORMAT = "s2-format";
    public static final String H_REQUEST_TOKEN = "s2-request-token";
    public static final String H_CONTENT_TYPE = "Content-Type";
    public static final String H_CACHE_CONTROL = "Cache-Control";
    public static final String H_LAST_EVENT_ID = "Last-Event-ID";
    public static final String Q_SEQ_NUM = "seq_num";
    public static final String Q_TIMESTAMP = "timestamp";
    public static final String Q_TAIL_OFFSET = "tail_offset";
    public static final String Q_CLAMP = "clamp";
    public static final String Q_COUNT = "count";
    public static final String Q_BYTES = "bytes";
    public static final String Q_UNTIL = "until";
    public static final String Q_WAIT = "wait";
    public static final String Q_PREFIX = "prefix";
    public static final String Q_START_AFTER = "start_after";
    public static final String Q_LIMIT = "limit";
    public static final String CT_JSON = "application/json";
    public static final String CT_EVENT_STREAM = "text/event-stream";
    public static final String COMMAND_FENCE = "fence";
    public static final String COMMAND_TRIM = "trim";
    public static final int RECORD_BATCH_MAX_COUNT = 1000;
    public static final int RECORD_BATCH_MAX_BYTES = 1024 * 1024;
    public static final int LIST_LIMIT_MAX = 1000;
    public static final int MAX_UNARY_READ_WAIT_SEC = 60;

    public static long meteredBytes(int headerCount, long headerBytes, long bodyBytes) {
        return 8L + 2L * headerCount + headerBytes + bodyBytes;
    }

    public static long meteredBytes(List<RecordHeader> headers, byte[] body) {
        long headerBytes = 0;
        for (RecordHeader header : headers) {
            headerBytes += header.name().length + header.value().length;
        }
        return meteredBytes(headers.size(), headerBytes, body.length);
    }
}
