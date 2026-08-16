package io.streamstack.model;

public final class Protocol {

    private Protocol() {
    }

    public static final String Q_SEQ = "seq";
    public static final String Q_COUNT = "count";
    public static final String Q_BYTES = "bytes";
    public static final String Q_FORMAT = "format";
    public static final String Q_LIVE = "live";
    public static final String Q_CURSOR = "cursor";
    public static final String Q_PREFIX = "prefix";
    public static final String Q_START_AFTER = "start_after";
    public static final String Q_LIMIT = "limit";
    public static final String SEQ_NOW = "now";
    public static final String FORMAT_JSON = "json";
    public static final String FORMAT_BINARY = "binary";
    public static final String FORMAT_RAW = "raw";
    public static final String LIVE_LONG_POLL = "long-poll";
    public static final String LIVE_SSE = "sse";
    public static final String H_START_SEQ = "SS-Start-Seq";
    public static final String H_NEXT_SEQ = "SS-Next-Seq";
    public static final String H_TIMESTAMP = "SS-Timestamp";
    public static final String H_MATCH_SEQ = "SS-Match-Seq";
    public static final String H_TRIM_SEQ = "SS-Trim-Seq";
    public static final String H_TTL = "SS-TTL";
    public static final String H_EXPIRES_AT = "SS-Expires-At";
    public static final String H_CLOSED = "SS-Closed";
    public static final String H_UP_TO_DATE = "SS-Up-To-Date";
    public static final String H_CURSOR = "SS-Cursor";
    public static final String H_PRODUCER_ID = "SS-Producer-Id";
    public static final String H_PRODUCER_EPOCH = "SS-Producer-Epoch";
    public static final String H_PRODUCER_SEQ = "SS-Producer-Seq";
    public static final String H_EXPECTED_SEQ = "SS-Expected-Seq";
    public static final String H_RECEIVED_SEQ = "SS-Received-Seq";
    public static final String H_CONTENT_TYPE = "Content-Type";
    public static final String H_CACHE_CONTROL = "Cache-Control";
    public static final String H_LOCATION = "Location";
    public static final String H_ETAG = "ETag";
    public static final String H_IF_NONE_MATCH = "If-None-Match";
    public static final String CT_BATCH_JSON = "application/vnd.streamstack.batch+json";
    public static final String CT_BATCH_BINARY = "application/vnd.streamstack.batch";
    public static final String CT_JSON = "application/json";
    public static final String CT_EVENT_STREAM = "text/event-stream";
    public static final String CT_CORE = "application/x-streamstack";
    public static final String CT_CORE_PARAM = "ct";
    public static final String DEFAULT_CT = "application/octet-stream";
    public static final String BOOL_TRUE = "true";
    public static final String CACHE_CATCH_UP = "public, max-age=60, stale-while-revalidate=300";
}
