package io.streamstack.ds.model;

public final class Protocol {

    private Protocol() {
    }

    public static final String Q_OFFSET = "offset";
    public static final String Q_LIVE = "live";
    public static final String Q_CURSOR = "cursor";
    public static final String LIVE_LONG_POLL = "long-poll";
    public static final String LIVE_SSE = "sse";
    public static final String H_STREAM_NEXT_OFFSET = "Stream-Next-Offset";
    public static final String H_STREAM_UP_TO_DATE = "Stream-Up-To-Date";
    public static final String H_STREAM_TTL = "Stream-TTL";
    public static final String H_STREAM_EXPIRES_AT = "Stream-Expires-At";
    public static final String H_STREAM_CLOSED = "Stream-Closed";
    public static final String H_STREAM_CURSOR = "Stream-Cursor";
    public static final String H_STREAM_SSE_DATA_ENCODING = "Stream-SSE-Data-Encoding";
    public static final String H_STREAM_SEQ = "Stream-Seq";
    public static final String H_PRODUCER_ID = "Producer-Id";
    public static final String H_PRODUCER_EPOCH = "Producer-Epoch";
    public static final String H_PRODUCER_SEQ = "Producer-Seq";
    public static final String H_PRODUCER_EXPECTED_SEQ = "Producer-Expected-Seq";
    public static final String H_PRODUCER_RECEIVED_SEQ = "Producer-Received-Seq";
    public static final String H_CONTENT_TYPE = "Content-Type";
    public static final String H_CACHE_CONTROL = "Cache-Control";
    public static final String H_LOCATION = "Location";
    public static final String H_ETAG = "ETag";
    public static final String H_IF_NONE_MATCH = "If-None-Match";
    public static final String CT_EVENT_STREAM = "text/event-stream";
    public static final String BOOL_TRUE = "true";
    public static final String CACHE_CATCH_UP = "public, max-age=60, stale-while-revalidate=300";
}
