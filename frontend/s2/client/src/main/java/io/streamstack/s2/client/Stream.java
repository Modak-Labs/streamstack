package io.streamstack.s2.client;

import io.streamstack.s2.client.helper.HttpTransport;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.request.AppendRequest;
import io.streamstack.s2.model.request.ReadRequest;
import io.streamstack.s2.model.response.AppendResponse;
import io.streamstack.s2.model.response.ReadResponse;
import io.streamstack.s2.model.response.TailResponse;

import java.util.Objects;

public final class Stream {

    private final HttpTransport transport;
    private final String basin;
    private final String name;

    Stream(HttpTransport transport, String basin, String name) {
        this.transport = transport;
        this.basin = Objects.requireNonNull(basin, "basin");
        this.name = Objects.requireNonNull(name, "name");
    }

    public String name() {
        return name;
    }

    public String basin() {
        return basin;
    }

    public TailResponse checkTail() {
        return transport.execute(
            transport.withBasin(transport.request(recordsPath() + "/tail"), basin).GET(),
            TailResponse.class,
            200);
    }

    public AppendResponse append(AppendRequest request) {
        return transport.execute(
            transport.withBasin(transport.request(recordsPath()), basin).POST(transport.jsonBody(request)),
            AppendResponse.class,
            200);
    }

    public ReadResponse read(ReadRequest request) {
        return transport.execute(
            transport.withBasin(transport.request(recordsPath() + query(request)), basin).GET(),
            ReadResponse.class,
            200);
    }

    public ReadSession readSession(ReadRequest request) {
        var builder = transport.withBasin(transport.request(recordsPath() + query(request)), basin)
            .header("Accept", Protocol.CT_EVENT_STREAM)
            .GET();
        return new ReadSession(transport.executeStream(builder), transport.format());
    }

    public AppendSession appendSession() {
        return new AppendSession(this);
    }

    public Producer producer() {
        return new Producer(appendSession());
    }

    private String recordsPath() {
        return "/v1/streams/" + HttpTransport.encodePath(name) + "/records";
    }

    private static String query(ReadRequest request) {
        if (Objects.isNull(request)) {
            return "";
        }

        StringBuilder query = new StringBuilder();

        HttpTransport.appendParam(query, Protocol.Q_SEQ_NUM, request.seqNum());
        HttpTransport.appendParam(query, Protocol.Q_TIMESTAMP, request.timestamp());
        HttpTransport.appendParam(query, Protocol.Q_TAIL_OFFSET, request.tailOffset());

        if (request.clamp()) {
            HttpTransport.appendParam(query, Protocol.Q_CLAMP, "true");
        }

        HttpTransport.appendParam(query, Protocol.Q_COUNT, request.count());
        HttpTransport.appendParam(query, Protocol.Q_BYTES, request.bytes());
        HttpTransport.appendParam(query, Protocol.Q_UNTIL, request.until());
        HttpTransport.appendParam(query, Protocol.Q_WAIT, request.waitSeconds());

        return query.isEmpty() ? "" : "?" + query;
    }
}
