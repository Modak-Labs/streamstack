package io.streamstack.s2.server;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.Javalin;
import io.streamstack.s2.model.Protocol;
import io.streamstack.s2.model.S2Json;
import io.streamstack.s2.model.exception.FencingTokenMismatchException;
import io.streamstack.s2.model.exception.S2Exception;
import io.streamstack.s2.model.exception.SeqNumMismatchException;
import io.streamstack.s2.model.response.ErrorResponse;
import io.streamstack.server.StreamStackNode;
import io.streamstack.server.model.config.ServerConfig;
import io.streamstack.server.service.StreamServiceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class S2Server implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(S2Server.class);

    private final ServerConfig config;
    private final StreamStackNode node;
    private final Javalin app;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public S2Server(ServerConfig config) throws Exception {
        this.config = Objects.requireNonNull(config, "config");
        this.node = new StreamStackNode(config);
        ObjectMapper mapper = new ObjectMapper();
        BasinRegistry registry = new BasinRegistry(node.kvClient(), mapper);
        StreamState state = new StreamState();
        StreamHandler streams = new StreamHandler(node.service(), registry, mapper, state);
        BasinHandler basins = new BasinHandler(registry, mapper, streams);
        RecordHandler records = new RecordHandler(
            node.service(), registry, streams, state, mapper,
            Duration.ofSeconds(config.sseMaxDurationSec()));
        OwnershipRouter router = new OwnershipRouter(node.service(), config.routingMode());
        this.app = Javalin.create(cfg -> {
            cfg.showJavalinBanner = false;
            cfg.useVirtualThreads = true;
        });
        app.get("/health", ctx -> {
            ctx.status(200);
            ctx.result("ok");
        });
        app.get("/v1/basins", basins::list);
        app.post("/v1/basins", basins::create);
        app.get("/v1/basins/{basin}", basins::getConfig);
        app.put("/v1/basins/{basin}", basins::ensure);
        app.patch("/v1/basins/{basin}", basins::reconfigure);
        app.delete("/v1/basins/{basin}", basins::delete);
        app.get("/v1/streams", streams::list);
        app.post("/v1/streams", streams::create);
        app.get("/v1/streams/<stream>/records/tail", router.route(records::checkTail));
        app.get("/v1/streams/<stream>/records", router.route(records::read));
        app.post("/v1/streams/<stream>/records", router.route(records::append));
        app.get("/v1/streams/<stream>", streams::getConfig);
        app.put("/v1/streams/<stream>", streams::ensure);
        app.patch("/v1/streams/<stream>", streams::reconfigure);
        app.delete("/v1/streams/<stream>", streams::delete);
        app.exception(SeqNumMismatchException.class, (e, ctx) ->
            write(ctx, 412, Map.of("seq_num_mismatch", e.actualSeqNum())));
        app.exception(FencingTokenMismatchException.class, (e, ctx) ->
            write(ctx, 412, Map.of("fencing_token_mismatch", e.actualToken())));
        app.exception(S2Exception.class, (e, ctx) ->
            write(ctx, e.status(), new ErrorResponse(e.code(), e.getMessage(), e.resource())));
        app.exception(StreamServiceException.class, (e, ctx) -> {
            switch (e.kind()) {
                case NOT_FOUND -> write(ctx, 404, new ErrorResponse("stream_not_found", "stream not found"));
                case BAD_REQUEST -> write(ctx, 422, new ErrorResponse("invalid", e.getMessage()));
                default -> write(ctx, 409, new ErrorResponse("transaction_conflict",
                    Objects.isNull(e.getMessage()) ? "conflict" : e.getMessage()));
            }
        });
        app.exception(Exception.class, (e, ctx) ->
            write(ctx, 500, new ErrorResponse("other", S2Exception.rootMessage(e))));
    }

    private static void write(io.javalin.http.Context ctx, int status, Object body) {
        ctx.status(status);
        ctx.header(Protocol.H_CONTENT_TYPE, Protocol.CT_JSON);
        ctx.result(S2Json.write(body, null));
    }

    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        node.start();
        app.start(config.httpHost(), config.httpPort());
        LOGGER.info("streamstack s2 server started nodeId={} http={}:{} raft={}:{} storage={} wal={}",
            config.nodeId(), config.httpHost(), config.httpPort(), config.raftHost(), config.raftPort(),
            config.storageUri(), config.resolveWalUri());
    }

    public String baseUrl() {
        return config.httpAddress();
    }

    public StreamStackNode node() {
        return node;
    }

    @Override
    public void close() {
        try {
            app.stop();
        } catch (Exception ignored) {
        }
        node.close();
        started.set(false);
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        S2Server server = new S2Server(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
    }
}
