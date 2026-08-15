package io.streamstack.server.ds;

import io.javalin.Javalin;
import io.javalin.util.LoomUtil;
import io.streamstack.server.StreamStackNode;
import io.streamstack.server.model.config.ServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DurableStreamsServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DurableStreamsServer.class);

    private static final long MAX_REQUEST_SIZE = 32L * 1024 * 1024;

    private final ServerConfig config;
    private final StreamStackNode node;
    private final Javalin app;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public DurableStreamsServer(ServerConfig config) throws Exception {
        this.config = Objects.requireNonNull(config, "config");
        this.node = new StreamStackNode(config);
        DurableStreamsHandler handler = new DurableStreamsHandler(
            node.service(),
            Duration.ofSeconds(config.longPollTimeoutSec()),
            Duration.ofSeconds(config.sseMaxDurationSec()),
            config.maxChunkSize());
        OwnershipRouter router = new OwnershipRouter(node.service(), handler, config.routingMode());

        this.app = Javalin.create(cfg -> {
            cfg.showJavalinBanner = false;
            cfg.useVirtualThreads = true;
            cfg.http.maxRequestSize = MAX_REQUEST_SIZE;
        });

        app.get("/*", router::handle);
        app.post("/*", router::handle);
        app.put("/*", router::handle);
        app.delete("/*", router::handle);
        app.head("/*", router::handle);
        app.options("/*", router::handle);
    }

    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        node.start();
        app.start(config.httpHost(), config.httpPort());
        boolean virtualThreads = LoomUtil.INSTANCE.getLoomAvailable();

        LOGGER.info(
            "streamstack durable-streams server started nodeId={} http={}:{} raft={}:{} storage={} wal={} httpThreads={}",
            config.nodeId(), config.httpHost(), config.httpPort(), config.raftHost(), config.raftPort(),
            config.storageUri(), config.resolveWalUri(),
            virtualThreads ? "virtual" : "platform");
        if (!virtualThreads) {
            LOGGER.info("JDK {} detected; HTTP virtual threads inactive "
                + "(Java 21+ recommended for long-poll/SSE concurrency)",
                Runtime.version().feature());
        }
    }

    public String baseUrl() {
        return config.httpAddress();
    }

    public StreamStackNode node() {
        return node;
    }

    @Override
    public void close() {
        node.drainBeforeShutdown();

        try {
            app.stop();
        } catch (Exception ignored) {
        }

        node.close();
        started.set(false);
    }

    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        DurableStreamsServer server = new DurableStreamsServer(config);

        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        server.start();
    }
}
