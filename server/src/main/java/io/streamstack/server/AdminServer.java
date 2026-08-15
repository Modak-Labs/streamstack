package io.streamstack.server;

import com.alipay.sofa.jraft.entity.PeerId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.streamstack.metadata.raft.MetadataHealth;
import io.streamstack.metadata.raft.MetadataNode;
import io.streamstack.metadata.raft.SnapshotArchive;
import io.streamstack.server.model.Owner;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.config.ServerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AdminServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminServer.class);

    private static final long PEER_CHANGE_TIMEOUT_SEC = 30;

    private final StreamStackNode node;
    private final ServerConfig config;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Javalin app;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public AdminServer(StreamStackNode node) {
        this.node = Objects.requireNonNull(node, "node");
        this.config = node.config();

        this.app = Javalin.create(cfg -> {
            cfg.showJavalinBanner = false;
            cfg.useVirtualThreads = true;

            if (Objects.nonNull(AdminServer.class.getResource("/admin/dist/index.html"))) {
                cfg.staticFiles.add(staticFiles -> {
                    staticFiles.directory = "/admin/dist";
                    staticFiles.location = Location.CLASSPATH;
                });
            }
        });

        app.get("/health", this::health);
        app.get("/ready", this::ready);
        app.get("/admin/cluster", this::cluster);
        app.get("/admin/nodes", this::nodes);
        app.get("/admin/streams/<name>", this::stream);
        app.post("/admin/peers", this::addPeer);
        app.delete("/admin/peers/{peer}", this::removePeer);
        app.post("/admin/transfer-leader", this::transferLeader);
        app.post("/admin/snapshot", this::triggerSnapshot);
        app.get("/admin/snapshots", this::snapshots);

        app.exception(Exception.class, (e, ctx) -> {
            LOGGER.warn("admin request failed: {} {}", ctx.method(), ctx.path(), e);
            error(ctx, 500, e.getMessage());
        });
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        app.start(config.httpHost(), config.adminPort());
        LOGGER.info("streamstack admin server started nodeId={} admin={}:{}",
            config.nodeId(), config.httpHost(), config.adminPort());
    }

    private void health(Context ctx) {
        ctx.status(200);
        ctx.result("ok");
    }

    private void ready(Context ctx) {
        MetadataNode metadata = node.metadataNode();
        boolean leaderKnown = Objects.nonNull(metadata.leaderId());
        boolean registered = metadata.isRegistered();
        boolean ready = node.isReady() && leaderKnown && registered;
        Map<String, Object> body = new LinkedHashMap<>();

        body.put("ready", ready);
        body.put("started", node.isReady());
        body.put("leaderKnown", leaderKnown);
        body.put("registered", registered);
        json(ctx, ready ? 200 : 503, body);
    }

    private void cluster(Context ctx) {
        MetadataNode metadata = node.metadataNode();
        MetadataHealth health = metadata.health();
        Map<String, Object> raft = new LinkedHashMap<>();

        raft.put("leader", health.leaderId());
        raft.put("isLeader", health.isLeader());
        raft.put("appliedIndex", health.appliedIndex());
        raft.put("applySuccessCount", health.applySuccessCount());
        raft.put("applyFailCount", health.applyFailCount());
        raft.put("peers", peers(metadata));

        Map<String, Object> body = new LinkedHashMap<>();

        body.put("clusterId", config.clusterId());
        body.put("nodeId", config.nodeId());
        body.put("nodeEpoch", metadata.nodeEpoch());
        body.put("advertisedAddress", config.httpAddress());
        body.put("registered", metadata.isRegistered());
        body.put("raft", raft);
        body.put("streamCount", health.streamCount());
        body.put("objectCount", health.objectCount());
        body.put("destroyedObjectBacklog", health.destroyedBacklog());
        json(ctx, 200, body);
    }

    private void nodes(Context ctx) {
        MetadataNode metadata = node.metadataNode();
        List<Map<String, Object>> result = metadata.stateMachine().read(() -> {
            Map<Integer, Long> epochs = metadata.stateMachine().streamControlManager().nodeEpochs();
            Map<Integer, String> addresses = metadata.stateMachine().streamControlManager().nodeAddresses();
            List<Map<String, Object>> list = new ArrayList<>();

            for (Map.Entry<Integer, Long> entry : epochs.entrySet()) {
                Map<String, Object> item = new LinkedHashMap<>();

                item.put("nodeId", entry.getKey());
                item.put("nodeEpoch", entry.getValue());
                item.put("advertisedAddress", addresses.get(entry.getKey()));
                item.put("local", entry.getKey() == config.nodeId());
                list.add(item);
            }

            return list;
        });

        json(ctx, 200, Map.of("nodes", result));
    }

    private void stream(Context ctx) throws Exception {
        String name = "/" + ctx.pathParam("name");
        Owner owner = node.service().ownership().ownerOf(name);
        Map<String, Object> body = new LinkedHashMap<>();

        body.put("name", name);
        body.put("streamId", owner.streamId().isPresent() ? owner.streamId().getAsLong() : null);
        body.put("ownerLocal", owner.local());
        body.put("ownerNodeId", owner.local() ? config.nodeId() : owner.ownerNodeId());
        body.put("ownerAdvertisedAddress",
            owner.local() ? config.httpAddress() : owner.ownerAdvertisedAddress());

        if (owner.local()) {
            Optional<StreamMeta> meta = node.service().lifecycle().head(name);

            if (meta.isEmpty()) {
                error(ctx, 404, "stream not found: " + name);
                return;
            }

            body.put("meta", metaBody(meta.get()));
        }

        json(ctx, 200, body);
    }

    private void addPeer(Context ctx) throws Exception {
        PeerId peer = requirePeer(ctx);

        if (Objects.isNull(peer) || !requireLeader(ctx)) {
            return;
        }

        node.metadataNode().addPeer(peer).get(PEER_CHANGE_TIMEOUT_SEC, TimeUnit.SECONDS);
        json(ctx, 200, Map.of("added", peer.toString()));
    }

    private void removePeer(Context ctx) throws Exception {
        PeerId peer = new PeerId();

        if (!peer.parse(ctx.pathParam("peer"))) {
            error(ctx, 400, "invalid peer, expected host:port");
            return;
        }

        if (!requireLeader(ctx)) {
            return;
        }

        node.metadataNode().removePeer(peer).get(PEER_CHANGE_TIMEOUT_SEC, TimeUnit.SECONDS);
        json(ctx, 200, Map.of("removed", peer.toString()));
    }

    private void transferLeader(Context ctx) throws Exception {
        PeerId peer = requirePeer(ctx);

        if (Objects.isNull(peer) || !requireLeader(ctx)) {
            return;
        }

        var status = node.metadataNode().transferLeader(peer);

        if (!status.isOk()) {
            error(ctx, 500, status.getErrorMsg());
            return;
        }

        json(ctx, 200, Map.of("transferredTo", peer.toString()));
    }

    private void triggerSnapshot(Context ctx) throws Exception {
        node.metadataNode().triggerSnapshot();
        json(ctx, 200, Map.of("appliedIndex", node.metadataNode().stateMachine().appliedIndex()));
    }

    private void snapshots(Context ctx) {
        SnapshotArchive archive = node.snapshotArchive();

        if (Objects.isNull(archive)) {
            error(ctx, 404, "metadata snapshot archive disabled");
            return;
        }

        List<Map<String, Object>> items = new ArrayList<>();

        for (SnapshotArchive.ArchivedSnapshot snapshot : archive.list()) {
            Map<String, Object> item = new LinkedHashMap<>();

            item.put("key", snapshot.key());
            item.put("appliedIndex", snapshot.appliedIndex());
            item.put("timestampMs", snapshot.timestampMs());
            item.put("size", snapshot.size());
            items.add(item);
        }

        Map<String, Object> body = new LinkedHashMap<>();

        body.put("archiveSuccessCount", archive.successCount());
        body.put("archiveFailureCount", archive.failureCount());
        body.put("lastArchivedIndex", archive.lastArchivedIndex());
        body.put("snapshots", items);
        json(ctx, 200, body);
    }

    private PeerId requirePeer(Context ctx) throws Exception {
        JsonNode json = mapper.readTree(ctx.bodyInputStream());
        JsonNode value = json.get("peer");

        if (Objects.isNull(value) || !value.isTextual()) {
            error(ctx, 400, "body must be {\"peer\": \"host:port\"}");
            return null;
        }

        PeerId peer = new PeerId();

        if (!peer.parse(value.asText())) {
            error(ctx, 400, "invalid peer, expected host:port");
            return null;
        }

        return peer;
    }

    private boolean requireLeader(Context ctx) {
        MetadataNode metadata = node.metadataNode();

        if (metadata.isLeader()) {
            return true;
        }

        PeerId leader = metadata.leaderId();
        Map<String, Object> body = new LinkedHashMap<>();

        body.put("error", "not raft leader");
        body.put("leader", Objects.isNull(leader) ? null : leader.toString());
        json(ctx, 409, body);
        return false;
    }

    private static List<String> peers(MetadataNode metadata) {
        try {
            return metadata.raftNode().listPeers().stream().map(PeerId::toString).toList();
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, Object> metaBody(StreamMeta meta) {
        Map<String, Object> body = new LinkedHashMap<>();

        body.put("streamId", meta.streamId());
        body.put("contentType", meta.contentType());
        body.put("ttlSeconds", meta.ttlSeconds());
        body.put("expiresAt", Objects.isNull(meta.expiresAt()) ? null : meta.expiresAt().toString());
        body.put("startOffset", Objects.toString(meta.startOffset(), null));
        body.put("nextOffset", Objects.toString(meta.nextOffset(), null));
        body.put("closed", meta.closed());
        return body;
    }

    private void json(Context ctx, int status, Object body) {
        try {
            ctx.status(status);
            ctx.contentType("application/json");
            ctx.result(mapper.writeValueAsBytes(body));
        } catch (Exception e) {
            ctx.status(500);
            ctx.result("{\"error\":\"serialization failed\"}");
        }
    }

    private void error(Context ctx, int status, String message) {
        json(ctx, status, Map.of("error", Objects.toString(message, "internal error")));
    }

    @Override
    public void close() {
        try {
            app.stop();
        } catch (Exception ignored) {
        }

        started.set(false);
    }
}
