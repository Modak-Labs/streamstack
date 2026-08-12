package io.streamstack.metadata.raft;

import java.util.Objects;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;

import io.streamstack.metadata.model.MetadataCommand;
import io.streamstack.metadata.codec.MetadataCommandCodec;
import io.streamstack.metadata.rpc.MetadataCommandProcessor;
import io.streamstack.s3.operator.ObjectStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MetadataNode implements AutoCloseable {

    public static final String DEFAULT_GROUP_ID = "streamstack-metadata";

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataNode.class);

    private final int nodeId;
    private final long nodeEpoch;
    private final String httpAddress;
    private final PeerId serverId;
    private final MetadataStateMachine stateMachine;
    private final RaftGroupService raftGroupService;
    private final Node node;
    private final MetadataClient client;
    private final MetadataLifecycle lifecycle;
    private final MetadataHealth health;
    private final ScheduledExecutorService registrar;

    private final AtomicBoolean registered = new AtomicBoolean(false);

    public record Options(
        int electionTimeoutMs,
        int snapshotIntervalSecs,
        int snapshotLogIndexMargin,
        boolean syncLog) {
        public static Options defaults() {
            return new Options(1000, 30, 0, true);
        }
    }

    public MetadataNode(int nodeId, String host, int port, File dataDir, List<String> peers) throws IOException {
        this(nodeId, host, port, dataDir, peers, System.currentTimeMillis(), null, Options.defaults(), "");
    }

    public MetadataNode(int nodeId, String host, int port, File dataDir, List<String> peers, long nodeEpoch)
        throws IOException {
        this(nodeId, host, port, dataDir, peers, nodeEpoch, null, Options.defaults(), "");
    }

    public MetadataNode(int nodeId, String host, int port, File dataDir, List<String> peers, long nodeEpoch,
        ObjectStorage objectStorage) throws IOException {
        this(nodeId, host, port, dataDir, peers, nodeEpoch, objectStorage, Options.defaults(), "");
    }

    public MetadataNode(
        int nodeId,
        String host,
        int port,
        File dataDir,
        List<String> peers,
        long nodeEpoch,
        ObjectStorage objectStorage,
        Options options) throws IOException {
        this(nodeId, host, port, dataDir, peers, nodeEpoch, objectStorage, options, "");
    }

    public MetadataNode(
        int nodeId,
        String host,
        int port,
        File dataDir,
        List<String> peers,
        long nodeEpoch,
        ObjectStorage objectStorage,
        Options options,
        String httpAddress) throws IOException {
        this.nodeId = nodeId;
        this.nodeEpoch = nodeEpoch;
        this.httpAddress = Objects.isNull(httpAddress) ? "" : httpAddress;
        this.serverId = new PeerId(host, port);
        this.stateMachine = new MetadataStateMachine();
        Files.createDirectories(dataDir.toPath());
        File logDir = new File(dataDir, "log");
        File metaDir = new File(dataDir, "meta");
        File snapshotDir = new File(dataDir, "snapshot");

        Files.createDirectories(logDir.toPath());
        Files.createDirectories(metaDir.toPath());
        Files.createDirectories(snapshotDir.toPath());
        NodeOptions nodeOptions = new NodeOptions();

        nodeOptions.setElectionTimeoutMs(options.electionTimeoutMs());
        nodeOptions.setDisableCli(false);
        nodeOptions.setSnapshotIntervalSecs(options.snapshotIntervalSecs());
        nodeOptions.setSnapshotLogIndexMargin(options.snapshotLogIndexMargin());
        RaftOptions raftOptions = new RaftOptions();

        raftOptions.setSync(options.syncLog());
        nodeOptions.setRaftOptions(raftOptions);
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setLogUri(logDir.getAbsolutePath());
        nodeOptions.setRaftMetaUri(metaDir.getAbsolutePath());
        nodeOptions.setSnapshotUri(snapshotDir.getAbsolutePath());
        Configuration conf = new Configuration();

        for (String peer : peers) {
            PeerId peerId = new PeerId();

            if (!peerId.parse(peer)) {
                throw new IllegalArgumentException("invalid peer " + peer);
            }

            conf.addPeer(peerId);
        }

        nodeOptions.setInitialConf(conf);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());

        rpcServer.registerProcessor(new MetadataCommandProcessor(this));
        this.raftGroupService = new RaftGroupService(DEFAULT_GROUP_ID, serverId, nodeOptions, rpcServer);
        this.node = raftGroupService.start();
        this.client = new MetadataClient(this, conf);
        this.lifecycle = new MetadataLifecycle(client, objectStorage);
        this.stateMachine.setLifecycle(lifecycle);
        this.health = new MetadataHealth(this);

        if (this.node.isLeader()) {
            lifecycle.onLeaderStart();
        }

        this.registrar = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metadata-registrar-" + nodeId);

            t.setDaemon(true);

            return t;
        });

        this.registrar.scheduleWithFixedDelay(this::tryRegister, 100, 500, TimeUnit.MILLISECONDS);
    }

    private void tryRegister() {
        if (registered.get()) {
            registrar.shutdown();
            return;
        }

        client.propose(new MetadataCommand.RegisterNode(nodeId, nodeEpoch, httpAddress))
            .whenComplete((result, error) -> {
                if (Objects.isNull(error)) {
                    if (registered.compareAndSet(false, true)) {
                        LOGGER.info("metadata node {} registered with epoch {}", nodeId, nodeEpoch);
                    }

                    registrar.shutdown();
                } else {
                    LOGGER.debug("metadata node {} registration attempt failed: {}", nodeId, error.getMessage());
                }
            });
    }

    public boolean isRegistered() {
        return registered.get();
    }

    public String groupId() {
        return DEFAULT_GROUP_ID;
    }

    public int nodeId() {
        return nodeId;
    }

    public long nodeEpoch() {
        return nodeEpoch;
    }

    public String httpAddress() {
        return httpAddress;
    }

    public Node raftNode() {
        return node;
    }

    public MetadataStateMachine stateMachine() {
        return stateMachine;
    }

    public MetadataClient client() {
        return client;
    }

    public MetadataLifecycle lifecycle() {
        return lifecycle;
    }

    public MetadataHealth health() {
        return health;
    }

    public boolean isLeader() {
        return node.isLeader();
    }

    public PeerId leaderId() {
        return node.getLeaderId();
    }

    public void awaitLeader(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        while (System.nanoTime() < deadline) {
            if (Objects.nonNull(node.getLeaderId())) {
                return;
            }

            Thread.sleep(50);
        }

        throw new IllegalStateException("timed out waiting for metadata leader");
    }

    public void awaitRegistered(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        while (System.nanoTime() < deadline) {
            if (registered.get()) {
                return;
            }

            Thread.sleep(50);
        }

        throw new IllegalStateException("timed out waiting for metadata node registration");
    }

    public CompletableFuture<Object> propose(MetadataCommand command) {
        if (!node.isLeader()) {
            PeerId leader = node.getLeaderId();
            return CompletableFuture.failedFuture(
                new NotLeaderException(Objects.isNull(leader) ? null : leader.toString()));
        }

        byte[] data = MetadataCommandCodec.encode(command);
        MetadataClosure closure = new MetadataClosure(command);
        Task task = new Task();

        task.setData(ByteBuffer.wrap(data));
        task.setDone(closure);
        node.apply(task);

        return closure.future();
    }

    public CompletableFuture<Status> changePeers(Configuration conf) {
        CompletableFuture<Status> future = new CompletableFuture<>();

        node.changePeers(conf, status -> completeStatus(future, status));

        return future;
    }

    public CompletableFuture<Status> addPeer(PeerId peer) {
        CompletableFuture<Status> future = new CompletableFuture<>();

        node.addPeer(peer, status -> completeStatus(future, status));

        return future;
    }

    public CompletableFuture<Status> removePeer(PeerId peer) {
        CompletableFuture<Status> future = new CompletableFuture<>();

        node.removePeer(peer, status -> completeStatus(future, status));

        return future;
    }

    public void triggerSnapshot() throws InterruptedException {
        CompletableFuture<Status> future = new CompletableFuture<>();

        node.snapshot(status -> {
            if (status.isOk()) {
                future.complete(status);
            } else {
                future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
            }
        });

        future.join();
    }

    public static String peerString(String host, int port) {
        return host + ":" + port;
    }

    public static List<String> singlePeer(String host, int port) {
        return List.of(peerString(host, port));
    }

    private static void completeStatus(CompletableFuture<Status> future, Status status) {
        if (status.isOk()) {
            future.complete(status);
        } else {
            future.completeExceptionally(new IllegalStateException(status.getErrorMsg()));
        }
    }

    @Override
    public void close() {
        registrar.shutdownNow();
        lifecycle.close();
        client.close();

        if (Objects.nonNull(raftGroupService)) {
            raftGroupService.shutdown();

            try {
                raftGroupService.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static final class NotLeaderException extends RuntimeException {
        private final String leaderId;
        public NotLeaderException(String leaderId) {
            super(Objects.isNull(leaderId) ? "no leader" : "not leader, leader=" + leaderId);
            this.leaderId = leaderId;
        }

        public String leaderId() {
            return leaderId;
        }
    }
}
