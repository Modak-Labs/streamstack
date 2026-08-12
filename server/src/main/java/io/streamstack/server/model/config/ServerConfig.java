package io.streamstack.server.model.config;

import io.streamstack.s3.ByteBufAllocPolicy;
import io.streamstack.s3.Config;
import io.streamstack.s3.operator.BucketURI;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class ServerConfig {

    private final int nodeId;
    private final long nodeEpoch;
    private final String httpHost;
    private final int httpPort;
    private final String raftHost;
    private final int raftPort;
    private final List<String> raftPeers;
    private final File dataDir;
    private final String storageUri;
    private final String walUri;
    private final String clusterId;
    private final RoutingMode routingMode;
    private final int longPollTimeoutSec;
    private final int sseMaxDurationSec;
    private final int maxChunkSize;
    private final StreamConfig streamConfig;
    private final Map<String, String> envs;

    private ServerConfig(Builder builder) {
        this.nodeId = builder.nodeId;
        this.nodeEpoch = builder.nodeEpoch;
        this.httpHost = builder.httpHost;
        this.httpPort = builder.httpPort;
        this.raftHost = builder.raftHost;
        this.raftPort = builder.raftPort;
        this.raftPeers = List.copyOf(builder.raftPeers);
        this.dataDir = builder.dataDir;
        this.storageUri = Objects.requireNonNull(builder.storageUri, "storageUri");
        this.walUri = builder.walUri;
        this.clusterId = builder.clusterId;
        this.routingMode = builder.routingMode;
        this.longPollTimeoutSec = builder.longPollTimeoutSec;
        this.sseMaxDurationSec = builder.sseMaxDurationSec;
        this.maxChunkSize = builder.maxChunkSize;
        this.streamConfig = builder.streamConfig.build();
        this.envs = Map.copyOf(builder.envs);
    }

    public static Builder builder() {
        return new Builder();
    }

    public int nodeId() {
        return nodeId;
    }

    public long nodeEpoch() {
        return nodeEpoch;
    }

    public String httpHost() {
        return httpHost;
    }

    public int httpPort() {
        return httpPort;
    }

    public String raftHost() {
        return raftHost;
    }

    public int raftPort() {
        return raftPort;
    }

    public List<String> raftPeers() {
        return raftPeers;
    }

    public File dataDir() {
        return dataDir;
    }

    public File objectDir() {
        BucketURI uri = BucketURI.parse(storageUri);

        if (!"file".equalsIgnoreCase(uri.protocol())) {
            return null;
        }

        return new File(uri.bucket());
    }

    public String storageUri() {
        return storageUri;
    }

    public Optional<String> walUri() {
        return Optional.ofNullable(walUri);
    }

    public String resolveWalUri() {
        if (Objects.nonNull(walUri) && !walUri.isBlank()) {
            return walUri;
        }

        BucketURI storage = BucketURI.parse(storageUri);

        if ("s3".equalsIgnoreCase(storage.protocol())) {
            return storageUri;
        }

        return "memory";
    }

    public String clusterId() {
        return clusterId;
    }

    public RoutingMode routingMode() {
        return routingMode;
    }

    public int longPollTimeoutSec() {
        return longPollTimeoutSec;
    }

    public int sseMaxDurationSec() {
        return sseMaxDurationSec;
    }

    public int maxChunkSize() {
        return maxChunkSize;
    }

    public StreamConfig streamConfig() {
        return streamConfig;
    }

    public Map<String, String> envs() {
        return envs;
    }

    public String httpAddress() {
        return "http://" + httpHost + ":" + httpPort;
    }

    public void applyTo(Config target) {
        streamConfig.applyTo(target);
        target.walConfig(resolveWalUri());
        target.dataBuckets(List.of(BucketURI.parse(storageUri)));
    }

    public static ServerConfig fromArgs(String[] args) {
        Path topoPath = null;
        Integer topoNodeId = null;

        for (int i = 0; i < args.length; i++) {
            if ("--topo".equals(args[i])) {
                topoPath = Path.of(requireValue(args, ++i, "--topo"));
            } else if ("--node-id".equals(args[i])) {
                topoNodeId = Integer.parseInt(requireValue(args, ++i, "--node-id"));
            }
        }

        Builder builder = builder();

        if (Objects.nonNull(topoPath)) {
            if (Objects.isNull(topoNodeId)) {
                throw new IllegalArgumentException("--topo requires --node-id");
            }

            builder.applyTopology(ClusterConfig.load(topoPath), topoNodeId);
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "--topo" -> i++;
                case "--node-id" -> builder.nodeId(Integer.parseInt(requireValue(args, ++i, arg)));
                case "--node-epoch" -> builder.nodeEpoch(Long.parseLong(requireValue(args, ++i, arg)));
                case "--http-host" -> builder.httpHost(requireValue(args, ++i, arg));
                case "--http-port" -> builder.httpPort(Integer.parseInt(requireValue(args, ++i, arg)));
                case "--raft-host" -> builder.raftHost(requireValue(args, ++i, arg));
                case "--raft-port" -> builder.raftPort(Integer.parseInt(requireValue(args, ++i, arg)));
                case "--peer" -> builder.addPeer(requireValue(args, ++i, arg));
                case "--data-dir" -> builder.dataDir(new File(requireValue(args, ++i, arg)));
                case "--object-dir" -> builder.objectDir(new File(requireValue(args, ++i, arg)));
                case "--storage" -> builder.storageUri(requireValue(args, ++i, arg));
                case "--wal" -> builder.walUri(requireValue(args, ++i, arg));
                case "--cluster-id" -> builder.clusterId(requireValue(args, ++i, arg));
                case "--routing" -> builder.routingMode(RoutingMode.valueOf(requireValue(args, ++i, arg)));
                case "--wal-cache-size" -> builder.streamConfig().walCacheSize(Long.parseLong(requireValue(args, ++i, arg)));
                case "--block-cache-size" ->
                    builder.streamConfig().blockCacheSize(Long.parseLong(requireValue(args, ++i, arg)));
                case "--wal-upload-threshold" ->
                    builder.streamConfig().walUploadThreshold(Long.parseLong(requireValue(args, ++i, arg)));
                case "--alloc-policy" ->
                    builder.streamConfig().allocPolicy(ByteBufAllocPolicy.valueOf(requireValue(args, ++i, arg)));
                default -> throw new IllegalArgumentException("unknown arg: " + arg);
            }
        }

        if (builder.raftPeers.isEmpty()) {
            builder.addPeer(builder.raftHost + ":" + builder.raftPort);
        }

        return builder.build();
    }

    private static String requireValue(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException("missing value for " + flag);
        }

        return args[index];
    }

    public static final class Builder {
        private int nodeId = 1;
        private long nodeEpoch = System.currentTimeMillis();
        private String httpHost = "127.0.0.1";
        private int httpPort = 4437;
        private String raftHost = "127.0.0.1";
        private int raftPort = 8091;
        private final List<String> raftPeers = new ArrayList<>();
        private File dataDir = new File("./data");
        private String storageUri = "-2@file://" + new File("./objects").getAbsolutePath();
        private String walUri;
        private String clusterId = "streamstack";
        private RoutingMode routingMode = RoutingMode.REDIRECT;
        private int longPollTimeoutSec = 25;
        private int sseMaxDurationSec = 55;
        private int maxChunkSize = 64 * 1024;
        private final StreamConfig.Builder streamConfig = StreamConfig.builder();
        private final Map<String, String> envs = new LinkedHashMap<>();
        public Builder nodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder nodeEpoch(long nodeEpoch) {
            this.nodeEpoch = nodeEpoch;
            return this;
        }

        public Builder httpHost(String httpHost) {
            this.httpHost = Objects.requireNonNull(httpHost);
            return this;
        }

        public Builder httpPort(int httpPort) {
            this.httpPort = httpPort;
            return this;
        }

        public Builder raftHost(String raftHost) {
            this.raftHost = Objects.requireNonNull(raftHost);
            return this;
        }

        public Builder raftPort(int raftPort) {
            this.raftPort = raftPort;
            return this;
        }

        public Builder addPeer(String peer) {
            this.raftPeers.add(peer);
            return this;
        }

        public Builder raftPeers(List<String> peers) {
            this.raftPeers.clear();
            this.raftPeers.addAll(peers);

            return this;
        }

        public Builder dataDir(File dataDir) {
            this.dataDir = Objects.requireNonNull(dataDir);
            return this;
        }

        public Builder objectDir(File objectDir) {
            Objects.requireNonNull(objectDir, "objectDir");
            this.storageUri = "-2@file://" + objectDir.getAbsolutePath();

            return this;
        }

        public Builder storageUri(String storageUri) {
            this.storageUri = Objects.requireNonNull(storageUri);
            return this;
        }

        public Builder walUri(String walUri) {
            this.walUri = walUri;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = Objects.requireNonNull(clusterId);
            return this;
        }

        public Builder routingMode(RoutingMode routingMode) {
            this.routingMode = Objects.requireNonNull(routingMode);
            return this;
        }

        public Builder longPollTimeoutSec(int longPollTimeoutSec) {
            this.longPollTimeoutSec = longPollTimeoutSec;
            return this;
        }

        public Builder sseMaxDurationSec(int sseMaxDurationSec) {
            this.sseMaxDurationSec = sseMaxDurationSec;
            return this;
        }

        public Builder maxChunkSize(int maxChunkSize) {
            this.maxChunkSize = maxChunkSize;
            return this;
        }

        public StreamConfig.Builder streamConfig() {
            return streamConfig;
        }

        public Builder env(String name, String value) {
            this.envs.put(name, value);
            return this;
        }

        public Builder applyTopology(ClusterConfig topology, int forNodeId) {
            Objects.requireNonNull(topology, "topology");
            ClusterConfig.Node node = topology.requireNode(forNodeId);

            this.clusterId = topology.global().clusterName();

            if (Objects.nonNull(topology.global().storage())) {
                this.storageUri = topology.global().storage();
            }

            if (Objects.nonNull(topology.global().wal())) {
                this.walUri = topology.global().wal();
            }

            this.nodeId = node.nodeId();
            this.httpHost = node.host();

            if (Objects.nonNull(node.httpPort())) {
                this.httpPort = node.httpPort();
            }

            this.raftHost = node.host();

            if (Objects.nonNull(node.raftPort())) {
                this.raftPort = node.raftPort();
            }

            if (Objects.nonNull(node.dataDir())) {
                this.dataDir = new File(node.dataDir());
            }

            this.raftPeers.clear();

            for (ClusterConfig.Node peer : topology.nodes()) {
                int peerRaft = Objects.nonNull(peer.raftPort()) ? peer.raftPort() : 8091;

                this.raftPeers.add(peer.host() + ":" + peerRaft);
            }

            for (ClusterConfig.Env env : topology.global().envs()) {
                this.envs.put(env.name(), env.value());
            }

            applyConfigMap(topology.global().config());

            return this;
        }

        public Builder applyConfigMap(Map<String, Object> config) {
            if (Objects.isNull(config) || config.isEmpty()) {
                return this;
            }

            for (Map.Entry<String, Object> entry : config.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (streamConfig.apply(key, value)) {
                    continue;
                }

                switch (key) {
                    case "routing" -> routingMode(RoutingMode.valueOf(String.valueOf(value)));
                    case "longPollTimeoutSec" -> longPollTimeoutSec(toInt(value));
                    case "sseMaxDurationSec" -> sseMaxDurationSec(toInt(value));
                    case "maxChunkSize" -> maxChunkSize(toInt(value));
                    case "clusterId" -> clusterId(String.valueOf(value));
                    default -> throw new IllegalArgumentException("unknown topology config key: " + key);
                }
            }

            return this;
        }

        private static int toInt(Object value) {
            if (value instanceof Number number) {
                return number.intValue();
            }

            return Integer.parseInt(String.valueOf(value));
        }

        public ServerConfig build() {
            return new ServerConfig(this);
        }
    }
}
