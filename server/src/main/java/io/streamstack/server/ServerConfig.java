package io.streamstack.server;

import io.streamstack.server.http.OwnershipRouter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ServerConfig {
    private final int nodeId;
    private final long nodeEpoch;
    private final String httpHost;
    private final int httpPort;
    private final String raftHost;
    private final int raftPort;
    private final List<String> raftPeers;
    private final File dataDir;
    private final File objectDir;
    private final OwnershipRouter.Mode routingMode;
    private final int longPollTimeoutSec;
    private final int sseMaxDurationSec;
    private final int maxChunkSize;

    private ServerConfig(Builder builder) {
        this.nodeId = builder.nodeId;
        this.nodeEpoch = builder.nodeEpoch;
        this.httpHost = builder.httpHost;
        this.httpPort = builder.httpPort;
        this.raftHost = builder.raftHost;
        this.raftPort = builder.raftPort;
        this.raftPeers = List.copyOf(builder.raftPeers);
        this.dataDir = builder.dataDir;
        this.objectDir = builder.objectDir;
        this.routingMode = builder.routingMode;
        this.longPollTimeoutSec = builder.longPollTimeoutSec;
        this.sseMaxDurationSec = builder.sseMaxDurationSec;
        this.maxChunkSize = builder.maxChunkSize;
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
        return objectDir;
    }

    public OwnershipRouter.Mode routingMode() {
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

    public String httpAddress() {
        return "http://" + httpHost + ":" + httpPort;
    }

    public static ServerConfig fromArgs(String[] args) {
        Builder builder = builder();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--node-id" -> builder.nodeId(Integer.parseInt(args[++i]));
                case "--node-epoch" -> builder.nodeEpoch(Long.parseLong(args[++i]));
                case "--http-host" -> builder.httpHost(args[++i]);
                case "--http-port" -> builder.httpPort(Integer.parseInt(args[++i]));
                case "--raft-host" -> builder.raftHost(args[++i]);
                case "--raft-port" -> builder.raftPort(Integer.parseInt(args[++i]));
                case "--peer" -> builder.addPeer(args[++i]);
                case "--data-dir" -> builder.dataDir(new File(args[++i]));
                case "--object-dir" -> builder.objectDir(new File(args[++i]));
                case "--routing" -> builder.routingMode(OwnershipRouter.Mode.valueOf(args[++i]));
                default -> throw new IllegalArgumentException("unknown arg: " + arg);
            }
        }
        if (builder.raftPeers.isEmpty()) {
            builder.addPeer(builder.raftHost + ":" + builder.raftPort);
        }
        return builder.build();
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
        private File objectDir = new File("./objects");
        private OwnershipRouter.Mode routingMode = OwnershipRouter.Mode.REDIRECT;
        private int longPollTimeoutSec = 25;
        private int sseMaxDurationSec = 55;
        private int maxChunkSize = 64 * 1024;

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
            this.objectDir = Objects.requireNonNull(objectDir);
            return this;
        }

        public Builder routingMode(OwnershipRouter.Mode routingMode) {
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

        public ServerConfig build() {
            return new ServerConfig(this);
        }
    }
}
