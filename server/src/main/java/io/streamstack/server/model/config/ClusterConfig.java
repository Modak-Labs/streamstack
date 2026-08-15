package io.streamstack.server.model.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = false)
public final class ClusterConfig {

    private Global global = new Global();

    private List<Node> nodes = new ArrayList<>();

    public Global getGlobal() {
        return global;
    }

    public void setGlobal(Global global) {
        this.global = Objects.nonNull(global) ? global : new Global();
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = Objects.nonNull(nodes) ? nodes : new ArrayList<>();
    }

    public Global global() {
        return global;
    }

    public List<Node> nodes() {
        return nodes;
    }

    public Node requireNode(int nodeId) {
        return nodes.stream()
            .filter(n -> n.nodeId == nodeId)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("nodeId not found in topology: " + nodeId));
    }

    public void validate() {
        if (Objects.isNull(global.clusterName) || global.clusterName.isBlank()) {
            throw new IllegalArgumentException("global.clusterName is required");
        }

        if (Objects.isNull(global.storage) || global.storage.isBlank()) {
            throw new IllegalArgumentException("global.storage is required");
        }

        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be empty");
        }

        for (Node node : nodes) {
            if (Objects.isNull(node.host) || node.host.isBlank()) {
                throw new IllegalArgumentException("node.host is required");
            }

            if (node.nodeId <= 0) {
                throw new IllegalArgumentException("node.nodeId must be > 0");
            }
        }

        long distinct = nodes.stream().map(n -> n.nodeId).distinct().count();

        if (distinct != nodes.size()) {
            throw new IllegalArgumentException("duplicate nodeId in topology");
        }
    }

    public static ClusterConfig load(Path path) {
        Objects.requireNonNull(path, "path");
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try (Reader reader = Files.newBufferedReader(path)) {
            ClusterConfig topology = mapper.readValue(reader, ClusterConfig.class);

            if (Objects.isNull(topology)) {
                throw new IllegalArgumentException("empty topology: " + path);
            }

            topology.validate();

            return topology;
        } catch (IOException e) {
            throw new IllegalArgumentException("failed to load topology: " + path, e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class Global {
        private String clusterName;
        private String storage;
        private String wal;
        private Map<String, Object> config = new LinkedHashMap<>();
        private List<Env> envs = new ArrayList<>();
        public String getClusterName() {
            return clusterName;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public String getStorage() {
            return storage;
        }

        public void setStorage(String storage) {
            this.storage = storage;
        }

        public String getWal() {
            return wal;
        }

        public void setWal(String wal) {
            this.wal = wal;
        }

        public Map<String, Object> getConfig() {
            return config;
        }

        public void setConfig(Map<String, Object> config) {
            this.config = Objects.nonNull(config) ? config : new LinkedHashMap<>();
        }

        public List<Env> getEnvs() {
            return envs;
        }

        public void setEnvs(List<Env> envs) {
            this.envs = Objects.nonNull(envs) ? envs : new ArrayList<>();
        }

        public String clusterName() {
            return clusterName;
        }

        public String storage() {
            return storage;
        }

        public String wal() {
            return wal;
        }

        public Map<String, Object> config() {
            return config;
        }

        public List<Env> envs() {
            return envs;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class Env {
        private String name;
        private String value;
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String name() {
            return name;
        }

        public String value() {
            return value;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = false)
    public static final class Node {
        private String host;
        private int nodeId;
        private Integer httpPort;
        private Integer adminPort;
        private Integer raftPort;
        private String dataDir;
        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getNodeId() {
            return nodeId;
        }

        public void setNodeId(int nodeId) {
            this.nodeId = nodeId;
        }

        public Integer getHttpPort() {
            return httpPort;
        }

        public void setHttpPort(Integer httpPort) {
            this.httpPort = httpPort;
        }

        public Integer getAdminPort() {
            return adminPort;
        }

        public void setAdminPort(Integer adminPort) {
            this.adminPort = adminPort;
        }

        public Integer getRaftPort() {
            return raftPort;
        }

        public void setRaftPort(Integer raftPort) {
            this.raftPort = raftPort;
        }

        public String getDataDir() {
            return dataDir;
        }

        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }

        public String host() {
            return host;
        }

        public int nodeId() {
            return nodeId;
        }

        public Integer httpPort() {
            return httpPort;
        }

        public Integer adminPort() {
            return adminPort;
        }

        public Integer raftPort() {
            return raftPort;
        }

        public String dataDir() {
            return dataDir;
        }
    }
}
