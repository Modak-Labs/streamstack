package io.streamstack.server.model.config;

import io.streamstack.s3.ByteBufAllocPolicy;
import io.streamstack.s3.Config;
import io.streamstack.s3.network.NetworkBandwidthMode;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class StreamConfig {

    private final ByteBufAllocPolicy allocPolicy;
    private final Long walCacheSize;
    private final Long walUploadThreshold;
    private final Long walUploadIntervalMs;
    private final Long blockCacheSize;
    private final Integer splitSize;
    private final Integer objectBlockSize;
    private final Integer objectPartSize;
    private final ObjectCompaction objectCompaction;
    private final StreamSetCompaction streamSetCompaction;
    private final Integer maxStreamsPerSetObject;
    private final Integer maxObjectsPerCommit;
    private final Long networkBaselineBandwidth;
    private final NetworkBandwidthMode networkBandwidthMode;
    private final Integer refillPeriodMs;
    private final Long objectRetentionTimeInSecond;

    private StreamConfig(Builder builder) {
        this.allocPolicy = builder.allocPolicy;
        this.walCacheSize = builder.walCacheSize;
        this.walUploadThreshold = builder.walUploadThreshold;
        this.walUploadIntervalMs = builder.walUploadIntervalMs;
        this.blockCacheSize = builder.blockCacheSize;
        this.splitSize = builder.splitSize;
        this.objectBlockSize = builder.objectBlockSize;
        this.objectPartSize = builder.objectPartSize;
        this.objectCompaction = builder.objectCompaction.build();
        this.streamSetCompaction = builder.streamSetCompaction.build();
        this.maxStreamsPerSetObject = builder.maxStreamsPerSetObject;
        this.maxObjectsPerCommit = builder.maxObjectsPerCommit;
        this.networkBaselineBandwidth = builder.networkBaselineBandwidth;
        this.networkBandwidthMode = builder.networkBandwidthMode;
        this.refillPeriodMs = builder.refillPeriodMs;
        this.objectRetentionTimeInSecond = builder.objectRetentionTimeInSecond;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<ByteBufAllocPolicy> allocPolicy() {
        return Optional.ofNullable(allocPolicy);
    }

    public ObjectCompaction objectCompaction() {
        return objectCompaction;
    }

    public StreamSetCompaction streamSetCompaction() {
        return streamSetCompaction;
    }

    public void applyTo(Config streamConfig) {
        Objects.requireNonNull(streamConfig, "streamConfig");
        if (Objects.nonNull(walCacheSize)) {
            streamConfig.walCacheSize(walCacheSize);
        }
        if (Objects.nonNull(walUploadThreshold)) {
            streamConfig.walUploadThreshold(walUploadThreshold);
        }
        if (Objects.nonNull(walUploadIntervalMs)) {
            streamConfig.walUploadIntervalMs(walUploadIntervalMs);
        }
        if (Objects.nonNull(blockCacheSize)) {
            streamConfig.blockCacheSize(blockCacheSize);
        }
        if (Objects.nonNull(splitSize)) {
            streamConfig.streamSplitSize(splitSize);
        }
        if (Objects.nonNull(objectBlockSize)) {
            streamConfig.objectBlockSize(objectBlockSize);
        }
        if (Objects.nonNull(objectPartSize)) {
            streamConfig.objectPartSize(objectPartSize);
        }
        if (Objects.nonNull(objectCompaction.intervalMinutes)) {
            streamConfig.streamObjectCompactionIntervalMinutes(objectCompaction.intervalMinutes);
        }
        if (Objects.nonNull(objectCompaction.maxSizeBytes)) {
            streamConfig.streamObjectCompactionMaxSizeBytes(objectCompaction.maxSizeBytes);
        }
        if (Objects.nonNull(streamSetCompaction.interval)) {
            streamConfig.streamSetObjectCompactionInterval(streamSetCompaction.interval);
        }
        if (Objects.nonNull(streamSetCompaction.cacheSize)) {
            streamConfig.streamSetObjectCompactionCacheSize(streamSetCompaction.cacheSize);
        }
        if (Objects.nonNull(streamSetCompaction.uploadConcurrency)) {
            streamConfig.streamSetObjectCompactionUploadConcurrency(streamSetCompaction.uploadConcurrency);
        }
        if (Objects.nonNull(streamSetCompaction.splitSize)) {
            streamConfig.streamSetObjectCompactionStreamSplitSize(streamSetCompaction.splitSize);
        }
        if (Objects.nonNull(streamSetCompaction.forceSplitPeriod)) {
            streamConfig.streamSetObjectCompactionForceSplitPeriod(streamSetCompaction.forceSplitPeriod);
        }
        if (Objects.nonNull(streamSetCompaction.maxObjectNum)) {
            streamConfig.streamSetObjectCompactionMaxObjectNum(streamSetCompaction.maxObjectNum);
        }
        if (Objects.nonNull(maxStreamsPerSetObject)) {
            streamConfig.maxStreamNumPerStreamSetObject(maxStreamsPerSetObject);
        }
        if (Objects.nonNull(maxObjectsPerCommit)) {
            streamConfig.maxStreamObjectNumPerCommit(maxObjectsPerCommit);
        }
        if (Objects.nonNull(networkBaselineBandwidth)) {
            streamConfig.networkBaselineBandwidth(networkBaselineBandwidth);
        }
        if (Objects.nonNull(networkBandwidthMode)) {
            streamConfig.networkBandwidthMode(networkBandwidthMode);
        }
        if (Objects.nonNull(refillPeriodMs)) {
            streamConfig.refillPeriodMs(refillPeriodMs);
        }
        if (Objects.nonNull(objectRetentionTimeInSecond)) {
            streamConfig.objectRetentionTimeInSecond(objectRetentionTimeInSecond);
        }
    }

    public static final class ObjectCompaction {
        private final Integer intervalMinutes;
        private final Long maxSizeBytes;
        private ObjectCompaction(Builder builder) {
            this.intervalMinutes = builder.intervalMinutes;
            this.maxSizeBytes = builder.maxSizeBytes;
        }
        public static Builder builder() {
            return new Builder();
        }
        public Optional<Integer> intervalMinutes() {
            return Optional.ofNullable(intervalMinutes);
        }
        public Optional<Long> maxSizeBytes() {
            return Optional.ofNullable(maxSizeBytes);
        }
        public static final class Builder {
            private Integer intervalMinutes;
            private Long maxSizeBytes;
            public Builder intervalMinutes(Integer intervalMinutes) {
                this.intervalMinutes = intervalMinutes;
                return this;
            }
            public Builder maxSizeBytes(Long maxSizeBytes) {
                this.maxSizeBytes = maxSizeBytes;
                return this;
            }
            public Builder applyMap(Object value) {
                if (!(value instanceof Map<?, ?> map)) {
                    throw new IllegalArgumentException("objectCompaction must be a map");
                }
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    Object v = entry.getValue();
                    if (Objects.isNull(v)) {
                        continue;
                    }
                    switch (key) {
                        case "intervalMinutes" -> intervalMinutes(toInt(v));
                        case "maxSizeBytes" -> maxSizeBytes(toLong(v));
                        default -> throw new IllegalArgumentException("unknown objectCompaction key: " + key);
                    }
                }
                return this;
            }
            public ObjectCompaction build() {
                return new ObjectCompaction(this);
            }
        }
    }

    public static final class StreamSetCompaction {
        private final Integer interval;
        private final Long cacheSize;
        private final Integer uploadConcurrency;
        private final Long splitSize;
        private final Integer forceSplitPeriod;
        private final Integer maxObjectNum;
        private StreamSetCompaction(Builder builder) {
            this.interval = builder.interval;
            this.cacheSize = builder.cacheSize;
            this.uploadConcurrency = builder.uploadConcurrency;
            this.splitSize = builder.splitSize;
            this.forceSplitPeriod = builder.forceSplitPeriod;
            this.maxObjectNum = builder.maxObjectNum;
        }
        public static Builder builder() {
            return new Builder();
        }
        public Optional<Integer> interval() {
            return Optional.ofNullable(interval);
        }
        public Optional<Long> cacheSize() {
            return Optional.ofNullable(cacheSize);
        }
        public Optional<Integer> uploadConcurrency() {
            return Optional.ofNullable(uploadConcurrency);
        }
        public Optional<Long> splitSize() {
            return Optional.ofNullable(splitSize);
        }
        public Optional<Integer> forceSplitPeriod() {
            return Optional.ofNullable(forceSplitPeriod);
        }
        public Optional<Integer> maxObjectNum() {
            return Optional.ofNullable(maxObjectNum);
        }
        public static final class Builder {
            private Integer interval;
            private Long cacheSize;
            private Integer uploadConcurrency;
            private Long splitSize;
            private Integer forceSplitPeriod;
            private Integer maxObjectNum;
            public Builder interval(Integer interval) {
                this.interval = interval;
                return this;
            }
            public Builder cacheSize(Long cacheSize) {
                this.cacheSize = cacheSize;
                return this;
            }
            public Builder uploadConcurrency(Integer uploadConcurrency) {
                this.uploadConcurrency = uploadConcurrency;
                return this;
            }
            public Builder splitSize(Long splitSize) {
                this.splitSize = splitSize;
                return this;
            }
            public Builder forceSplitPeriod(Integer forceSplitPeriod) {
                this.forceSplitPeriod = forceSplitPeriod;
                return this;
            }
            public Builder maxObjectNum(Integer maxObjectNum) {
                this.maxObjectNum = maxObjectNum;
                return this;
            }
            public Builder applyMap(Object value) {
                if (!(value instanceof Map<?, ?> map)) {
                    throw new IllegalArgumentException("streamSetCompaction must be a map");
                }
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    String key = String.valueOf(entry.getKey());
                    Object v = entry.getValue();
                    if (Objects.isNull(v)) {
                        continue;
                    }
                    switch (key) {
                        case "interval" -> interval(toInt(v));
                        case "cacheSize" -> cacheSize(toLong(v));
                        case "uploadConcurrency" -> uploadConcurrency(toInt(v));
                        case "splitSize" -> splitSize(toLong(v));
                        case "forceSplitPeriod" -> forceSplitPeriod(toInt(v));
                        case "maxObjectNum" -> maxObjectNum(toInt(v));
                        default -> throw new IllegalArgumentException("unknown streamSetCompaction key: " + key);
                    }
                }
                return this;
            }
            public StreamSetCompaction build() {
                return new StreamSetCompaction(this);
            }
        }
    }

    public static final class Builder {
        private ByteBufAllocPolicy allocPolicy;
        private Long walCacheSize;
        private Long walUploadThreshold;
        private Long walUploadIntervalMs;
        private Long blockCacheSize;
        private Integer splitSize;
        private Integer objectBlockSize;
        private Integer objectPartSize;
        private final ObjectCompaction.Builder objectCompaction = ObjectCompaction.builder();
        private final StreamSetCompaction.Builder streamSetCompaction = StreamSetCompaction.builder();
        private Integer maxStreamsPerSetObject;
        private Integer maxObjectsPerCommit;
        private Long networkBaselineBandwidth;
        private NetworkBandwidthMode networkBandwidthMode;
        private Integer refillPeriodMs;
        private Long objectRetentionTimeInSecond;
        public Builder allocPolicy(ByteBufAllocPolicy allocPolicy) {
            this.allocPolicy = allocPolicy;
            return this;
        }
        public Builder walCacheSize(Long walCacheSize) {
            this.walCacheSize = walCacheSize;
            return this;
        }
        public Builder walUploadThreshold(Long walUploadThreshold) {
            this.walUploadThreshold = walUploadThreshold;
            return this;
        }
        public Builder walUploadIntervalMs(Long walUploadIntervalMs) {
            this.walUploadIntervalMs = walUploadIntervalMs;
            return this;
        }
        public Builder blockCacheSize(Long blockCacheSize) {
            this.blockCacheSize = blockCacheSize;
            return this;
        }
        public Builder splitSize(Integer splitSize) {
            this.splitSize = splitSize;
            return this;
        }
        public Builder objectBlockSize(Integer objectBlockSize) {
            this.objectBlockSize = objectBlockSize;
            return this;
        }
        public Builder objectPartSize(Integer objectPartSize) {
            this.objectPartSize = objectPartSize;
            return this;
        }
        public ObjectCompaction.Builder objectCompaction() {
            return objectCompaction;
        }
        public StreamSetCompaction.Builder streamSetCompaction() {
            return streamSetCompaction;
        }
        public Builder maxStreamsPerSetObject(Integer maxStreamsPerSetObject) {
            this.maxStreamsPerSetObject = maxStreamsPerSetObject;
            return this;
        }
        public Builder maxObjectsPerCommit(Integer maxObjectsPerCommit) {
            this.maxObjectsPerCommit = maxObjectsPerCommit;
            return this;
        }
        public Builder networkBaselineBandwidth(Long bandwidth) {
            this.networkBaselineBandwidth = bandwidth;
            return this;
        }
        public Builder networkBandwidthMode(NetworkBandwidthMode mode) {
            this.networkBandwidthMode = mode;
            return this;
        }
        public Builder refillPeriodMs(Integer refillPeriodMs) {
            this.refillPeriodMs = refillPeriodMs;
            return this;
        }
        public Builder objectRetentionTimeInSecond(Long seconds) {
            this.objectRetentionTimeInSecond = seconds;
            return this;
        }
        public boolean apply(String key, Object value) {
            if (Objects.isNull(value)) {
                return true;
            }
            switch (key) {
                case "allocPolicy" -> allocPolicy(ByteBufAllocPolicy.valueOf(String.valueOf(value)));
                case "walCacheSize" -> walCacheSize(toLong(value));
                case "walUploadThreshold" -> walUploadThreshold(toLong(value));
                case "walUploadIntervalMs" -> walUploadIntervalMs(toLong(value));
                case "blockCacheSize" -> blockCacheSize(toLong(value));
                case "splitSize" -> splitSize(toInt(value));
                case "objectBlockSize" -> objectBlockSize(toInt(value));
                case "objectPartSize" -> objectPartSize(toInt(value));
                case "objectCompaction" -> objectCompaction.applyMap(value);
                case "streamSetCompaction" -> streamSetCompaction.applyMap(value);
                case "maxStreamsPerSetObject" -> maxStreamsPerSetObject(toInt(value));
                case "maxObjectsPerCommit" -> maxObjectsPerCommit(toInt(value));
                case "networkBaselineBandwidth" -> networkBaselineBandwidth(toLong(value));
                case "networkBandwidthMode" ->
                    networkBandwidthMode(NetworkBandwidthMode.valueOf(String.valueOf(value)));
                case "refillPeriodMs" -> refillPeriodMs(toInt(value));
                case "objectRetentionTimeInSecond" -> objectRetentionTimeInSecond(toLong(value));
                default -> {
                    return false;
                }
            }
            return true;
        }
        public StreamConfig build() {
            return new StreamConfig(this);
        }
    }

    private static int toInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(String.valueOf(value));
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }
}
