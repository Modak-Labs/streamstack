package io.streamstack.client.model;

import io.streamstack.model.exception.DurableStreamException;

import java.util.function.Consumer;

public final class ProducerConfig {

    private final long epoch;
    private final long startingSeq;
    private final boolean autoClaim;
    private final int maxBatchBytes;
    private final long lingerMs;
    private final int maxInFlight;
    private final String contentType;
    private final Consumer<DurableStreamException> onError;

    private ProducerConfig(Builder builder) {
        this.epoch = builder.epoch;
        this.startingSeq = builder.startingSeq;
        this.autoClaim = builder.autoClaim;
        this.maxBatchBytes = builder.maxBatchBytes;
        this.lingerMs = builder.lingerMs;
        this.maxInFlight = builder.maxInFlight;
        this.contentType = builder.contentType;
        this.onError = builder.onError;
    }

    public static ProducerConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public long epoch() {
        return epoch;
    }

    public long startingSeq() {
        return startingSeq;
    }

    public boolean autoClaim() {
        return autoClaim;
    }

    public int maxBatchBytes() {
        return maxBatchBytes;
    }

    public long lingerMs() {
        return lingerMs;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    public String contentType() {
        return contentType;
    }

    public Consumer<DurableStreamException> onError() {
        return onError;
    }

    public static final class Builder {
        private long epoch;
        private long startingSeq;
        private boolean autoClaim;
        private int maxBatchBytes = 1024 * 1024;
        private long lingerMs = 5;
        private int maxInFlight = 5;
        private String contentType;
        private Consumer<DurableStreamException> onError;
        public Builder epoch(long epoch) {
            this.epoch = epoch;
            return this;
        }
        public Builder startingSeq(long startingSeq) {
            this.startingSeq = startingSeq;
            return this;
        }
        public Builder autoClaim(boolean autoClaim) {
            this.autoClaim = autoClaim;
            return this;
        }
        public Builder maxBatchBytes(int maxBatchBytes) {
            this.maxBatchBytes = maxBatchBytes;
            return this;
        }
        public Builder lingerMs(long lingerMs) {
            this.lingerMs = lingerMs;
            return this;
        }
        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }
        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }
        public Builder onError(Consumer<DurableStreamException> onError) {
            this.onError = onError;
            return this;
        }
        public ProducerConfig build() {
            if (maxBatchBytes <= 0) {
                throw new IllegalArgumentException("maxBatchBytes must be > 0");
            }
            if (maxInFlight <= 0) {
                throw new IllegalArgumentException("maxInFlight must be > 0");
            }
            if (lingerMs < 0) {
                throw new IllegalArgumentException("lingerMs must be >= 0");
            }
            return new ProducerConfig(this);
        }
    }
}
