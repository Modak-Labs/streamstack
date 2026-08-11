package io.streamstack.model;

public enum LiveMode {
    LONG_POLL(Protocol.LIVE_LONG_POLL),
    SSE(Protocol.LIVE_SSE);

    private final String wire;

    LiveMode(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    public static LiveMode parse(String raw) {
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        if (Protocol.LIVE_LONG_POLL.equals(raw)) {
            return LONG_POLL;
        }
        if (Protocol.LIVE_SSE.equals(raw)) {
            return SSE;
        }
        throw new IllegalArgumentException("invalid live mode");
    }
}
