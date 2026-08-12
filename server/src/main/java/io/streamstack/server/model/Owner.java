package io.streamstack.server.model;

import java.util.Objects;
import java.util.OptionalLong;

public record Owner(
    OptionalLong streamId,
    boolean local,
    Integer ownerNodeId,
    String ownerAdvertisedAddress) {

    public Owner {
        streamId = Objects.isNull(streamId) ? OptionalLong.empty() : streamId;
    }

    public static Owner local(OptionalLong streamId) {
        return new Owner(streamId, true, null, null);
    }

    public static Owner remote(long streamId, int ownerNodeId, String ownerAdvertisedAddress) {
        return new Owner(OptionalLong.of(streamId), false, ownerNodeId, ownerAdvertisedAddress);
    }
}
