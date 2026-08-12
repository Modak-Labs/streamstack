package io.streamstack.server.service;

import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;

public interface OwnershipService {

    Owner ownerOf(String name) throws StreamServiceException;
    NodeMeta localNode();
}
