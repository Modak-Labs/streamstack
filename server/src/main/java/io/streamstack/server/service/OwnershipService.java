package io.streamstack.server.service;

import io.streamstack.server.model.NodeMeta;
import io.streamstack.server.model.Owner;
import io.streamstack.server.model.StreamServiceException;

public interface OwnershipService {

    Owner ownerOf(String name) throws StreamServiceException;

    NodeMeta localNode();
}
