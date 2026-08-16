package io.streamstack.server.service;

import io.streamstack.server.model.CloseResult;
import io.streamstack.server.model.CreateCommand;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.StreamList;
import io.streamstack.server.model.StreamMeta;
import io.streamstack.server.model.StreamServiceException;

import java.util.Optional;

public interface StreamLifecycleService {

    CreateResult create(CreateCommand command) throws StreamServiceException;

    Optional<StreamMeta> head(String name) throws StreamServiceException;

    StreamList list(String prefix, String startAfter, int limit) throws StreamServiceException;

    CloseResult close(String name) throws StreamServiceException;

    boolean delete(String name) throws StreamServiceException;

    long trim(String name, long newStartOffset) throws StreamServiceException;
}
