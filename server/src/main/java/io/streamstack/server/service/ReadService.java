package io.streamstack.server.service;

import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface ReadService {
    ReadResult read(String name, OffsetToken from, int maxBytes, int maxRecords) throws StreamServiceException;

    boolean await(String name, OffsetToken from, Duration timeout) throws StreamServiceException;

    CompletableFuture<Boolean> whenAppended(String name, OffsetToken from, Duration timeout);
}
