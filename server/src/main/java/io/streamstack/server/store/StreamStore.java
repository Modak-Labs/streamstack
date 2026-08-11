package io.streamstack.server.store;

import io.streamstack.server.model.AppendCommand;
import io.streamstack.server.model.AppendResult;
import io.streamstack.server.model.CreateResult;
import io.streamstack.server.model.OffsetToken;
import io.streamstack.server.model.ReadResult;
import io.streamstack.server.model.StreamInfo;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public interface StreamStore {
    CreateResult create(
        URI url,
        String contentType,
        Long ttlSeconds,
        Instant expiresAt,
        boolean closed,
        InputStream initialBody) throws StoreException;

    AppendResult append(URI url, AppendCommand request) throws StoreException;

    OffsetToken close(URI url) throws StoreException;

    boolean delete(URI url) throws StoreException;

    Optional<StreamInfo> head(URI url) throws StoreException;

    ReadResult read(URI url, OffsetToken startOffset, int maxBytes) throws StoreException;

    boolean await(URI url, OffsetToken startOffset, Duration timeout) throws StoreException;
}
