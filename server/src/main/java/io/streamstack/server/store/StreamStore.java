package io.streamstack.server.store;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public interface StreamStore {
    CreateResult create(URI url, String contentType, Long ttlSeconds, Instant expiresAt, InputStream initialBody)
        throws StoreException;

    OffsetToken append(URI url, String contentType, InputStream body) throws StoreException;

    OffsetToken close(URI url) throws StoreException;

    boolean delete(URI url) throws StoreException;

    Optional<StreamInfo> head(URI url) throws StoreException;

    ReadResult read(URI url, OffsetToken startOffset, int maxBytes) throws StoreException;

    boolean await(URI url, OffsetToken startOffset, Duration timeout) throws StoreException;
}
