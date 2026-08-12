package io.streamstack.metadata;

import io.streamstack.api.exceptions.ErrorCode;
import io.streamstack.api.exceptions.StreamClientException;

public final class MetadataException extends RuntimeException {

    public static final int STREAM_NOT_EXIST = 1;
    public static final int STREAM_NOT_CLOSED = 2;
    public static final int STREAM_FENCED = 3;
    public static final int EXPIRED_EPOCH = 4;
    public static final int NODE_EPOCH_MISMATCH = 5;
    public static final int REDUNDANT_OPERATION = 6;
    public static final int UNEXPECTED = 99;
    private final int code;

    public MetadataException(int code, String message) {
        super(message);
        this.code = code;
    }

    public MetadataException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public int code() {
        return code;
    }

    public boolean isRedundant() {
        return code == REDUNDANT_OPERATION;
    }

    public static boolean isRedundant(Throwable t) {
        return t instanceof MetadataException && ((MetadataException) t).isRedundant();
    }

    public static StreamClientException toStreamClientException(MetadataException e) {
        return new StreamClientException(toErrorCode(e.code()), e.getMessage(), e);
    }

    public static short toErrorCode(int metadataCode) {
        return switch (metadataCode) {
            case STREAM_NOT_EXIST -> ErrorCode.STREAM_NOT_EXIST;
            case STREAM_NOT_CLOSED -> ErrorCode.STREAM_NOT_CLOSED;
            case STREAM_FENCED, EXPIRED_EPOCH -> ErrorCode.EXPIRED_STREAM_EPOCH;
            case NODE_EPOCH_MISMATCH -> ErrorCode.UNEXPECTED;
            case REDUNDANT_OPERATION -> ErrorCode.UNEXPECTED;
            default -> ErrorCode.UNEXPECTED;
        };
    }

    public static MetadataException streamNotExist(long streamId) {
        return new MetadataException(STREAM_NOT_EXIST, "stream " + streamId + " not found");
    }

    public static MetadataException streamNotClosed(long streamId) {
        return new MetadataException(STREAM_NOT_CLOSED, "stream " + streamId + " is not closed");
    }

    public static MetadataException streamFenced(String message) {
        return new MetadataException(STREAM_FENCED, message);
    }

    public static MetadataException expiredEpoch(String message) {
        return new MetadataException(EXPIRED_EPOCH, message);
    }

    public static MetadataException nodeEpochMismatch(String message) {
        return new MetadataException(NODE_EPOCH_MISMATCH, message);
    }

    public static MetadataException redundant(String message) {
        return new MetadataException(REDUNDANT_OPERATION, message);
    }

    public static MetadataException unexpected(String message) {
        return new MetadataException(UNEXPECTED, message);
    }
}
