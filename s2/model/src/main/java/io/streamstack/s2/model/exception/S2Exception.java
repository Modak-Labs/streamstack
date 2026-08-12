package io.streamstack.s2.model.exception;

import java.util.Objects;

public class S2Exception extends RuntimeException {

    private final int status;
    private final String code;

    public S2Exception(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public S2Exception(int status, String code, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.code = code;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }

    public String resource() {
        return null;
    }

    public static String rootMessage(Throwable t) {
        Throwable root = t;
        while (Objects.nonNull(root.getCause()) && root.getCause() != root) {
            root = root.getCause();
        }
        return Objects.isNull(root.getMessage()) ? t.toString() : root.getMessage();
    }

    public static BasinNotFoundException basinNotFound(String basin) {
        return new BasinNotFoundException(basin);
    }

    public static StreamNotFoundException streamNotFound(String stream) {
        return new StreamNotFoundException(stream);
    }

    public static ResourceAlreadyExistsException alreadyExists(String what) {
        return new ResourceAlreadyExistsException(what);
    }

    public static InvalidException invalid(String message) {
        return new InvalidException(message);
    }

    public static BadRequestException badJson(String message) {
        return new BadRequestException("bad_json", message);
    }

    public static BadRequestException badQuery(String message) {
        return new BadRequestException("bad_query", message);
    }

    public static BadRequestException badHeader(String message) {
        return new BadRequestException("bad_header", message);
    }

    public static SeqNumMismatchException seqNumMismatch(long actual) {
        return new SeqNumMismatchException(actual);
    }

    public static FencingTokenMismatchException fencingTokenMismatch(String actual) {
        return new FencingTokenMismatchException(actual);
    }

    public static S2Exception other(String message) {
        return new S2Exception(500, "other", message);
    }

    public static S2Exception unavailable(String message) {
        return new S2Exception(0, "unavailable", message);
    }
}
