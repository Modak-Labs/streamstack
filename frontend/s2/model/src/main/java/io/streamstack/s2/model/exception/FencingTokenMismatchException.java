package io.streamstack.s2.model.exception;

import java.util.Objects;

public final class FencingTokenMismatchException extends S2Exception {

    private final String actualToken;

    public FencingTokenMismatchException(String actualToken) {
        super(412, "fencing_token_mismatch",
            "fencing token mismatch: actual=" + (Objects.isNull(actualToken) ? "" : actualToken));
        this.actualToken = Objects.isNull(actualToken) ? "" : actualToken;
    }

    public String actualToken() {
        return actualToken;
    }
}
