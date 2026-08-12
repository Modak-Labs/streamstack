package io.streamstack.s2.model.exception;

public final class ResourceAlreadyExistsException extends S2Exception {

    private final String what;

    public ResourceAlreadyExistsException(String what) {
        super(409, "resource_already_exists", what + " already exists");
        this.what = what;
    }

    @Override
    public String resource() {
        return what;
    }
}
