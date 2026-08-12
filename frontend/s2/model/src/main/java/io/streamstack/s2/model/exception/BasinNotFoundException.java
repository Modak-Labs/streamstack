package io.streamstack.s2.model.exception;

public final class BasinNotFoundException extends S2Exception {

    private final String basin;

    public BasinNotFoundException(String basin) {
        super(404, "basin_not_found", "basin " + basin + " not found");
        this.basin = basin;
    }

    public String basin() {
        return basin;
    }

    @Override
    public String resource() {
        return basin;
    }
}
