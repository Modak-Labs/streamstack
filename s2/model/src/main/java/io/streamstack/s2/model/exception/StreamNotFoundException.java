package io.streamstack.s2.model.exception;

public final class StreamNotFoundException extends S2Exception {

    private final String stream;

    public StreamNotFoundException(String stream) {
        super(404, "stream_not_found", "stream " + stream + " not found");
        this.stream = stream;
    }

    public String stream() {
        return stream;
    }

    @Override
    public String resource() {
        return stream;
    }
}
