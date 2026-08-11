package io.streamstack.metadata.model;

import java.io.Serializable;

public final class MetadataCommandRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private byte[] command;

    public MetadataCommandRequest() {
    }

    public MetadataCommandRequest(byte[] command) {
        this.command = command;
    }

    public byte[] getCommand() {
        return command;
    }

    public void setCommand(byte[] command) {
        this.command = command;
    }
}
