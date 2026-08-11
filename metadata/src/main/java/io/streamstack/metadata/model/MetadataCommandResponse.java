package io.streamstack.metadata.model;

import java.io.Serializable;

public final class MetadataCommandResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final byte OK = 0;
    public static final byte NOT_LEADER = 1;
    public static final byte METADATA_ERROR = 2;
    public static final byte RAFT_ERROR = 3;

    private byte status;
    private String leaderId;
    private int errorCode;
    private String errorMessage;
    private byte[] result;

    public MetadataCommandResponse() {
    }

    public static MetadataCommandResponse ok(byte[] result) {
        MetadataCommandResponse response = new MetadataCommandResponse();
        response.status = OK;
        response.result = result;
        return response;
    }

    public static MetadataCommandResponse notLeader(String leaderId) {
        MetadataCommandResponse response = new MetadataCommandResponse();
        response.status = NOT_LEADER;
        response.leaderId = leaderId;
        return response;
    }

    public static MetadataCommandResponse metadataError(int errorCode, String errorMessage) {
        MetadataCommandResponse response = new MetadataCommandResponse();
        response.status = METADATA_ERROR;
        response.errorCode = errorCode;
        response.errorMessage = errorMessage;
        return response;
    }

    public static MetadataCommandResponse raftError(String errorMessage) {
        MetadataCommandResponse response = new MetadataCommandResponse();
        response.status = RAFT_ERROR;
        response.errorMessage = errorMessage;
        return response;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public byte[] getResult() {
        return result;
    }

    public void setResult(byte[] result) {
        this.result = result;
    }
}
