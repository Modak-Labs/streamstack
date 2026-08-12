package io.streamstack.s2.model.exception;

public final class SeqNumMismatchException extends S2Exception {

    private final long actualSeqNum;

    public SeqNumMismatchException(long actualSeqNum) {
        super(412, "seq_num_mismatch", "seq_num mismatch: actual=" + actualSeqNum);
        this.actualSeqNum = actualSeqNum;
    }

    public long actualSeqNum() {
        return actualSeqNum;
    }
}
