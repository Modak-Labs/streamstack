use thiserror::Error;

/// WAL errors.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("over capacity: {unconfirmed_bytes} unconfirmed bytes >= cap {cap_bytes}")]
    OverCapacity {
        unconfirmed_bytes: u64,
        cap_bytes: u64,
    },

    #[error("fenced: our epoch {our_epoch} superseded (nodeId={node_id})")]
    Fenced { node_id: u32, our_epoch: u64 },

    #[error("WAL not started")]
    NotInitialized,

    #[error("record too large: {size} > {max}")]
    RecordTooLarge { size: u64, max: u64 },

    #[error("recovery: {0}")]
    Recovery(String),

    #[error("WAL shut down")]
    Shutdown,

    #[error("unmarshal: {0}")]
    Unmarshal(#[from] s3stream_codec::CodecError),

    #[error("object storage: {0}")]
    Storage(#[from] s3stream_object::ObjectError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
