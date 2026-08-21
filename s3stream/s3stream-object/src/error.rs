use thiserror::Error;

/// Errors from object format encoding/decoding and object storage operations.
///
/// (`s3.ObjectReader`), and operator-level exceptions from `s3.operator.*`. Unified here.
#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("codec: {0}")]
    Codec(#[from] s3stream_codec::CodecError),

    #[error("object not found: {key}")]
    NotFound { key: String },

    #[error("invalid object format: {reason}")]
    InvalidFormat { reason: String },

    #[error("writer ordering violation: {reason}")]
    OrderingViolation { reason: String },

    #[error("storage backend: {0}")]
    Backend(#[from] object_store::Error),

    #[error("timed out: {key} (last error: {last})")]
    Timeout { key: String, last: String },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

impl ObjectError {
    /// True when the error means "this object/key no longer exists". Either our own
    /// `NotFound` or the backend's (e.g. S3 `NoSuchKey`). The read path treats these
    /// as recoverable after object compaction.
    pub fn is_not_found(&self) -> bool {
        match self {
            ObjectError::NotFound { .. } => true,
            ObjectError::Backend(e) => matches!(e, object_store::Error::NotFound { .. }),
            _ => false,
        }
    }
}
