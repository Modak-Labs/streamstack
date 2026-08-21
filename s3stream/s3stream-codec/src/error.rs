use thiserror::Error;

/// Errors produced while encoding/decoding wire formats.
///
/// `StreamRecordBatch#parse` and `UnmarshalException` from WAL-side decoding. The Rust
/// port unifies both under a typed error.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("invalid magic: expected {expected:#x}, got {actual:#x}")]
    InvalidMagic { expected: u64, actual: u64 },

    #[error("buffer too short: need {need} bytes, have {have}")]
    BufferTooShort { need: usize, have: usize },

    #[error("crc mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { expected: u32, computed: u32 },
}
