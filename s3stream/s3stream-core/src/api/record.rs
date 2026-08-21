//! The host-facing record batch (what callers append).
//!
//! (count, baseTimestamp, properties, payload).

use std::collections::HashMap;

use bytes::Bytes;

/// A batch of records supplied by the host. Opaque to the engine except for `count`.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// Number of logical records in the payload.
    pub count: u32,
    /// Minimum timestamp (ms) of the records.
    pub base_timestamp_ms: i64,
    /// Extension properties.
    pub properties: HashMap<String, String>,
    /// The raw payload (host-defined encoding).
    pub payload: Bytes,
}

impl RecordBatch {
    pub fn new(count: u32, base_timestamp_ms: i64, payload: Bytes) -> Self {
        Self {
            count,
            base_timestamp_ms,
            properties: HashMap::new(),
            payload,
        }
    }
}
