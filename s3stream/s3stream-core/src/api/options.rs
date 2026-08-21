//! Stream create/open options.

use std::collections::HashMap;

/// (replicaCount is legacy Kafka-ism, always 1 on s3stream,
/// dropped).
#[derive(Debug, Clone, Default)]
pub struct CreateStreamOptions {
    /// Stream epoch to open with.
    pub epoch: u64,
    /// Tags forwarded to the metadata plane.
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct OpenStreamOptions {
    /// Epoch to open with. The metadata plane fences older epochs.
    pub epoch: u64,
    /// Read-only open (no write fencing performed).
    pub read_only: bool,
    /// Snapshot-read mode: this node serves reads for a stream owned elsewhere.
    ///
    /// (`READ_WRITE` vs `SNAPSHOT_READ`). A bool is the same two-state protocol.
    pub snapshot_read: bool,
    pub tags: HashMap<String, String>,
}
