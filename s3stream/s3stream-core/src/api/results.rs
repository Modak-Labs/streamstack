//! Append/fetch results.
//!

use std::collections::HashMap;

use bytes::Bytes;

/// Result of a durable append.
#[derive(Debug, Clone, Copy)]
pub struct AppendResult {
    /// Base offset assigned to the appended batch.
    pub base_offset: u64,
}

/// A submitted-but-not-yet-durable append: the offset is fixed, durability is
/// pending.
///
/// Produced by [`crate::api::Stream::submit_append`]. Lets callers
/// pipeline appends: submit several in order, then await durability together,
/// so queued appends share WAL group commits instead of paying one flush
/// round-trip each.
///
/// Cancel-safe by construction: the engine completes the append (confirm
/// offset advance, fencing on error) in a detached task, so dropping this
/// handle abandons only the *notification*, never the bookkeeping.
#[derive(Debug)]
pub struct PendingAppend {
    base_offset: u64,
    durable: tokio::sync::oneshot::Receiver<Result<AppendResult, crate::api::StreamError>>,
}

impl PendingAppend {
    pub fn new(
        base_offset: u64,
        durable: tokio::sync::oneshot::Receiver<Result<AppendResult, crate::api::StreamError>>,
    ) -> Self {
        Self {
            base_offset,
            durable,
        }
    }

    /// Base offset reserved at submit time (records land here, in submit order).
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Resolve when the appended records are durable (WAL-confirmed).
    pub async fn durable(self) -> Result<AppendResult, crate::api::StreamError> {
        self.durable
            .await
            .map_err(|_| crate::api::StreamError::Unexpected("append completer dropped".into()))?
    }
}

/// One fetched batch with its stream context. Payload slices are zero-copy
/// views into cache blocks / GET responses.
#[derive(Debug, Clone)]
pub struct RecordBatchWithContext {
    pub stream_id: u64,
    pub base_offset: u64,
    /// Exclusive last offset.
    pub last_offset: u64,
    pub count: u32,
    pub properties: HashMap<String, String>,
    pub payload: Bytes,
}

/// Result of a fetch. Buffers are `Bytes`, so release is automatic on drop.
/// `CacheAccessType` is kept for observability.
#[derive(Debug)]
pub struct FetchResult {
    pub records: Vec<RecordBatchWithContext>,
    pub cache_access: CacheAccessType,
}

/// Where a fetch was served from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAccessType {
    /// Served entirely from the delta WAL LogCache.
    DeltaWalCacheHit,
    BlockCacheHit,
    BlockCacheMiss,
}
