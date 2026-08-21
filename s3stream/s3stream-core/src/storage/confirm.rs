//! ConfirmWAL: the WAL view handed to higher layers (snapshot-read cache, link
//! writers). Confirm offset, append notifications, and lazy-commit routing.

use std::sync::{Arc, RwLock};

use futures::future::BoxFuture;

use s3stream_codec::StreamRecordBatch;
use s3stream_wal::{RecordOffset, WriteAheadLog};

use crate::api::StreamError;

/// Commit trigger with linger semantics: if no other commit happens within
/// `lazy_linger_ms`, trigger one (0 = force upload as soon as possible).
#[derive(Debug, Clone, Copy)]
pub struct LazyCommit {
    pub lazy_linger_ms: u64,
    /// Resolve the caller only after the WAL trim completes (vs at commit).
    pub await_trim: bool,
}

/// Observer of confirmed appends, invoked in confirm order.
pub trait AppendListener: Send + Sync {
    fn on_append(&self, record: &StreamRecordBatch, offset: RecordOffset, next: RecordOffset);
}

/// Routes lazy-commit requests into S3Storage's upload machinery.
pub type CommitHandle =
    Arc<dyn Fn(LazyCommit) -> BoxFuture<'static, Result<(), StreamError>> + Send + Sync>;

pub struct ConfirmWal {
    wal: Arc<dyn WriteAheadLog>,
    commit_handle: CommitHandle,
    listeners: RwLock<Vec<Arc<dyn AppendListener>>>,
}

impl ConfirmWal {
    pub fn new(wal: Arc<dyn WriteAheadLog>, commit_handle: CommitHandle) -> Self {
        Self {
            wal,
            commit_handle,
            listeners: RwLock::new(Vec::new()),
        }
    }

    pub fn confirm_offset(&self) -> RecordOffset {
        self.wal.confirm_offset()
    }

    pub fn uri(&self) -> String {
        self.wal.uri().to_string()
    }

    /// Commit with lazy timeout: if within `[0, lazy_linger_ms)` no other commit
    /// happened, trigger a new one.
    pub async fn commit(&self, lazy_linger_ms: u64, await_trim: bool) -> Result<(), StreamError> {
        (self.commit_handle)(LazyCommit {
            lazy_linger_ms,
            await_trim,
        })
        .await
    }

    /// (drop the returned guard's id via
    /// `remove_append_listener` to unsubscribe).
    pub fn add_append_listener(&self, listener: Arc<dyn AppendListener>) {
        self.listeners
            .write()
            .expect("listeners poisoned")
            .push(listener);
    }

    /// Fan an append confirmation out to listeners. Called from the WAL's in-order
    /// confirm path.
    pub fn on_append(&self, record: &StreamRecordBatch, offset: RecordOffset, next: RecordOffset) {
        for listener in self.listeners.read().expect("listeners poisoned").iter() {
            listener.on_append(record, offset, next);
        }
    }
}
