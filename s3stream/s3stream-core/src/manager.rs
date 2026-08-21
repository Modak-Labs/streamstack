//! The metadata-plane side door: traits the HOST implements.
//!
//!
//! Contract semantics are part of the recovery protocol (specification/upload-protocol.md).
//! Implement them exactly.

use std::collections::HashMap;

use async_trait::async_trait;

use s3stream_object::{ObjectStreamRange, S3ObjectMetadata};

use crate::api::StreamError;

/// Stream metadata as tracked by the metadata plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    pub stream_id: u64,
    pub epoch: u64,
    pub start_offset: u64,
    /// Committed end offset (recovery filters WAL records below this).
    pub end_offset: u64,
    pub state: StreamState,
    /// Owner node id. Used by snapshot-read `RequestCommitEvent`.
    pub node_id: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    Closed,
    Opened,
}

/// A stream object descriptor inside a commit request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamObject {
    pub object_id: u64,
    pub object_size: u64,
    pub stream_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub attributes: u32,
}

/// Commit request for one delta-WAL upload (or one stream-set compaction).
/// `object_id == NOOP_OBJECT_ID` means no stream set object was produced
/// (everything split into stream objects).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitStreamSetObjectRequest {
    pub object_id: u64,
    pub object_size: u64,
    pub attributes: u32,
    /// Ranges inside the stream set object.
    pub stream_ranges: Vec<ObjectStreamRange>,
    /// Split stream objects committed atomically with it.
    pub stream_objects: Vec<StreamObject>,
    /// Objects consumed by compaction (empty for delta uploads).
    pub compacted_object_ids: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct CommitStreamSetObjectResponse {}

/// Commit request for stream-object compaction.
///
/// `object_id == NOOP_OBJECT_ID` means
/// pure cleanup (no replacement object). `operations[i]` describes how
/// `source_object_ids[i]` is disposed (delete / keep data / deep delete).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactStreamObjectRequest {
    pub object_id: u64,
    pub object_size: u64,
    pub stream_id: u64,
    pub stream_epoch: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub source_object_ids: Vec<u64>,
    /// (parallel to
    /// `source_object_ids`).
    pub operations: Vec<crate::compact::CompactOperations>,
    pub attributes: u32,
}

/// Object metadata registry (host-implemented).
///
/// Contract:
/// - `prepare_object` leases `count` consecutive ids. Ids not committed within `ttl_ms`
///   may be garbage-collected.
/// - `commit_stream_set_object` is atomic: all ranges/objects in the request become
///   visible together. Commits from this node must be applied in call order.
/// - `get_objects` returns logical slices covering `[start_offset, end_offset)` of the
///   stream, continuous, in order (a physical object may appear multiple times).
/// - `get_server_objects` returns this node's stream set objects (recovery input).
/// - `get_stream_objects` returns stream objects only, ascending, possibly
///   discontinuous.
#[async_trait]
pub trait ObjectManager: Send + Sync {
    async fn prepare_object(&self, count: usize, ttl_ms: u64) -> Result<u64, StreamError>;

    async fn commit_stream_set_object(
        &self,
        request: CommitStreamSetObjectRequest,
    ) -> Result<CommitStreamSetObjectResponse, StreamError>;

    async fn compact_stream_object(
        &self,
        request: CompactStreamObjectRequest,
    ) -> Result<(), StreamError>;

    async fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError>;

    async fn get_server_objects(&self) -> Result<Vec<S3ObjectMetadata>, StreamError>;

    async fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError>;

    async fn is_object_exist(&self, object_id: u64) -> Result<bool, StreamError>;
}

/// Stream registry (host-implemented).
///
/// Contract:
/// - `open_stream(stream_id, epoch)` bumps the epoch (fencing older writers), closes
///   the previous range, and opens a new one on this node.
/// - `get_opening_streams` returns streams open on THIS node (recovery input).
/// - `close_stream` releases ownership so another node can open with a newer epoch.
#[async_trait]
pub trait StreamManager: Send + Sync {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError>;

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError>;

    async fn create_stream(&self, tags: HashMap<String, String>) -> Result<u64, StreamError>;

    async fn open_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        tags: HashMap<String, String>,
    ) -> Result<StreamMetadata, StreamError>;

    async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        new_start_offset: u64,
    ) -> Result<(), StreamError>;

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError>;

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError>;

    fn add_metadata_listener(
        &self,
        stream_id: u64,
        listener: std::sync::Arc<dyn StreamMetadataListener>,
    ) -> std::sync::Arc<dyn StreamMetadataListenerHandle> {
        let _ = (stream_id, listener);
        std::sync::Arc::new(NoopMetadataListenerHandle)
    }
}

/// Called after a stream-set object commit succeeds at the metadata plane.
///
/// Production use: the local stream
/// range index (`LocalStreamRangeIndexCache`) updates itself from every commit so
/// cold reads can skip objects without asking the metadata plane.
#[async_trait]
pub trait CommitStreamSetObjectHook: Send + Sync {
    async fn on_commit_success(
        &self,
        request: &CommitStreamSetObjectRequest,
    ) -> Result<(), StreamError>;
}

/// Called before a stream close is sent to the metadata plane.
///
/// Production use: flush the local range index
/// (rate-limited) so a node handing off a stream leaves a fresh index behind.
#[async_trait]
pub trait StreamCloseHook: Send + Sync {
    async fn before_stream_close(&self, stream_id: u64) -> Result<(), StreamError>;
}

/// `ObjectManager` decorator that fires a hook after each successful
/// stream-set-object commit.
///
/// (`ControllerObjectManager#commitStreamSetObject`:
/// `cf.thenAccept(resp -> commitStreamSetObjectHook.onCommitSuccess(request))`).
/// a decorator instead of `setCommitStreamSetObjectHook` mutation.
/// Dispatch semantics are identical (fire-and-forget, commit response does not wait
/// for the hook).
pub struct HookedObjectManager {
    inner: std::sync::Arc<dyn ObjectManager>,
    hook: std::sync::Arc<dyn CommitStreamSetObjectHook>,
}

impl HookedObjectManager {
    pub fn new(
        inner: std::sync::Arc<dyn ObjectManager>,
        hook: std::sync::Arc<dyn CommitStreamSetObjectHook>,
    ) -> Self {
        Self { inner, hook }
    }
}

#[async_trait]
impl ObjectManager for HookedObjectManager {
    async fn prepare_object(&self, count: usize, ttl_ms: u64) -> Result<u64, StreamError> {
        self.inner.prepare_object(count, ttl_ms).await
    }

    async fn commit_stream_set_object(
        &self,
        request: CommitStreamSetObjectRequest,
    ) -> Result<CommitStreamSetObjectResponse, StreamError> {
        let hook_request = request.clone();
        let response = self.inner.commit_stream_set_object(request).await?;
        let hook = std::sync::Arc::clone(&self.hook);
        tokio::spawn(async move {
            if let Err(e) = hook.on_commit_success(&hook_request).await {
                tracing::error!(error = %e, "commit stream set object hook failed");
            }
        });
        Ok(response)
    }

    async fn compact_stream_object(
        &self,
        request: CompactStreamObjectRequest,
    ) -> Result<(), StreamError> {
        self.inner.compact_stream_object(request).await
    }

    async fn get_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        self.inner
            .get_objects(stream_id, start_offset, end_offset, limit)
            .await
    }

    async fn get_server_objects(&self) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        self.inner.get_server_objects().await
    }

    async fn get_stream_objects(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        limit: usize,
    ) -> Result<Vec<S3ObjectMetadata>, StreamError> {
        self.inner
            .get_stream_objects(stream_id, start_offset, end_offset, limit)
            .await
    }

    async fn is_object_exist(&self, object_id: u64) -> Result<bool, StreamError> {
        self.inner.is_object_exist(object_id).await
    }
}

/// `StreamManager` decorator that runs a hook before each stream close.
/// Close proceeds whether the hook succeeds, fails, or times out.
pub struct HookedStreamManager {
    inner: std::sync::Arc<dyn StreamManager>,
    hook: std::sync::Arc<dyn StreamCloseHook>,
}

impl HookedStreamManager {
    pub fn new(
        inner: std::sync::Arc<dyn StreamManager>,
        hook: std::sync::Arc<dyn StreamCloseHook>,
    ) -> Self {
        Self { inner, hook }
    }
}

#[async_trait]
impl StreamManager for HookedStreamManager {
    async fn get_opening_streams(&self) -> Result<Vec<StreamMetadata>, StreamError> {
        self.inner.get_opening_streams().await
    }

    async fn get_streams(&self, stream_ids: &[u64]) -> Result<Vec<StreamMetadata>, StreamError> {
        self.inner.get_streams(stream_ids).await
    }

    async fn create_stream(&self, tags: HashMap<String, String>) -> Result<u64, StreamError> {
        self.inner.create_stream(tags).await
    }

    async fn open_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        tags: HashMap<String, String>,
    ) -> Result<StreamMetadata, StreamError> {
        self.inner.open_stream(stream_id, epoch, tags).await
    }

    async fn trim_stream(
        &self,
        stream_id: u64,
        epoch: u64,
        new_start_offset: u64,
    ) -> Result<(), StreamError> {
        self.inner
            .trim_stream(stream_id, epoch, new_start_offset)
            .await
    }

    async fn close_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        if let Err(e) = self.hook.before_stream_close(stream_id).await {
            tracing::warn!(stream_id, error = %e, "stream close hook failed; closing anyway");
        }
        self.inner.close_stream(stream_id, epoch).await
    }

    async fn delete_stream(&self, stream_id: u64, epoch: u64) -> Result<(), StreamError> {
        self.inner.delete_stream(stream_id, epoch).await
    }

    fn add_metadata_listener(
        &self,
        stream_id: u64,
        listener: std::sync::Arc<dyn StreamMetadataListener>,
    ) -> std::sync::Arc<dyn StreamMetadataListenerHandle> {
        // Must forward (not use the trait default): SNAPSHOT_READ registration goes
        // to the real manager.
        self.inner.add_metadata_listener(stream_id, listener)
    }
}

/// Push notification of stream metadata changes from the metadata plane.
///
/// (registered via
/// `StreamManager#addMetadataListener`). Production use: SNAPSHOT_READ streams
/// update confirm/start offsets from the owner node's metadata.
pub trait StreamMetadataListener: Send + Sync {
    fn on_new_stream_metadata(&self, metadata: StreamMetadata);
}

pub trait StreamMetadataListenerHandle: Send + Sync {
    fn close(&self);
}

/// `UnsupportedOperationException`. Protocol for hosts that implement registration is
/// unchanged. This default only exists so test stubs without snapshot-read do not
/// need an empty override.
struct NoopMetadataListenerHandle;

impl StreamMetadataListenerHandle for NoopMetadataListenerHandle {
    fn close(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::memory::MemoryMetadataManager;

    #[derive(Default)]
    struct RecordingHook {
        commits: Mutex<Vec<u64>>,
        closes: Mutex<Vec<u64>>,
        fail_close: bool,
    }

    #[async_trait]
    impl CommitStreamSetObjectHook for RecordingHook {
        async fn on_commit_success(
            &self,
            request: &CommitStreamSetObjectRequest,
        ) -> Result<(), StreamError> {
            self.commits.lock().unwrap().push(request.object_id);
            Ok(())
        }
    }

    #[async_trait]
    impl StreamCloseHook for RecordingHook {
        async fn before_stream_close(&self, stream_id: u64) -> Result<(), StreamError> {
            self.closes.lock().unwrap().push(stream_id);
            if self.fail_close {
                return Err(StreamError::Unexpected("hook failed".into()));
            }
            Ok(())
        }
    }

    /// `cf.thenAccept(resp -> hook.onCommitSuccess(request))`: the hook observes every
    /// successful commit without blocking the commit response.
    #[tokio::test]
    async fn hooked_object_manager_fires_on_commit_success() {
        let inner = MemoryMetadataManager::new();
        let hook = Arc::new(RecordingHook::default());
        let manager = HookedObjectManager::new(inner, Arc::clone(&hook) as _);

        let object_id = manager.prepare_object(1, 60_000).await.unwrap();
        manager
            .commit_stream_set_object(CommitStreamSetObjectRequest {
                object_id,
                object_size: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if hook.commits.lock().unwrap().as_slice() == [object_id] {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "hook never fired");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn hooked_stream_manager_closes_despite_hook_failure() {
        let inner = MemoryMetadataManager::new();
        let hook = Arc::new(RecordingHook {
            fail_close: true,
            ..Default::default()
        });
        let manager = HookedStreamManager::new(inner.clone(), Arc::clone(&hook) as _);

        let stream_id = manager.create_stream(HashMap::new()).await.unwrap();
        manager
            .open_stream(stream_id, 1, HashMap::new())
            .await
            .unwrap();
        manager.close_stream(stream_id, 1).await.unwrap();

        assert_eq!(hook.closes.lock().unwrap().as_slice(), [stream_id]);
        // The inner manager actually closed the stream.
        let metadata = inner.get_streams(&[stream_id]).await.unwrap();
        assert_eq!(metadata[0].state, StreamState::Closed);
    }
}
