//! Write-ahead log abstraction and implementations.
//!
//! Specification: `specification/wal-protocol.md`.
//!
//! The WAL is a per-node, single-writer, epoch-fenced durability buffer. v1 ships the
//! object WAL (`object` module). The trait keeps the slot open for a block-device WAL.
//! `memory` provides an in-process implementation for tests and emulation.

pub mod error;
pub mod factory;
pub mod memory;
pub mod object;

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use s3stream_codec::StreamRecordBatch;

pub use error::WalError;
pub use factory::{
    AcquirePermissionOptions, BuildOptions, DefaultWalHandle, ObjectWalFactory, WalFactory,
    WalHandle,
};

/// Position of one record in the WAL's logical address space: epoch, offset,
/// and size. Serialized with magic `0xA8` when it crosses process boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordOffset {
    /// Node epoch that wrote the record.
    pub epoch: u64,
    /// Logical byte offset in the WAL address space.
    pub offset: u64,
    /// Framed size (header + body) in bytes.
    pub size: u32,
}

impl RecordOffset {
    /// End position of the record: `offset + size`. Used as the next append position
    /// and as the exclusive bound for trim/recovery bookkeeping.
    pub fn end_offset(&self) -> u64 {
        self.offset + self.size as u64
    }
}

/// Result of a WAL append. S3Storage consumes the record offset and the
/// next offset.
#[derive(Debug, Clone, Copy)]
pub struct AppendResult {
    /// Where this record landed.
    pub record_offset: RecordOffset,
    /// The next append position after this record.
    pub next_offset: RecordOffset,
}

/// A record already placed in the log, not yet durable.
///
/// `WriteAheadLog#append`. Splitting placement from durability is what lets a
/// caller keep log order while many appends are in flight.
pub struct PendingAppend {
    pub durable: futures::future::BoxFuture<'static, Result<AppendResult, WalError>>,
}

/// One recovered record.
pub struct RecoverResult {
    pub record: StreamRecordBatch,
    pub record_offset: RecordOffset,
}

/// Callback invoked when a record is confirmed durable, in confirm (offset) order.
///
/// `ConfirmWAL#onAppend` run on the WAL's single callback thread in bulk-completion
/// explicitly. Implementations call it from their ordered completion path *before*
/// resolving the append future. Arguments: (record, record_offset, next_offset).
pub type AppendListener =
    std::sync::Arc<dyn Fn(&StreamRecordBatch, RecordOffset, RecordOffset) + Send + Sync>;

/// WAL open mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenMode {
    /// Owner: read + write (normal operation).
    #[default]
    ReadWrite,
    /// Failover: read another node's WAL without writing.
    Recovery,
}

/// Identity of a WAL instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalMetadata {
    pub node_id: u32,
    pub epoch: u64,
}

/// Verifies the caller's (nodeId, epoch) reservation with the metadata plane before the
/// WAL accepts writes. A stale writer is fenced.
///
/// (object WAL uses `ObjectReservationService` which
/// CAS-writes a reservation object in the bucket).
#[async_trait]
pub trait ReservationService: Send + Sync {
    /// Acquire/refresh the reservation for (node_id, epoch).`failover` marks a
    /// recovery-mode acquisition.
    async fn acquire(&self, node_id: u32, epoch: u64, failover: bool) -> Result<(), WalError>;

    /// Verify the reservation still holds (fencing check). The `failover` flag is part
    /// of the stored reservation bytes, so verification must present the same flag it
    async fn verify(&self, node_id: u32, epoch: u64, failover: bool) -> Result<bool, WalError>;
}

/// No-op reservation service for tests.
pub struct NoopReservationService;

#[async_trait]
impl ReservationService for NoopReservationService {
    async fn acquire(&self, _: u32, _: u64, _: bool) -> Result<(), WalError> {
        Ok(())
    }
    async fn verify(&self, _: u32, _: u64, _: bool) -> Result<bool, WalError> {
        Ok(true)
    }
}

/// Stream of recovered records, in WAL order. Async because recovery reads
/// objects over the network.
pub type RecoverStream = Pin<Box<dyn Stream<Item = Result<RecoverResult, WalError>> + Send>>;

/// The write-ahead log contract.
///
/// Contract invariants (see specification/wal-protocol.md):
/// - An append future completing successfully means the record is durable.
/// - `confirm_offset` is contiguous: everything below it is durable, no holes.
/// - Appends may *complete* out of order but confirm in order.
/// - `trim(offset)` releases `data <= offset` (inclusive). Never exceeds confirm.
/// - `recover()` yields exactly the durable suffix after the trim offset, in order,
///   stopping cleanly at the first torn write.
/// - A fenced instance fails all writes with `WalError::Fenced`.
#[async_trait]
pub trait WriteAheadLog: Send + Sync {
    /// Start the WAL (verify reservation, load trim/recover point).
    async fn start(&self) -> Result<(), WalError>;

    /// Graceful shutdown: flush pending batches, stop background tasks.
    async fn shutdown_gracefully(&self);

    fn metadata(&self) -> WalMetadata;

    /// Config URI this WAL was built from (reconstructable).
    fn uri(&self) -> &str;

    /// Place one record in the log, returning a future that completes when it is
    /// durable. Fails fast with `WalError::OverCapacity` when unconfirmed bytes
    /// exceed the configured cap (caller backs off and force-uploads).
    ///
    /// Callers get their log positions in call order, which is why this is not
    /// `async`: a caller that needs two records adjacent in the log only has to
    /// call this twice in sequence, with no await in between where another
    /// caller's record could be interleaved. Durability still completes out of
    /// order. A full WAL surfaces as [`WalError::OverCapacity`].
    fn submit(&self, record: StreamRecordBatch) -> Result<PendingAppend, WalError>;

    /// Append one record and wait for durability.
    ///
    /// Order between concurrent callers of this method is undefined (awaiting
    /// is the interleaving point), so anything order-sensitive must use
    /// [`Self::submit`].
    async fn append(&self, record: StreamRecordBatch) -> Result<AppendResult, WalError> {
        self.submit(record)?.durable.await
    }

    /// Register the in-order confirm callback (see [`AppendListener`]). At most one
    /// listener. Setting replaces the previous one.
    fn set_append_listener(&self, listener: AppendListener);

    /// Read back one record by offset (used by snapshot-read / confirm WAL paths).
    async fn get(&self, offset: RecordOffset) -> Result<StreamRecordBatch, WalError>;

    /// Read back records in `[start, end)`.
    async fn get_range(
        &self,
        start: RecordOffset,
        end: RecordOffset,
    ) -> Result<Vec<StreamRecordBatch>, WalError>;

    /// Highest contiguous durable position.
    fn confirm_offset(&self) -> RecordOffset;

    /// Recover the durable suffix from the trim offset onward.
    fn recover(&self) -> RecoverStream;

    /// Trim everything: equivalent to trim(end-of-log). Called after recovery upload.
    async fn reset(&self) -> Result<(), WalError>;

    /// Release `data <= offset`.
    async fn trim(&self, offset: RecordOffset) -> Result<(), WalError>;
}
