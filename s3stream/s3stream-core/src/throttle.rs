//! Network bandwidth throttling for object-storage traffic.
//!
//! NetworkBandwidthLimiter, ThrottleStrategy}`.
//!
//! - Token bucket: `available_tokens` starts at `token_size`. Every
//!   `refill_interval_ms` the bucket gains `token_size`, capped at `max_tokens`.
//! - `BYPASS` traffic **force-debits** the bucket (balance may go negative, floored
//!   at `-max_tokens`) and never waits. Throttled tiers pay the debt.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use s3stream_object::storage::ThrottleStrategy;
use s3stream_object::{
    MultipartWriter, ObjectError, ObjectInfo, ObjectPath, ObjectStorage, ReadOptions, WriteOptions,
    WriteResult,
};
use tokio::sync::oneshot;

const MAX_TOKEN_PART_SIZE: i64 = 1024 * 1024;

/// One queued acquisition.
///
/// (priority + nanoTime ordering,
/// `size` counts down as chunks are granted).
struct BucketItem {
    priority: u32,
    seq: u64,
    remaining: i64,
    tx: Option<oneshot::Sender<()>>,
}

impl PartialEq for BucketItem {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.seq == other.seq
    }
}
impl Eq for BucketItem {}

impl PartialOrd for BucketItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for BucketItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

/// Mutable limiter state. Callers must hold the throttle lock.
struct QueueState {
    queued: BinaryHeap<BucketItem>,
    next_seq: u64,
}

struct Shared {
    /// (`AtomicLong`, may go
    /// negative down to `-max_tokens`).
    available_tokens: AtomicI64,
    token_size: i64,
    max_tokens: i64,
    state: Mutex<QueueState>,
    shutdown: std::sync::atomic::AtomicBool,
}

/// Async token-bucket bandwidth limiter with priority tiers.
///
/// `NetworkBandwidthLimiter.NOOP` is expressed as `Option<Arc<BandwidthLimiter>>::None`
/// at the call sites (no trait object needed).
pub struct BandwidthLimiter {
    shared: Arc<Shared>,
    refill_task: tokio::task::JoinHandle<()>,
}

impl BandwidthLimiter {
    /// `maxTokens = tokenSize`.
    pub fn new(token_size: u64, refill_interval_ms: u64) -> Self {
        Self::with_max_tokens(token_size, refill_interval_ms, token_size)
    }

    pub fn with_max_tokens(token_size: u64, refill_interval_ms: u64, max_tokens: u64) -> Self {
        assert!(token_size > 0, "tokenSize must be positive");
        let shared = Arc::new(Shared {
            available_tokens: AtomicI64::new(token_size as i64),
            token_size: token_size as i64,
            max_tokens: max_tokens as i64,
            state: Mutex::new(QueueState {
                queued: BinaryHeap::new(),
                next_seq: 0,
            }),
            shutdown: std::sync::atomic::AtomicBool::new(false),
        });
        // One task does refill-then-drain per tick.
        let refill_task = {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_millis(refill_interval_ms.max(1)));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    if shared.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    shared.refill_and_drain();
                }
            })
        };
        Self {
            shared,
            refill_task,
        }
    }

    /// Acquire `bytes` of budget at `strategy` priority. Returns `true` when the
    /// request had to queue (used by [`MeteredBandwidthLimiter`] for queue-time).
    ///
    /// `Bypass` force-debits and returns immediately. Other tiers wait until the
    /// drain loop grants their full size (FIFO within priority).
    ///
    /// `!cf.isDone()`. Here `acquire` reports it directly. Protocol unchanged.
    pub async fn acquire(&self, strategy: ThrottleStrategy, bytes: u64) -> bool {
        let bytes = bytes as i64;
        if strategy == ThrottleStrategy::Bypass {
            self.shared.reduce_token(bytes);
            return false;
        }
        let rx = {
            let mut state = self.shared.state.lock().expect("limiter poisoned");
            if self.shared.available_tokens.load(Ordering::Acquire) <= 0 || !state.queued.is_empty()
            {
                let (tx, rx) = oneshot::channel();
                let seq = state.next_seq;
                state.next_seq += 1;
                state.queued.push(BucketItem {
                    priority: strategy.priority(),
                    seq,
                    remaining: bytes,
                    tx: Some(tx),
                });
                Some(rx)
            } else {
                self.shared.reduce_token(bytes);
                None
            }
        };
        match rx {
            Some(rx) => {
                let _ = rx.await;
                true
            }
            None => false,
        }
    }

    pub fn max_tokens(&self) -> i64 {
        self.shared.max_tokens
    }

    pub fn available_tokens(&self) -> i64 {
        self.shared.available_tokens.load(Ordering::Acquire)
    }

    pub fn queue_size(&self) -> usize {
        self.shared
            .state
            .lock()
            .expect("limiter poisoned")
            .queued
            .len()
    }

    pub fn shutdown(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.refill_task.abort();
        let mut state = self.shared.state.lock().expect("limiter poisoned");
        for mut item in state.queued.drain() {
            if let Some(tx) = item.tx.take() {
                let _ = tx.send(());
            }
        }
    }
}

impl Drop for BandwidthLimiter {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.refill_task.abort();
    }
}

/// Records directional network metrics while delegating throttling. After
/// every consume it counts
/// `kafka_stream_network_{in,out}bound_usage{type=<strategy>}`, records
/// `..._limiter_queue_time{type}` when the request queued, and publishes the
/// queue-size gauge. In SHARED mode both directions wrap the same underlying
pub struct MeteredBandwidthLimiter {
    direction: crate::metrics::Direction,
    inner: Arc<BandwidthLimiter>,
}

impl MeteredBandwidthLimiter {
    pub fn new(direction: crate::metrics::Direction, inner: Arc<BandwidthLimiter>) -> Self {
        Self { direction, inner }
    }

    pub async fn acquire(&self, strategy: ThrottleStrategy, bytes: u64) {
        let start = std::time::Instant::now();
        let queued = self.inner.acquire(strategy, bytes).await;
        crate::metrics::record_network_usage(self.direction, strategy, bytes);
        if queued {
            crate::metrics::record_network_limiter_queue_time(
                self.direction,
                strategy,
                start.elapsed().as_nanos() as i64,
            );
        }
        crate::metrics::set_network_limiter_queue_size(self.direction, self.inner.queue_size());
    }

    pub fn max_tokens(&self) -> i64 {
        self.inner.max_tokens()
    }

    pub fn available_tokens(&self) -> i64 {
        self.inner.available_tokens()
    }

    pub fn queue_size(&self) -> usize {
        self.inner.queue_size()
    }

    /// "the delegate owns its
    /// lifecycle": only the metric registration is released (a no-op here, the
    /// `metrics` facade has no unregister).
    pub fn shutdown(&self) {}
}

impl Shared {
    /// `availableTokens = max(-maxTokens, tokens - size)`.
    fn reduce_token(&self, size: i64) {
        self.available_tokens
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                Some((old - size).max(-self.max_tokens))
            })
            .expect("fetch_update closure always returns Some");
    }

    /// ` + signal) followed by
    /// the `#run` drain: while tokens are positive and the queue is non-empty, debit
    /// the head in 1 MiB chunks. Pop + complete when its remainder reaches 0.
    fn refill_and_drain(&self) {
        let mut state = self.state.lock().expect("limiter poisoned");
        self.available_tokens
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                Some((old + self.token_size).min(self.max_tokens))
            })
            .expect("fetch_update closure always returns Some");
        while self.available_tokens.load(Ordering::Acquire) > 0 {
            let Some(mut head) = state.queued.pop() else {
                break;
            };
            let chunk = head.remaining.min(MAX_TOKEN_PART_SIZE);
            self.reduce_token(chunk);
            head.remaining -= chunk;
            if head.remaining <= 0 {
                // → `cf.complete(null)`, item removed.
                if let Some(tx) = head.tx.take() {
                    let _ = tx.send(());
                }
            } else {
                state.queued.push(head);
            }
        }
    }
}

/// Whether inbound and outbound traffic draw from separate or one shared bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NetworkBandwidthMode {
    #[default]
    Separate,
    Shared,
}

impl NetworkBandwidthMode {
    pub fn name(self) -> &'static str {
        match self {
            NetworkBandwidthMode::Separate => "separate",
            NetworkBandwidthMode::Shared => "shared",
        }
    }

    pub fn parse(value: &str) -> Result<Self, crate::api::StreamError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "separate" => Ok(NetworkBandwidthMode::Separate),
            "shared" => Ok(NetworkBandwidthMode::Shared),
            other => Err(crate::api::StreamError::Unexpected(format!(
                "Unsupported network bandwidth mode: {other}"
            ))),
        }
    }
}

const OUTBOUND_MAX_TOKENS_MULTIPLIER: u64 = 5;
const SHARED_MAX_TOKENS_MULTIPLIER: u64 = 2;

/// Build the (inbound, outbound) limiter pair from the configured bandwidth mode.
///
/// `tokenSize = bandwidth * refillIntervalMs / 1000` (must be > 0).
/// SHARED: one physical limiter (`maxTokens = bandwidth * 2`) viewed from both
/// directions. SEPARATE: inbound `maxTokens = bandwidth`, outbound
/// `maxTokens = bandwidth * 5`.
///
/// Both directions come back wrapped in [`MeteredBandwidthLimiter`]. The
/// builder owns the pair and passes it to `ThrottledObjectStorage`.
pub fn build_network_limiters(
    mode: NetworkBandwidthMode,
    bandwidth: u64,
    refill_interval_ms: u64,
) -> Result<(Arc<MeteredBandwidthLimiter>, Arc<MeteredBandwidthLimiter>), crate::api::StreamError> {
    let token_size = (bandwidth as f64 * (refill_interval_ms as f64 / 1000.0)) as u64;
    if token_size == 0 {
        return Err(crate::api::StreamError::Unexpected(format!(
            "tokenSize must be greater than 0, bandwidth: {bandwidth}, refill period: {refill_interval_ms}ms"
        )));
    }
    let metered = |direction, limiter: &Arc<BandwidthLimiter>| {
        Arc::new(MeteredBandwidthLimiter::new(direction, Arc::clone(limiter)))
    };
    use crate::metrics::Direction;
    match mode {
        NetworkBandwidthMode::Shared => {
            // SHARED. One physical bucket, metered from both directions.
            let shared = Arc::new(BandwidthLimiter::with_max_tokens(
                token_size,
                refill_interval_ms,
                bandwidth * SHARED_MAX_TOKENS_MULTIPLIER,
            ));
            Ok((
                metered(Direction::Inbound, &shared),
                metered(Direction::Outbound, &shared),
            ))
        }
        NetworkBandwidthMode::Separate => {
            let inbound = Arc::new(BandwidthLimiter::with_max_tokens(
                token_size,
                refill_interval_ms,
                bandwidth,
            ));
            let outbound = Arc::new(BandwidthLimiter::with_max_tokens(
                token_size,
                refill_interval_ms,
                bandwidth * OUTBOUND_MAX_TOKENS_MULTIPLIER,
            ));
            Ok((
                metered(Direction::Inbound, &inbound),
                metered(Direction::Outbound, &outbound),
            ))
        }
    }
}

/// Object storage decorated with network bandwidth limiters.
///
/// Object storage decorator that debits the network bandwidth limiters around
/// each request:
/// - inbound debited before the GET is issued (`range_read`),
/// - outbound debited before the PUT/part is issued (`write`/`upload_part`).
///
/// Limiters are injected via the builder. An absent limiter means no
/// throttling.
pub struct ThrottledObjectStorage {
    inner: Arc<dyn ObjectStorage>,
    inbound: Option<Arc<MeteredBandwidthLimiter>>,
    outbound: Option<Arc<MeteredBandwidthLimiter>>,
}

impl ThrottledObjectStorage {
    /// `AbstractObjectStorage(bucketURI, networkInboundBandwidthLimiter,
    /// networkOutboundBandwidthLimiter, ...)` — `None` behaves like `NOOP`.
    /// `GlobalNetworkBandwidthLimiters`).
    pub fn new(
        inner: Arc<dyn ObjectStorage>,
        inbound: Option<Arc<MeteredBandwidthLimiter>>,
        outbound: Option<Arc<MeteredBandwidthLimiter>>,
    ) -> Self {
        Self {
            inner,
            inbound,
            outbound,
        }
    }
}

#[async_trait]
impl ObjectStorage for ThrottledObjectStorage {
    async fn readiness_check(&self) -> Result<(), ObjectError> {
        self.inner.readiness_check().await
    }

    async fn range_read(
        &self,
        options: &ReadOptions,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Bytes, ObjectError> {
        if end == Some(start) {
            return self.inner.range_read(options, key, start, end).await;
        }
        let Some(inbound) = &self.inbound else {
            return self.inner.range_read(options, key, start, end).await;
        };
        match end {
            Some(end) => {
                inbound.acquire(options.throttle, end - start).await;
                self.inner.range_read(options, key, start, Some(end)).await
            }
            None => {
                // "we don't know the size so acquire size 1 first".
                inbound.acquire(options.throttle, 1).await;
                let data = self.inner.range_read(options, key, start, None).await?;
                if data.len() > 1 {
                    // "when read complete use bypass to forceConsume limiter
                    // token". `consume(BYPASS, readableBytes - 1)` on success only.
                    inbound
                        .acquire(ThrottleStrategy::Bypass, data.len() as u64 - 1)
                        .await;
                }
                Ok(data)
            }
        }
    }

    /// `networkOutboundBandwidthLimiter
    /// .consume(options.throttleStrategy(), data.readableBytes())`, then the PUT.
    async fn write(
        &self,
        options: &WriteOptions,
        key: &str,
        data: Bytes,
    ) -> Result<WriteResult, ObjectError> {
        if let Some(outbound) = &self.outbound {
            outbound.acquire(options.throttle, data.len() as u64).await;
        }
        self.inner.write(options, key, data).await
    }

    /// → `ProxyWriter`. Each part upload goes through
    /// `AbstractObjectStorage#uploadPart`, which consumes outbound tokens. Mirrored
    /// by wrapping the writer.
    async fn writer(
        &self,
        options: &WriteOptions,
        key: &str,
    ) -> Result<Box<dyn MultipartWriter>, ObjectError> {
        let inner = self.inner.writer(options, key).await?;
        Ok(Box::new(ThrottledMultipartWriter {
            inner,
            outbound: self.outbound.clone(),
            throttle: options.throttle,
        }))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
        self.inner.list(prefix).await
    }

    async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
        self.inner.delete(paths).await
    }

    fn bucket_id(&self) -> i16 {
        self.inner.bucket_id()
    }
}

/// Multipart writer that debits outbound bandwidth per part.
///
/// `networkOutboundBandwidthLimiter
/// .consume(options.throttleStrategy(), data.readableBytes())` before each part.
struct ThrottledMultipartWriter {
    inner: Box<dyn MultipartWriter>,
    outbound: Option<Arc<MeteredBandwidthLimiter>>,
    /// ProxyWriter carries the WriteOptions through to every uploadPart).
    throttle: ThrottleStrategy,
}

#[async_trait]
impl MultipartWriter for ThrottledMultipartWriter {
    async fn write(&mut self, part: Bytes) -> Result<(), ObjectError> {
        if let Some(outbound) = &self.outbound {
            outbound.acquire(self.throttle, part.len() as u64).await;
        }
        self.inner.write(part).await
    }

    async fn finish(&mut self) -> Result<WriteResult, ObjectError> {
        self.inner.finish().await
    }

    async fn abort(&mut self) -> Result<(), ObjectError> {
        self.inner.abort().await
    }

    fn bucket_id(&self) -> i16 {
        self.inner.bucket_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test(start_paused = true)]
    async fn priority_ordering_under_contention() {
        // 10 tokens per 100ms tick, cap 10.
        let limiter = Arc::new(BandwidthLimiter::new(10, 100));
        // Drain the initial balance so everything below must queue.
        limiter.acquire(ThrottleStrategy::Bypass, 10).await;
        assert_eq!(limiter.available_tokens(), 0);

        let order = Arc::new(Mutex::new(Vec::new()));
        let spawn = |strategy: ThrottleStrategy, label: &'static str| {
            let limiter = Arc::clone(&limiter);
            let order = Arc::clone(&order);
            tokio::spawn(async move {
                limiter.acquire(strategy, 5).await;
                order.lock().unwrap().push(label);
            })
        };
        // Enqueue lowest-priority first to prove ordering is by tier, not arrival.
        let h1 = spawn(ThrottleStrategy::IcebergWrite, "iceberg");
        tokio::task::yield_now().await;
        let h2 = spawn(ThrottleStrategy::CatchUp, "catchup");
        tokio::task::yield_now().await;
        let h3 = spawn(ThrottleStrategy::Tail, "tail-1");
        tokio::task::yield_now().await;
        let h4 = spawn(ThrottleStrategy::Tail, "tail-2");
        tokio::task::yield_now().await;
        let h5 = spawn(ThrottleStrategy::Compaction, "compaction");
        tokio::task::yield_now().await;
        assert_eq!(limiter.queue_size(), 5);

        // BYPASS still never waits, even with zero tokens and a full queue.
        limiter.acquire(ThrottleStrategy::Bypass, 3).await;
        assert!(limiter.available_tokens() < 0);

        // Each tick refills 10 tokens. 5 x 5-byte waiters finish within a few ticks.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        for h in [h1, h2, h3, h4, h5] {
            h.await.unwrap();
        }
        assert_eq!(
            *order.lock().unwrap(),
            vec!["compaction", "tail-1", "tail-2", "catchup", "iceberg"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bypass_debt_floor_and_refill_cap() {
        let limiter = BandwidthLimiter::with_max_tokens(10, 100, 20);
        // Initial balance is tokenSize (10), not maxTokens (20).
        assert_eq!(limiter.available_tokens(), 10);
        // Massive bypass debits floor at -maxTokens.
        limiter.acquire(ThrottleStrategy::Bypass, 1_000_000).await;
        assert_eq!(limiter.available_tokens(), -20);
        // Each tick adds tokenSize, capped at maxTokens.
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(limiter.available_tokens(), 20);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(limiter.available_tokens(), 20);
    }

    #[tokio::test(start_paused = true)]
    async fn large_request_drains_in_chunks() {
        // 2 MiB per 100ms tick.
        let two_mib = 2 * 1024 * 1024;
        let limiter = Arc::new(BandwidthLimiter::new(two_mib, 100));
        limiter.acquire(ThrottleStrategy::Bypass, two_mib).await;

        let done = Arc::new(AtomicUsize::new(0));
        let handle = {
            let limiter = Arc::clone(&limiter);
            let done = Arc::clone(&done);
            tokio::spawn(async move {
                // 3 MiB: needs two ticks (1 MiB chunk cap per positive-balance step,
                // but each tick drains while positive: tick1 grants 2x1MiB, tick2 1MiB).
                limiter
                    .acquire(ThrottleStrategy::CatchUp, 3 * 1024 * 1024)
                    .await;
                done.store(1, Ordering::Release);
            })
        };
        tokio::task::yield_now().await;
        assert_eq!(limiter.queue_size(), 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            done.load(Ordering::Acquire),
            0,
            "3 MiB not fully granted after one tick"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.await.unwrap();
        assert_eq!(done.load(Ordering::Acquire), 1);
        assert_eq!(limiter.queue_size(), 0);
    }

    /// Writes and part uploads debit the outbound limiter by payload size.
    /// Sized range reads debit the inbound limiter by `end - start`.
    #[tokio::test(start_paused = true)]
    async fn throttled_storage_debits_limiters() {
        use s3stream_object::MemoryObjectStorage;

        let inbound = Arc::new(MeteredBandwidthLimiter::new(
            crate::metrics::Direction::Inbound,
            Arc::new(BandwidthLimiter::new(1000, 1_000_000)),
        ));
        let outbound = Arc::new(MeteredBandwidthLimiter::new(
            crate::metrics::Direction::Outbound,
            Arc::new(BandwidthLimiter::new(1000, 1_000_000)),
        ));
        let storage = ThrottledObjectStorage::new(
            Arc::new(MemoryObjectStorage::new(0)),
            Some(Arc::clone(&inbound)),
            Some(Arc::clone(&outbound)),
        );

        let opts = WriteOptions {
            throttle: ThrottleStrategy::Tail,
            ..Default::default()
        };
        storage
            .write(&opts, "obj", Bytes::from(vec![7u8; 100]))
            .await
            .unwrap();
        assert_eq!(outbound.available_tokens(), 900);

        let mut writer = storage.writer(&opts, "obj2").await.unwrap();
        writer.write(Bytes::from(vec![1u8; 50])).await.unwrap();
        writer.write(Bytes::from(vec![2u8; 30])).await.unwrap();
        writer.finish().await.unwrap();
        assert_eq!(outbound.available_tokens(), 820);

        let ropts = ReadOptions {
            throttle: ThrottleStrategy::CatchUp,
            ..Default::default()
        };
        let data = storage
            .range_read(&ropts, "obj", 10, Some(60))
            .await
            .unwrap();
        assert_eq!(data.len(), 50);
        assert_eq!(inbound.available_tokens(), 950);

        storage
            .range_read(&ropts, "obj", 10, Some(10))
            .await
            .unwrap();
        assert_eq!(inbound.available_tokens(), 950);
    }

    #[tokio::test(start_paused = true)]
    async fn throttled_storage_read_to_end_settles_true_size() {
        use s3stream_object::MemoryObjectStorage;

        let inbound = Arc::new(MeteredBandwidthLimiter::new(
            crate::metrics::Direction::Inbound,
            Arc::new(BandwidthLimiter::new(1000, 1_000_000)),
        ));
        let storage = ThrottledObjectStorage::new(
            Arc::new(MemoryObjectStorage::new(0)),
            Some(Arc::clone(&inbound)),
            None,
        );
        storage
            .write(&WriteOptions::default(), "obj", Bytes::from(vec![9u8; 200]))
            .await
            .unwrap();

        let ropts = ReadOptions {
            throttle: ThrottleStrategy::Tail,
            ..Default::default()
        };
        let data = storage.range_read(&ropts, "obj", 0, None).await.unwrap();
        assert_eq!(data.len(), 200);
        // 1 up front + 199 bypass settle = 200 total.
        assert_eq!(inbound.available_tokens(), 800);
    }

    #[tokio::test(start_paused = true)]
    async fn no_queue_jumping_when_waiters_exist() {
        let limiter = Arc::new(BandwidthLimiter::new(10, 100));
        limiter.acquire(ThrottleStrategy::Bypass, 10).await; // drain to 0

        let first = {
            let limiter = Arc::clone(&limiter);
            tokio::spawn(async move { limiter.acquire(ThrottleStrategy::Tail, 4).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(limiter.queue_size(), 1);
        // One tick: balance positive again (10 - 4 granted = 6 left).
        tokio::time::sleep(Duration::from_millis(150)).await;
        first.await.unwrap();
        assert!(limiter.available_tokens() > 0);

        // A new waiter with tokens available and empty queue takes the sync path.
        limiter.acquire(ThrottleStrategy::Tail, 1).await;
        assert_eq!(limiter.queue_size(), 0);
    }
}
