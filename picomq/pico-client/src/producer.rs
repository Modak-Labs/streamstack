use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::error::{ClientError, ErrorKind, Result};
use crate::pico::{PicoClient, ProducerRef};
use crate::retry::RetryPolicy;

/// How a [`Producer`] batches and pipelines.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Producer epoch. Raise it to fence an earlier session for the same id
    /// (its appends then fail as stale). A new epoch restarts sequences at 0.
    pub epoch: u64,
    /// How long a partly-filled batch waits for company. This is latency added
    /// to an idle producer's first record, and the main throughput knob for one
    /// that is busy.
    pub linger: Duration,
    /// Records per append. This is the throughput knob: measured on one node
    /// with 1 KiB records, 100 gives ~50 MiB/s and 500 gives ~198 MiB/s, while
    /// 1000 gains nothing and triples the tail latency.
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    /// Batches in flight at once. Defaults to 1, and raising it is usually a
    /// mistake: throughput comes from `max_batch_records`.
    ///
    /// Kafka's idempotent producer allows 5 because a broker handles one
    /// connection's requests in arrival order, so a pipelined batch is rarely
    /// out of order. This server dispatches each HTTP request to its own task,
    /// so with 5 in flight the later sequences routinely arrive first, get
    /// rejected, and back off. Measured at 86 records/s against 60,000 for the
    /// same load at 1. Ordering is never violated either way. The cost is
    /// throughput.
    pub max_inflight: usize,
    /// Bytes allowed in the session before `send` blocks, which is how
    /// backpressure reaches the caller instead of growing a queue.
    pub max_buffered_bytes: usize,
    /// Applies to a batch rejected for arriving early, and to retryable
    /// transport failures.
    pub retry: RetryPolicy,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            epoch: 0,
            linger: Duration::from_millis(5),
            max_batch_records: 500,
            max_batch_bytes: 1024 * 1024,
            max_inflight: 1,
            max_buffered_bytes: 32 * 1024 * 1024,
            retry: RetryPolicy {
                max_attempts: 12,
                initial_backoff: Duration::from_millis(1),
                max_backoff: Duration::from_millis(100),
                multiplier: 2.0,
            },
        }
    }
}

/// A record accepted by the session but not yet durable.
#[derive(Debug)]
pub struct Pending {
    rx: oneshot::Receiver<Result<u64>>,
}

impl Pending {
    /// The record's sequence number in the stream, once it is durable.
    pub async fn durable(self) -> Result<u64> {
        self.rx
            .await
            .unwrap_or_else(|_| Err(stopped("producer stopped before the record was durable")))
    }
}

struct Item {
    body: Bytes,
    ack: oneshot::Sender<Result<u64>>,
    permit: OwnedSemaphorePermit,
}

/// A session that batches records into ordered, pipelined appends.
///
/// Dropping it stops the session. In-flight records may not complete, so prefer
/// [`Producer::close`].
pub struct Producer {
    tx: mpsc::Sender<Item>,
    budget: Arc<Semaphore>,
    /// Set when a batch fails for good. The sequence cannot be continued past a
    /// hole, so every later record has to fail too rather than silently landing
    /// out of order.
    poisoned: Arc<AtomicBool>,
    max_buffered_bytes: usize,
}

impl Producer {
    /// Open a session appending to `name` as producer `id`.
    ///
    /// The id identifies the session across reconnects and retries: reusing it
    /// is what lets the server recognize a duplicate append, so it should be
    /// stable for as long as the caller's sequence is.
    pub fn new(client: Arc<PicoClient>, name: &str, id: &str, config: ProducerConfig) -> Self {
        let budget = Arc::new(Semaphore::new(config.max_buffered_bytes));
        let poisoned = Arc::new(AtomicBool::new(false));
        // One slot per in-flight batch is enough queueing. The byte budget is
        // what actually bounds the session.
        let (tx, rx) = mpsc::channel(config.max_inflight.max(1));
        tokio::spawn(run(
            client,
            name.to_owned(),
            id.to_owned(),
            config.clone(),
            rx,
            Arc::clone(&poisoned),
        ));
        Self {
            tx,
            budget,
            poisoned,
            max_buffered_bytes: config.max_buffered_bytes,
        }
    }

    /// Hand a record to the session, waiting only for buffer space.
    ///
    /// Returning does not mean durable. Await the [`Pending`] for that. The
    /// order of these calls is the order the records land in the stream.
    pub async fn send(&self, record: Bytes) -> Result<Pending> {
        if record.len() > self.max_buffered_bytes {
            return Err(
                ClientError::new(0, ErrorKind::BadRequest, "record_too_large").with_message(Some(
                    format!(
                        "record of {} bytes exceeds the session's {} byte budget",
                        record.len(),
                        self.max_buffered_bytes
                    ),
                )),
            );
        }
        self.check_poisoned()?;
        // Held until the record is durable, so a caller that outruns the server
        // ends up waiting here.
        let permit = Arc::clone(&self.budget)
            .acquire_many_owned(record.len().max(1) as u32)
            .await
            .map_err(|_| stopped("producer is closed"))?;
        let (ack, rx) = oneshot::channel();
        self.tx
            .send(Item {
                body: record,
                ack,
                permit,
            })
            .await
            .map_err(|_| stopped("producer is closed"))?;
        Ok(Pending { rx })
    }

    /// Send one record and wait for it to be durable.
    ///
    /// Convenient, but a caller that awaits every record in turn holds one
    /// record in flight and gets latency-bound throughput. That is what
    /// [`Producer::send`] plus a later await avoids.
    pub async fn send_durable(&self, record: Bytes) -> Result<u64> {
        self.send(record).await?.durable().await
    }

    /// Wait for every record handed over so far to be durable.
    pub async fn flush(&self) -> Result<()> {
        // Reclaiming the whole budget can only happen once every outstanding
        // record has completed and dropped its permit.
        let _all = Arc::clone(&self.budget)
            .acquire_many_owned(self.max_buffered_bytes as u32)
            .await
            .map_err(|_| stopped("producer is closed"))?;
        self.check_poisoned()
    }

    /// Flush, then stop the session.
    pub async fn close(self) -> Result<()> {
        let result = self.flush().await;
        drop(self.tx);
        result
    }

    fn check_poisoned(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(
                ClientError::new(0, ErrorKind::Conflict, "producer_poisoned").with_message(Some(
                    "producer session failed and cannot continue its sequence; open a new \
                     session (a higher epoch restarts at sequence 0)"
                        .to_owned(),
                )),
            );
        }
        Ok(())
    }
}

/// Collect records into batches and keep `max_inflight` of them going.
async fn run(
    client: Arc<PicoClient>,
    name: String,
    id: String,
    config: ProducerConfig,
    mut rx: mpsc::Receiver<Item>,
    poisoned: Arc<AtomicBool>,
) {
    let inflight = Arc::new(Semaphore::new(config.max_inflight.max(1)));
    let mut seq = 0u64;
    while let Some(first) = rx.recv().await {
        let batch = collect(&mut rx, first, &config).await;
        // Sequences are assigned here, in batch order, and the server enforces
        // that order on arrival.
        let this_seq = seq;
        seq += 1;
        let Ok(permit) = Arc::clone(&inflight).acquire_owned().await else {
            return;
        };
        tokio::spawn(send_batch(
            Arc::clone(&client),
            name.clone(),
            id.clone(),
            config.clone(),
            batch,
            this_seq,
            Arc::clone(&poisoned),
            permit,
        ));
    }
}

/// Fill a batch until it is full or the linger expires.
async fn collect(rx: &mut mpsc::Receiver<Item>, first: Item, config: &ProducerConfig) -> Vec<Item> {
    let mut bytes = first.body.len();
    let mut batch = vec![first];
    if config.linger.is_zero() {
        // Still worth draining whatever is already queued.
        while batch.len() < config.max_batch_records && bytes < config.max_batch_bytes {
            match rx.try_recv() {
                Ok(item) => {
                    bytes += item.body.len();
                    batch.push(item);
                }
                Err(_) => break,
            }
        }
        return batch;
    }
    let deadline = tokio::time::Instant::now() + config.linger;
    while batch.len() < config.max_batch_records && bytes < config.max_batch_bytes {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(item)) => {
                bytes += item.body.len();
                batch.push(item);
            }
            // Sender gone, or the linger expired.
            Ok(None) | Err(_) => break,
        }
    }
    batch
}

#[allow(clippy::too_many_arguments)]
async fn send_batch(
    client: Arc<PicoClient>,
    name: String,
    id: String,
    config: ProducerConfig,
    batch: Vec<Item>,
    seq: u64,
    poisoned: Arc<AtomicBool>,
    _permit: OwnedSemaphorePermit,
) {
    let records: Vec<Bytes> = batch.iter().map(|item| item.body.clone()).collect();
    let producer = ProducerRef {
        id: &id,
        epoch: config.epoch,
        seq,
    };
    let result = append_with_retries(&client, &name, &records, &producer, &config).await;

    match result {
        Ok(start) => {
            for (i, item) in batch.into_iter().enumerate() {
                let _ = item.ack.send(Ok(start + i as u64));
                drop(item.permit);
            }
        }
        Err(e) => {
            // This sequence will never be applied, so every later batch would
            // be rejected as a gap forever. Fail the session rather than let
            // callers keep sending into a hole.
            poisoned.store(true, Ordering::Release);
            for item in batch {
                let _ = item.ack.send(Err(e.clone()));
                drop(item.permit);
            }
        }
    }
}

/// Retry a batch that arrived out of order, or hit a retryable transport error.
///
/// Resending is safe at any point: the producer sequence makes the append
/// idempotent, so a request that did land is recognized and applied once.
async fn append_with_retries(
    client: &PicoClient,
    name: &str,
    records: &[Bytes],
    producer: &ProducerRef<'_>,
    config: &ProducerConfig,
) -> Result<u64> {
    let mut attempt = 0;
    loop {
        match client.append_as(name, records, producer).await {
            Ok(ack) => {
                if ack.duplicate {
                    // Applied by an earlier attempt. The response carries the
                    // producer's last sequence, not this batch's placement, so
                    // derive the start from the stream's tail.
                    let next: u64 = ack.ack.next.parse().unwrap_or_default();
                    return Ok(next.saturating_sub(records.len() as u64));
                }
                return ack
                    .ack
                    .start
                    .parse()
                    .map_err(|_| stopped("server returned a non-numeric start"));
            }
            Err(e) => {
                // A gap means an earlier batch has not landed yet. Waiting is
                // the fix, and it will be brief.
                let out_of_order = e.code == "sequence_gap";
                match config.retry.delay(attempt) {
                    Some(delay) if out_of_order || e.retryable() => tokio::time::sleep(delay).await,
                    _ => return Err(e),
                }
                attempt += 1;
            }
        }
    }
}

fn stopped(message: &str) -> ClientError {
    ClientError::new(0, ErrorKind::Other, "producer_stopped").with_message(Some(message.to_owned()))
}
