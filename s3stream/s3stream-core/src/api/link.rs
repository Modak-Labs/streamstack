//! Link-record decoding hook.
//!
//! A link record (`StreamRecordBatch` count == 0 with negative flag magic, see
//! specification/record-format.md) is a host-defined pointer to payload stored elsewhere
//! (e.g. table-topic segments). The engine never interprets link payloads.
//! When a fetch path needs the materialized data (snapshot-read cache replay,
//! compaction of linked ranges) it calls the host-provided decoder.

use async_trait::async_trait;
use bytes::Bytes;

use crate::api::StreamError;
use s3stream_codec::StreamRecordBatch;

/// The default engine wiring uses no decoder
/// (equivalent of `LinkRecordDecoder.NOOP`): any attempt to decode fails.
#[async_trait]
pub trait LinkRecordDecoder: Send + Sync {
    /// Size of the decoded record for cache accounting, without decoding.
    fn decoded_size(&self, link_payload: &Bytes) -> Result<usize, StreamError>;

    /// Materialize the linked payload into a plain record batch.
    async fn decode(&self, src: StreamRecordBatch) -> Result<StreamRecordBatch, StreamError>;
}
