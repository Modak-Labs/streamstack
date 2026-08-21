//! The vocabulary shared by both protocols.
//!
//! Types are merged where the two protocols say the same thing. Positions
//! are strings because the protocols disagree on their shape (see the crate
//! docs).

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Result;

/// Which wire protocol a client speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Pico,
    Ds,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pico => "pico",
            Self::Ds => "ds",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub name: String,
    pub content_type: Option<String>,
    /// First readable position (DS does not report one, the beginning token).
    pub start: String,
    pub next: String,
    pub closed: bool,
    pub ttl_seconds: Option<u64>,
    /// RFC 3339, as both protocols send it.
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AppendAck {
    pub start: String,
    pub next: String,
    /// Server-assigned timestamp, when the protocol reports one.
    pub timestamp: Option<i64>,
}

/// One record read back.
///
/// A DS read is one chunk of concatenated bodies, so it yields a single
/// record with no headers or timestamp. The protocol does not carry them.
#[derive(Debug, Clone)]
pub struct Record {
    pub position: String,
    pub timestamp: Option<i64>,
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct ReadPage {
    pub records: Vec<Record>,
    pub next: String,
    pub up_to_date: bool,
    pub closed: bool,
}

#[derive(Debug, Clone)]
pub struct StreamListing {
    pub streams: Vec<StreamInfo>,
    pub has_more: bool,
}

/// How much one read may return.
///
/// The Durable Streams protocol has no record count, so it uses `bytes` alone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadLimits {
    pub count: u64,
    pub bytes: u64,
}

impl ReadLimits {
    /// Let the server decide (its `max_chunk_size`).
    pub fn server_default() -> Self {
        Self::default()
    }

    pub fn bytes(bytes: u64) -> Self {
        Self { count: 0, bytes }
    }
}

/// How a read waits for data. SSE is not modeled (see the crate docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Live {
    /// Return whatever is there now.
    Off,
    /// Block server-side until data arrives or the poll times out.
    LongPoll,
}

/// What both protocols can do.
#[async_trait]
pub trait StreamApi: Send + Sync {
    fn protocol(&self) -> Protocol;

    /// The position that means "start of the stream" for this protocol.
    fn beginning(&self) -> String;

    /// The position that means "only records appended from now on".
    fn now(&self) -> Result<String>;

    /// `true` when the stream was created, `false` when it already existed
    async fn create(
        &self,
        name: &str,
        content_type: &str,
        ttl_seconds: Option<u64>,
    ) -> Result<bool>;

    async fn head(&self, name: &str) -> Result<Option<StreamInfo>>;

    /// Append `records` as one request. `content_type` applies where the
    /// protocol sends bodies unwrapped (DS, which requires it to match the
    /// stream). Pico frames records in its batch codec and the stream's type is
    /// fixed at create, so it is ignored there. DS has no batch framing, so
    /// more than one record is rejected rather than silently concatenated.
    async fn append(&self, name: &str, records: &[Bytes], content_type: &str) -> Result<AppendAck>;

    async fn read(
        &self,
        name: &str,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage>;

    /// List streams by name prefix. The Durable Streams protocol has no
    /// listing, so its client returns an `unsupported` error.
    async fn list(&self, prefix: &str, limit: u64) -> Result<StreamListing>;

    /// Seal the stream. Returns its final position.
    async fn close(&self, name: &str) -> Result<String>;

    async fn delete(&self, name: &str) -> Result<bool>;
}
