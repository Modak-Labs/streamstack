//! Object WAL configuration.

use std::sync::Arc;
use std::time::Duration;

use s3stream_object::IdUri;

use crate::{NoopReservationService, OpenMode, ReservationService, WalError};

/// Configuration for one object WAL instance.
#[derive(Clone)]
pub struct ObjectWalConfig {
    /// Bucket URI this WAL was built from (IdURI format, reconstructable).
    pub uri: String,
    pub cluster_id: String,
    pub node_id: u32,
    /// Node epoch granted by the metadata plane. Fences older writers.
    pub epoch: u64,
    pub open_mode: OpenMode,
    pub bucket_id: i16,
    /// Optional WAL type tag appended to the node prefix (e.g. snapshot-read WALs).
    pub wal_type: String,

    /// Seal + upload a batch after this long even if not full. Default 250 ms.
    pub batch_interval: Duration,
    /// Seal + upload a batch at this size. Default 8 MiB.
    pub max_bytes_in_batch: u64,
    /// Cap on unconfirmed bytes. Appends beyond it fail OverCapacity. Default 1 GiB.
    pub max_unflushed_bytes: u64,
    /// Max concurrent WAL object PUTs. Default 50 (min 1).
    pub max_inflight_upload_count: usize,
    /// Readahead window for recovery/get. Default 100 MiB (min 1).
    pub readahead_data_size: u64,

    pub reservation_service: Arc<dyn ReservationService>,
}

impl ObjectWalConfig {
    /// Defaults mirroring `ObjectWALConfig.Builder`. Identity fields (cluster, node,
    /// `withClusterId`/`withNodeId`/... setters.
    pub fn defaults() -> Self {
        Self {
            uri: String::new(),
            cluster_id: String::new(),
            node_id: 0,
            epoch: 0,
            open_mode: OpenMode::ReadWrite,
            bucket_id: 0,
            wal_type: String::new(),
            batch_interval: Duration::from_millis(250),
            max_bytes_in_batch: 8 * 1024 * 1024,
            max_unflushed_bytes: 1024 * 1024 * 1024,
            max_inflight_upload_count: 50,
            readahead_data_size: 100 * 1024 * 1024,
            reservation_service: Arc::new(NoopReservationService),
        }
    }

    /// Parse defaults + overrides from the IdURI extension params (`batchInterval`,
    /// `maxBytesInBatch`, `maxUnflushedBytes`, `maxInflightUploadCount`,
    /// `StringUtils.isNumeric` guard.
    ///
    /// Like [`Self::from_uri`], but an empty URI means "all defaults".
    ///
    /// Callers that inject their own WAL object storage (embedded use,
    /// tests) have no URI to give, while a caller that does give one must
    /// have its parameters honored. Silently defaulting a non-empty URI would
    /// pin every deployment to the 250 ms batch window.
    pub fn from_uri_or_defaults(uri: &str) -> Result<Self, WalError> {
        if uri.is_empty() {
            return Ok(Self::defaults());
        }
        Self::from_uri(uri)
    }

    pub fn from_uri(uri: &str) -> Result<Self, WalError> {
        let parsed = IdUri::parse(uri)?;
        let mut config = Self::defaults();
        config.uri = uri.to_string();
        config.bucket_id = parsed.id;
        if let Some(v) = parsed
            .extension_str("batchInterval")
            .and_then(numeric::<u64>)
        {
            config.batch_interval = Duration::from_millis(v);
        }
        if let Some(v) = parsed
            .extension_str("maxBytesInBatch")
            .and_then(numeric::<u64>)
        {
            config.max_bytes_in_batch = v;
        }
        if let Some(v) = parsed
            .extension_str("maxUnflushedBytes")
            .and_then(numeric::<u64>)
        {
            config.max_unflushed_bytes = v;
        }
        if let Some(v) = parsed
            .extension_str("maxInflightUploadCount")
            .and_then(numeric::<usize>)
        {
            config.max_inflight_upload_count = v.max(1);
        }
        if let Some(v) = parsed
            .extension_str("readaheadDataSize")
            .and_then(numeric::<u64>)
        {
            config.readahead_data_size = v.max(1);
        }
        Ok(config)
    }
}

fn numeric<T: std::str::FromStr>(s: &str) -> Option<T> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java_builder() {
        let c = ObjectWalConfig::defaults();
        assert_eq!(c.batch_interval, Duration::from_millis(250));
        assert_eq!(c.max_bytes_in_batch, 8 * 1024 * 1024);
        assert_eq!(c.max_unflushed_bytes, 1024 * 1024 * 1024);
        assert_eq!(c.max_inflight_upload_count, 50);
        assert_eq!(c.readahead_data_size, 100 * 1024 * 1024);
    }

    #[test]
    fn uri_overrides_applied() {
        let c = ObjectWalConfig::from_uri(
            "5@s3://bucket?batchInterval=100&maxBytesInBatch=1024&maxUnflushedBytes=2048&maxInflightUploadCount=0&readaheadDataSize=0",
        )
        .unwrap();
        assert_eq!(c.bucket_id, 5);
        assert_eq!(c.batch_interval, Duration::from_millis(100));
        assert_eq!(c.max_bytes_in_batch, 1024);
        assert_eq!(c.max_unflushed_bytes, 2048);
        assert_eq!(c.max_inflight_upload_count, 1);
        assert_eq!(c.readahead_data_size, 1);
    }

    #[test]
    fn non_numeric_extensions_ignored() {
        let c = ObjectWalConfig::from_uri("0@s3://bucket?batchInterval=-5&maxBytesInBatch=abc")
            .unwrap();
        assert_eq!(c.batch_interval, Duration::from_millis(250));
        assert_eq!(c.max_bytes_in_batch, 8 * 1024 * 1024);
    }
}
