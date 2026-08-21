//! Engine configuration. Field names and defaults are load-bearing: tests
//! and deployments rely on them.

use s3stream_core::api::StreamError;
pub use s3stream_core::throttle::NetworkBandwidthMode;

/// Engine configuration. URI fields use the s3stream IdURI format
#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_id: String,
    pub node_id: u32,
    /// Node epoch from the metadata plane (fences older instances of this node).
    pub node_epoch: u64,

    /// WAL config URI (e.g. `0@s3://wal-bucket?batchInterval=250`).
    pub wal_config: String,
    pub data_buckets: Vec<String>,

    pub wal_cache_size: u64,
    /// (default 100 MiB, clamped to 2/5 of cache size at wiring).
    pub wal_upload_threshold: u64,
    /// Periodic upload interval (ms), 0 = disabled.
    pub wal_upload_interval_ms: u64,

    pub block_cache_size: u64,

    pub stream_split_size: u64,
    pub max_stream_num_per_stream_set_object: usize,
    pub object_block_size: usize,
    pub object_part_size: usize,

    pub compaction_interval_ms: u64,
    pub compaction_bandwidth: u64,

    pub network_baseline_bandwidth: u64,
    /// Separate vs shared inbound/outbound token buckets.
    pub network_bandwidth_mode: NetworkBandwidthMode,
    pub refill_period_ms: u64,

    /// Failover mode (this instance recovers foreign WALs).
    pub failover_enable: bool,
    /// Replica node serves tail reads for streams it does not own.
    pub snapshot_read_enable: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cluster_id: String::new(),
            node_id: 0,
            node_epoch: 0,
            wal_config: String::new(),
            data_buckets: Vec::new(),
            wal_cache_size: 200 * 1024 * 1024,
            wal_upload_threshold: 100 * 1024 * 1024,
            wal_upload_interval_ms: 0,
            block_cache_size: 100 * 1024 * 1024,
            stream_split_size: 8 * 1024 * 1024,
            max_stream_num_per_stream_set_object: 100_000,
            object_block_size: 1024 * 1024,
            object_part_size: 16 * 1024 * 1024,
            compaction_interval_ms: 20 * 60 * 1000,
            compaction_bandwidth: 200 * 1024 * 1024,
            network_baseline_bandwidth: 1024 * 1024 * 1024,
            network_bandwidth_mode: NetworkBandwidthMode::Separate,
            refill_period_ms: 10,
            failover_enable: false,
            snapshot_read_enable: false,
        }
    }
}

impl Config {
    /// Validate cross-field invariants before wiring the engine.
    ///
    /// - `walUploadThreshold > walCacheSize` → error (same message shape).
    ///   allocator budget to check, so that clause is intentionally not ported.
    ///
    /// `GlobalNetworkBandwidthLimiters#setup`) runs at build time in
    /// `build_network_limiters` when throttling is enabled.
    pub fn validate(&self) -> Result<(), StreamError> {
        if self.wal_upload_threshold > self.wal_cache_size {
            return Err(StreamError::Unexpected(format!(
                "walUploadThreshold {} exceeds walCacheSize {}",
                self.wal_upload_threshold, self.wal_cache_size
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_threshold_above_cache() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());
        config.wal_upload_threshold = config.wal_cache_size + 1;
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("exceeds walCacheSize"));
    }

    #[test]
    fn bandwidth_mode_parse() {
        assert_eq!(
            NetworkBandwidthMode::parse(" Shared ").unwrap(),
            NetworkBandwidthMode::Shared
        );
        assert_eq!(
            NetworkBandwidthMode::parse("SEPARATE").unwrap(),
            NetworkBandwidthMode::Separate
        );
        assert!(NetworkBandwidthMode::parse("bogus").is_err());
    }
}
