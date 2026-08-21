//! `pico serve` arguments.
//!
//! One `--meta-url` points at the SQL metadata log, and `--protocol` picks
//! which frontend is mounted.
//!
//! This module only *parses*: every field maps to
//! [`pico_runtime::ServerConfig`], which owns what starting a server means.
//! `--topo` / `ClusterConfig` files are not ported.

use std::net::SocketAddr;
use std::time::Duration;

use clap::{Args, ValueEnum};
use pico_frontend::{Protocol, RoutingMode};
use pico_runtime::{MetaBackend, ServerConfig};

#[derive(Debug, Args)]
pub struct ServeArgs {
    #[arg(long, env = "PICO_NODE_ID", default_value_t = 1)]
    node_id: i32,

    /// Fencing token for this incarnation. Defaults to the current time in
    #[arg(long, env = "PICO_NODE_EPOCH")]
    node_epoch: Option<i64>,

    /// `--http-port`, collapsed into one flag).
    #[arg(long, env = "PICO_LISTEN", default_value = "127.0.0.1:4437")]
    listen: SocketAddr,

    #[arg(long, env = "PICO_ADMIN_LISTEN", default_value = "127.0.0.1:9090")]
    admin_listen: SocketAddr,

    #[arg(long)]
    no_admin: bool,

    /// Public URL of this node, used for cross-node redirects and registered
    /// in metadata. Defaults to `http://{listen}`, which only works when
    #[arg(long, env = "PICO_HTTP_ADDRESS")]
    http_address: Option<String>,

    /// Metadata log: `sqlite::memory:`, `sqlite:<path>` or `postgres://…`
    #[arg(long, env = "PICO_META_URL", default_value = "sqlite:./data/meta.db")]
    meta_url: String,

    #[arg(long, env = "PICO_STORAGE", default_value = "-2@file://./objects")]
    storage: String,

    /// WAL bucket URI. Defaults to the data bucket with its own bucket id
    #[arg(long, env = "PICO_WAL")]
    wal: Option<String>,

    #[arg(long, env = "PICO_CLUSTER_ID", default_value = "picomq")]
    cluster_id: String,

    /// `--routing`).
    #[arg(long, value_enum, default_value_t = RoutingArg::Redirect)]
    routing: RoutingArg,

    /// Placement weight: how many streams this node takes relative to its
    #[arg(long, default_value_t = 1)]
    slots: u32,

    /// `--long-poll-timeout-sec`).
    #[arg(long, default_value_t = 25)]
    long_poll_timeout_sec: u64,

    #[arg(long, default_value_t = 55)]
    sse_max_duration_sec: u64,

    #[arg(long, default_value_t = 64 * 1024)]
    max_chunk_size: usize,

    /// Seconds to fail readiness before closing listeners, so a load balancer
    #[arg(long, default_value_t = 0)]
    shutdown_drain_sec: u64,

    /// Accept queue depth for the listeners. The kernel clamps it to
    /// `somaxconn` (128 on macOS, 4096 on current Linux).
    #[arg(long, default_value_t = 1024)]
    backlog: i32,

    #[arg(long)]
    wal_cache_size: Option<u64>,

    #[arg(long)]
    block_cache_size: Option<u64>,

    /// `--wal-upload-threshold`).
    #[arg(long)]
    wal_upload_threshold: Option<u64>,

    /// Upload buffered WAL this often even below the threshold. 0 disables it.
    #[arg(long)]
    wal_upload_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RoutingArg {
    Redirect,
    Local,
}

impl ServeArgs {
    /// `protocol` is the global `--protocol` flag: which frontend to serve
    pub fn into_config(
        self,
        protocol: Protocol,
    ) -> Result<ServerConfig, Box<dyn std::error::Error>> {
        let defaults = ServerConfig::default();
        let mut engine = s3stream::Config::default();
        if let Some(size) = self.wal_cache_size {
            engine.wal_cache_size = size;
        }
        if let Some(size) = self.block_cache_size {
            engine.block_cache_size = size;
        }
        if let Some(threshold) = self.wal_upload_threshold {
            engine.wal_upload_threshold = threshold;
        }
        if let Some(interval) = self.wal_upload_interval_ms {
            engine.wal_upload_interval_ms = interval;
        }

        Ok(ServerConfig {
            node_id: self.node_id,
            node_epoch: self.node_epoch.unwrap_or(defaults.node_epoch),
            addr: self.listen,
            admin_addr: (!self.no_admin).then_some(self.admin_listen),
            advertised_url: self.http_address,
            protocol,
            meta_backend: MetaBackend::parse(&self.meta_url)?,
            storage_uri: self.storage,
            wal_uri: self.wal,
            cluster_id: self.cluster_id,
            routing_mode: match self.routing {
                RoutingArg::Redirect => RoutingMode::Redirect,
                RoutingArg::Local => RoutingMode::LocalAlways,
            },
            slots: self.slots,
            long_poll_timeout: Duration::from_secs(self.long_poll_timeout_sec),
            sse_max_duration: Duration::from_secs(self.sse_max_duration_sec),
            max_chunk_size: self.max_chunk_size,
            shutdown_drain: Duration::from_secs(self.shutdown_drain_sec),
            backlog: self.backlog,
            engine,
        })
    }
}
