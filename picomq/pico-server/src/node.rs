//! Node assembly: metadata plane + s3stream engine + named-stream service.
//!
//! Intentionally not here: the metadata layer is a
//! [`pico_metadata::CommandSink`] the host constructs (`LocalSink` or
//! `pico-sql`'s `SqlSink`) and owns, including its shutdown and any lease
//! keeper.

use std::sync::Arc;

use pico_metadata::{CommandSink, MetadataNodeHandle, ViewPublisher};
use s3stream::{
    Client as _, Config, KVClient, ObjectStorageTrait, ObjectWalConfig, ObjectWalService,
    S3StreamBuilder, S3StreamEngine,
};

use crate::error::ServiceError;
use crate::ownership::MetadataOwnershipService;
use crate::service::S3StreamService;
use crate::waiter::StreamWaiterRegistry;

/// The node identity + engine tuning the host passes in.
///
/// `slots` is the placement weight. Storage and WAL come in as host-provided
/// handles. URI parsing belongs to the CLI.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: i32,
    pub node_epoch: i64,
    /// (advertised address, used for
    /// redirects).
    pub http_address: String,
    pub slots: u32,
    pub cluster_id: String,
    pub engine: Config,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4437".to_owned(),
            slots: 1,
            cluster_id: "picomq".to_owned(),
            engine: Config::default(),
        }
    }
}

pub struct PicoNode {
    config: NodeConfig,
    handle: MetadataNodeHandle,
    views: Arc<ViewPublisher>,
    engine: S3StreamEngine,
    service: Arc<S3StreamService>,
    ownership: Arc<MetadataOwnershipService>,
}

impl PicoNode {
    /// Wire and start a node on an already-open metadata sink. The engine
    /// builder recovers the WAL and starts the pipeline. A `propose` that
    /// returns is already applied, so registration needs no separate wait.
    pub async fn start(
        config: NodeConfig,
        sink: Arc<dyn CommandSink>,
        views: Arc<ViewPublisher>,
        object_storage: Arc<dyn ObjectStorageTrait>,
        wal_storage: Arc<dyn ObjectStorageTrait>,
    ) -> Result<Self, ServiceError> {
        let handle =
            MetadataNodeHandle::new(config.node_id, config.node_epoch, sink, views.clone());
        handle
            .register_with_slots(&config.http_address, config.slots)
            .await
            .map_err(|e| e.to_stream_error())?;

        let mut wal_config = ObjectWalConfig::from_uri_or_defaults(&config.engine.wal_config)
            .map_err(|e| {
                ServiceError::with_message(crate::ErrorKind::BadRequest, None, false, e.to_string())
            })?;
        wal_config.cluster_id = config.cluster_id.clone();
        wal_config.node_id = config.node_id as u32;
        wal_config.epoch = config.node_epoch as u64;

        let engine = S3StreamBuilder::new(config.engine.clone())
            .object_storage(object_storage)
            .write_ahead_log(Arc::new(ObjectWalService::new(wal_storage, wal_config)))
            .stream_manager(Arc::new(handle.stream_manager()))
            .object_manager(Arc::new(handle.object_manager()))
            .kv_client(Arc::new(handle.kv_client()))
            .build()
            .await?;

        let kv_client: Arc<dyn KVClient> = engine.kv_client();
        let service = Arc::new(S3StreamService::new(
            engine.stream_client(),
            kv_client,
            views.clone(),
            handle.clone(),
            Arc::new(StreamWaiterRegistry::new()),
        ));
        let ownership = Arc::new(MetadataOwnershipService::new(
            views.clone(),
            config.node_id,
            config.http_address.clone(),
            service.clone(),
        ));

        Ok(Self {
            config,
            handle,
            views,
            engine,
            service,
            ownership,
        })
    }

    pub fn service(&self) -> Arc<S3StreamService> {
        self.service.clone()
    }

    pub fn stream_service(&self) -> Arc<S3StreamService> {
        self.service.clone()
    }

    pub fn ownership(&self) -> Arc<MetadataOwnershipService> {
        self.ownership.clone()
    }

    pub fn advertised_address(&self) -> &str {
        &self.config.http_address
    }

    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    pub fn metadata(&self) -> &MetadataNodeHandle {
        &self.handle
    }

    pub fn views(&self) -> Arc<ViewPublisher> {
        self.views.clone()
    }

    pub fn engine(&self) -> &S3StreamEngine {
        &self.engine
    }

    pub async fn close(&self) {
        self.service.shutdown().await;
        self.engine.shutdown().await;
    }
}
