//! Building WALs from config URIs and fencing access to them.
//!
//! Two separable concerns:
//! - `WalFactory`: URI (`0@s3://bucket?...`) -> `WriteAheadLog` instance. This is how
//!   both normal startup and failover construct a WAL without knowing its kind
//!   (scheme selects object WAL vs future block WAL).
//! - `WalHandle`: acquire/release *permission* to access a WAL on a given node.
//!   The fencing step above the WAL itself. For the object WAL, `DefaultWalHandle`
//!   delegates to the reservation object (acquire = PUT the reservation with the new
//!   epoch / failover flag, which fences the previous writer).

use std::sync::Arc;

use async_trait::async_trait;
use s3stream_object::{IdUri, ObjectStorage, ObjectStoreAdapter};

use crate::object::{ObjectReservationService, ObjectWalConfig, ObjectWalService};
use crate::{OpenMode, ReservationService, WalError, WriteAheadLog};

#[derive(Debug, Clone, Copy)]
pub struct BuildOptions {
    pub node_epoch: u64,
    pub open_mode: OpenMode,
}

impl BuildOptions {
    pub fn validate(&self) -> Result<(), WalError> {
        if self.node_epoch == 0 {
            return Err(WalError::Recovery(
                "The node epoch must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

pub trait WalFactory: Send + Sync {
    /// Build a WAL from an IdURI-style config string.
    fn build(&self, uri: &str, options: BuildOptions) -> Result<Arc<dyn WriteAheadLog>, WalError>;
}

#[derive(Debug, Clone, Copy)]
pub struct AcquirePermissionOptions {
    pub failover_mode: bool,
    pub timeout_ms: u64,
}

impl Default for AcquirePermissionOptions {
    fn default() -> Self {
        Self {
            failover_mode: false,
            timeout_ms: 20_000,
        }
    }
}

#[async_trait]
pub trait WalHandle: Send + Sync {
    /// Fence the WAL identified by `wal_config` on `node_id` and acquire access.
    async fn acquire_permission(
        &self,
        node_id: u32,
        node_epoch: u64,
        wal_config: &str,
        options: AcquirePermissionOptions,
    ) -> Result<(), WalError>;

    async fn release_permission(&self, wal_config: &str) -> Result<(), WalError>;
}

/// Default handle: object WAL fencing via the reservation object.
pub struct DefaultWalHandle {
    cluster_id: String,
    /// `mem://` is not a singleton, so tests inject the shared `MemoryObjectStorage`
    /// the WAL also uses. Protocol (PUT reservation with failover flag) is unchanged.
    storage: Option<Arc<dyn ObjectStorage>>,
}

impl DefaultWalHandle {
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            storage: None,
        }
    }

    /// Shared object storage for acquire + the WAL itself (required for `mem://` tests).
    pub fn with_storage(cluster_id: impl Into<String>, storage: Arc<dyn ObjectStorage>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            storage: Some(storage),
        }
    }
}

#[async_trait]
impl WalHandle for DefaultWalHandle {
    async fn acquire_permission(
        &self,
        node_id: u32,
        node_epoch: u64,
        wal_config: &str,
        options: AcquirePermissionOptions,
    ) -> Result<(), WalError> {
        let parsed = IdUri::parse(wal_config)?;
        check_object_wal_protocol(&parsed.protocol, wal_config)?;
        let storage = self.storage_for(&parsed, wal_config)?;
        let reservation =
            ObjectReservationService::new(storage, self.cluster_id.clone(), parsed.id);
        reservation
            .acquire(node_id, node_epoch, options.failover_mode)
            .await
    }

    async fn release_permission(&self, wal_config: &str) -> Result<(), WalError> {
        let parsed = IdUri::parse(wal_config)?;
        check_object_wal_protocol(&parsed.protocol, wal_config)?;
        Ok(())
    }
}

impl DefaultWalHandle {
    fn storage_for(
        &self,
        _parsed: &IdUri,
        wal_config: &str,
    ) -> Result<Arc<dyn ObjectStorage>, WalError> {
        if let Some(storage) = &self.storage {
            return Ok(Arc::clone(storage));
        }
        Ok(Arc::new(ObjectStoreAdapter::from_bucket_uri(wal_config)?))
    }
}

pub struct ObjectWalFactory {
    cluster_id: String,
    node_id: u32,
    /// Same `with_storage` reason as [`DefaultWalHandle`]: `mem://` is not shared.
    storage: Option<Arc<dyn ObjectStorage>>,
}

impl ObjectWalFactory {
    pub fn new(cluster_id: impl Into<String>, node_id: u32) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id,
            storage: None,
        }
    }

    pub fn with_storage(
        cluster_id: impl Into<String>,
        node_id: u32,
        storage: Arc<dyn ObjectStorage>,
    ) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id,
            storage: Some(storage),
        }
    }
}

impl WalFactory for ObjectWalFactory {
    fn build(&self, uri: &str, options: BuildOptions) -> Result<Arc<dyn WriteAheadLog>, WalError> {
        options.validate()?;
        let parsed = IdUri::parse(uri)?;
        check_object_wal_protocol(&parsed.protocol, uri)?;
        let storage = if let Some(storage) = &self.storage {
            Arc::clone(storage)
        } else {
            Arc::new(ObjectStoreAdapter::from_bucket_uri(uri)?)
        };
        let mut config = ObjectWalConfig::from_uri(uri)?;
        config.cluster_id = self.cluster_id.clone();
        config.node_id = self.node_id;
        config.epoch = options.node_epoch;
        config.open_mode = options.open_mode;
        config.reservation_service = Arc::new(ObjectReservationService::new(
            Arc::clone(&storage),
            self.cluster_id.clone(),
            config.bucket_id,
        ));
        Ok(Arc::new(ObjectWalService::new(storage, config)))
    }
}

fn check_object_wal_protocol(protocol: &str, uri: &str) -> Result<(), WalError> {
    match protocol.to_ascii_uppercase().as_str() {
        "S3" | "MEM" | "FILE" => Ok(()),
        other => Err(WalError::Recovery(format!(
            "Unsupported WAL protocol {other} in {uri}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReservationService, WalMetadata};
    use s3stream_object::MemoryObjectStorage;

    const URI: &str = "0@mem://wal";

    #[tokio::test]
    async fn acquire_fences_previous_writer() {
        let storage: Arc<dyn ObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let handle = DefaultWalHandle::with_storage("c", Arc::clone(&storage));
        handle
            .acquire_permission(7, 1, URI, AcquirePermissionOptions::default())
            .await
            .unwrap();

        let reservation = ObjectReservationService::new(Arc::clone(&storage), "c".into(), 0);
        assert!(reservation.verify(7, 1, false).await.unwrap());

        handle
            .acquire_permission(
                7,
                1,
                URI,
                AcquirePermissionOptions {
                    failover_mode: true,
                    timeout_ms: 20_000,
                },
            )
            .await
            .unwrap();
        assert!(!reservation.verify(7, 1, false).await.unwrap());
        assert!(reservation.verify(7, 1, true).await.unwrap());
    }

    #[tokio::test]
    async fn release_is_noop_for_object_wal() {
        let handle = DefaultWalHandle::new("c");
        handle.release_permission(URI).await.unwrap();
    }

    #[tokio::test]
    async fn object_wal_factory_builds_recovery_wal() {
        let storage: Arc<dyn ObjectStorage> = Arc::new(MemoryObjectStorage::new(0));
        let handle = DefaultWalHandle::with_storage("c", Arc::clone(&storage));
        handle
            .acquire_permission(
                3,
                1,
                URI,
                AcquirePermissionOptions {
                    failover_mode: true,
                    timeout_ms: 20_000,
                },
            )
            .await
            .unwrap();

        let factory = ObjectWalFactory::with_storage("c", 3, storage);
        let wal = factory
            .build(
                URI,
                BuildOptions {
                    node_epoch: 1,
                    open_mode: OpenMode::Recovery,
                },
            )
            .unwrap();
        wal.start().await.unwrap();
        assert_eq!(
            wal.metadata(),
            WalMetadata {
                node_id: 3,
                epoch: 1
            }
        );
        wal.shutdown_gracefully().await;
    }

    #[test]
    fn epoch_zero_rejected() {
        let factory = ObjectWalFactory::new("c", 1);
        let err = factory
            .build(
                URI,
                BuildOptions {
                    node_epoch: 0,
                    open_mode: OpenMode::ReadWrite,
                },
            )
            .err()
            .expect("epoch 0 must be rejected");
        assert!(matches!(err, WalError::Recovery(_)), "{err}");
    }
}
