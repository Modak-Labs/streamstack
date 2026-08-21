//! Reservation service backed by the object storage itself.
//!
//! The reservation is a small object at `reservation/{namespace}{clusterId}/{nodeId}`
//! recording (magic, nodeId, epoch, failover flag). `acquire` overwrites it (newer
//! epoch wins).`verify` reads it back and byte-compares against the caller's identity.
//! This is what fences a zombie writer whose metadata-plane epoch has been superseded.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};

use s3stream_object::{ObjectStorage, ReadOptions, ThrottleStrategy, WriteOptions};

use super::keys::DEFAULT_NAMESPACE;
use crate::{ReservationService, WalError};

pub const S3_RESERVATION_OBJECT_MAGIC_CODE: u32 = 0x1234_5678;
/// Layout: magic(4) + node_id(8) + epoch(8) + failover(1).
pub const S3_RESERVATION_OBJECT_LENGTH: usize = 4 + 8 + 8 + 1;

pub struct ObjectReservationService {
    storage: Arc<dyn ObjectStorage>,
    cluster_id: String,
    bucket_id: i16,
}

impl ObjectReservationService {
    pub fn new(storage: Arc<dyn ObjectStorage>, cluster_id: String, bucket_id: i16) -> Self {
        Self {
            storage,
            cluster_id,
            bucket_id,
        }
    }

    fn path(&self, node_id: u32) -> String {
        format!(
            "reservation/{DEFAULT_NAMESPACE}{}/{node_id}",
            self.cluster_id
        )
    }

    fn target_bytes(node_id: u32, epoch: u64, failover: bool) -> Bytes {
        let mut buf = BytesMut::with_capacity(S3_RESERVATION_OBJECT_LENGTH);
        buf.put_u32(S3_RESERVATION_OBJECT_MAGIC_CODE);
        buf.put_i64(node_id as i64);
        buf.put_i64(epoch as i64);
        buf.put_u8(failover as u8);
        buf.freeze()
    }
}

#[async_trait]
impl ReservationService for ObjectReservationService {
    async fn acquire(&self, node_id: u32, epoch: u64, failover: bool) -> Result<(), WalError> {
        tracing::info!(node_id, epoch, failover, "acquire WAL reservation");
        let options = WriteOptions {
            throttle: ThrottleStrategy::Bypass,
            bucket_id: Some(self.bucket_id),
            ..Default::default()
        };
        self.storage
            .write(
                &options,
                &self.path(node_id),
                Self::target_bytes(node_id, epoch, failover),
            )
            .await?;
        Ok(())
    }

    async fn verify(&self, node_id: u32, epoch: u64, failover: bool) -> Result<bool, WalError> {
        let options = ReadOptions {
            throttle: ThrottleStrategy::Bypass,
            bucket_id: Some(self.bucket_id),
        };
        let read = self
            .storage
            .range_read(
                &options,
                &self.path(node_id),
                0,
                Some(S3_RESERVATION_OBJECT_LENGTH as u64),
            )
            .await;
        match read {
            Ok(bytes) => {
                if bytes.len() != S3_RESERVATION_OBJECT_LENGTH {
                    return Ok(false);
                }
                Ok(bytes == Self::target_bytes(node_id, epoch, failover))
            }
            Err(e) => {
                tracing::error!(error = %e, "check reservation object failed");
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3stream_object::MemoryObjectStorage;

    fn service() -> ObjectReservationService {
        ObjectReservationService::new(
            Arc::new(MemoryObjectStorage::new(0)),
            "clusterX".to_string(),
            0,
        )
    }

    #[test]
    fn reservation_bytes_match_java_layout() {
        let bytes = ObjectReservationService::target_bytes(3, 7, true);
        assert_eq!(bytes.len(), S3_RESERVATION_OBJECT_LENGTH);
        assert_eq!(
            &bytes[0..4],
            &S3_RESERVATION_OBJECT_MAGIC_CODE.to_be_bytes()
        );
        assert_eq!(&bytes[4..12], &3i64.to_be_bytes());
        assert_eq!(&bytes[12..20], &7i64.to_be_bytes());
        assert_eq!(bytes[20], 1);
    }

    /// A newer epoch's acquire makes the older epoch's verify return false.
    #[tokio::test]
    async fn newer_epoch_fences_older() {
        let svc = service();
        svc.acquire(1, 5, false).await.unwrap();
        assert!(svc.verify(1, 5, false).await.unwrap());

        svc.acquire(1, 6, false).await.unwrap();
        assert!(!svc.verify(1, 5, false).await.unwrap());
        assert!(svc.verify(1, 6, false).await.unwrap());
    }

    /// The failover flag participates in the byte comparison.
    #[tokio::test]
    async fn failover_flag_must_match() {
        let svc = service();
        svc.acquire(2, 1, true).await.unwrap();
        assert!(svc.verify(2, 1, true).await.unwrap());
        assert!(!svc.verify(2, 1, false).await.unwrap());
    }

    /// Missing reservation object verifies false, not an error.
    #[tokio::test]
    async fn missing_reservation_verifies_false() {
        let svc = service();
        assert!(!svc.verify(9, 1, false).await.unwrap());
    }
}
