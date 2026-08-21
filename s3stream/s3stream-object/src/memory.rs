//! In-memory `ObjectStorage` for tests and simulation. A working
//! implementation (not a stub) so every layer above can be tested without a
//! network. Deterministic-simulation failure injection hooks come with the
//! test harness later.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use crate::error::ObjectError;
use crate::storage::{
    MultipartWriter, ObjectInfo, ObjectPath, ObjectStorage, ReadOptions, WriteOptions, WriteResult,
};

#[derive(Default)]
struct Store {
    objects: BTreeMap<String, (Bytes, i64)>,
}

#[derive(Clone, Default)]
pub struct MemoryObjectStorage {
    store: Arc<Mutex<Store>>,
    bucket_id: i16,
}

impl MemoryObjectStorage {
    pub fn new(bucket_id: i16) -> Self {
        Self {
            store: Arc::default(),
            bucket_id,
        }
    }

    fn now_ms() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

#[async_trait]
impl ObjectStorage for MemoryObjectStorage {
    async fn readiness_check(&self) -> Result<(), ObjectError> {
        Ok(())
    }

    async fn range_read(
        &self,
        _options: &ReadOptions,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Bytes, ObjectError> {
        let store = self.store.lock().unwrap();
        let (data, _) = store
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::NotFound {
                key: key.to_string(),
            })?;
        let len = data.len() as u64;
        let start = start.min(len);
        let end = end.unwrap_or(len).min(len);
        Ok(data.slice(start as usize..end.max(start) as usize))
    }

    async fn write(
        &self,
        _options: &WriteOptions,
        key: &str,
        data: Bytes,
    ) -> Result<WriteResult, ObjectError> {
        let mut store = self.store.lock().unwrap();
        store
            .objects
            .insert(key.to_string(), (data, Self::now_ms()));
        Ok(WriteResult {
            bucket_id: self.bucket_id,
        })
    }

    async fn writer(
        &self,
        _options: &WriteOptions,
        key: &str,
    ) -> Result<Box<dyn MultipartWriter>, ObjectError> {
        Ok(Box::new(MemoryMultipartWriter {
            storage: self.clone(),
            key: key.to_string(),
            buf: BytesMut::new(),
        }))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectError> {
        let store = self.store.lock().unwrap();
        Ok(store
            .objects
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, (data, ts))| ObjectInfo {
                path: ObjectPath {
                    bucket_id: self.bucket_id,
                    key: k.clone(),
                },
                timestamp_ms: *ts,
                size: data.len() as u64,
            })
            .collect())
    }

    async fn delete(&self, paths: &[ObjectPath]) -> Result<(), ObjectError> {
        let mut store = self.store.lock().unwrap();
        for path in paths {
            store.objects.remove(&path.key);
        }
        Ok(())
    }

    fn bucket_id(&self) -> i16 {
        self.bucket_id
    }
}

struct MemoryMultipartWriter {
    storage: MemoryObjectStorage,
    key: String,
    buf: BytesMut,
}

#[async_trait]
impl MultipartWriter for MemoryMultipartWriter {
    async fn write(&mut self, part: Bytes) -> Result<(), ObjectError> {
        self.buf.extend_from_slice(&part);
        Ok(())
    }

    async fn finish(&mut self) -> Result<WriteResult, ObjectError> {
        let data = self.buf.split().freeze();
        self.storage
            .write(&WriteOptions::default(), &self.key, data)
            .await
    }

    async fn abort(&mut self) -> Result<(), ObjectError> {
        Ok(())
    }

    fn bucket_id(&self) -> i16 {
        self.storage.bucket_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_roundtrip() {
        let storage = MemoryObjectStorage::new(0);
        let opts = WriteOptions::default();
        storage
            .write(&opts, "a/1", Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let read = storage
            .range_read(&ReadOptions::default(), "a/1", 1, Some(4))
            .await
            .unwrap();
        assert_eq!(&read[..], b"ell");
    }

    #[tokio::test]
    async fn missing_key_is_not_found() {
        let storage = MemoryObjectStorage::new(0);
        let err = storage
            .range_read(&ReadOptions::default(), "nope", 0, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectError::NotFound { .. }));
    }

    #[tokio::test]
    async fn list_respects_prefix_and_delete_is_idempotent() {
        let storage = MemoryObjectStorage::new(0);
        let opts = WriteOptions::default();
        storage
            .write(&opts, "wal/1", Bytes::from_static(b"x"))
            .await
            .unwrap();
        storage
            .write(&opts, "wal/2", Bytes::from_static(b"y"))
            .await
            .unwrap();
        storage
            .write(&opts, "data/1", Bytes::from_static(b"z"))
            .await
            .unwrap();

        let listed = storage.list("wal/").await.unwrap();
        assert_eq!(listed.len(), 2);

        let paths: Vec<ObjectPath> = listed.iter().map(|info| info.path.clone()).collect();
        storage.delete(&paths).await.unwrap();
        storage.delete(&paths).await.unwrap(); // idempotent
        assert!(storage.list("wal/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn multipart_writer_concatenates_parts() {
        let storage = MemoryObjectStorage::new(0);
        let mut writer = storage
            .writer(&WriteOptions::default(), "mp/1")
            .await
            .unwrap();
        writer.write(Bytes::from_static(b"ab")).await.unwrap();
        writer.write(Bytes::from_static(b"cd")).await.unwrap();
        writer.finish().await.unwrap();
        let read = storage.read(&ReadOptions::default(), "mp/1").await.unwrap();
        assert_eq!(&read[..], b"abcd");
    }
}
