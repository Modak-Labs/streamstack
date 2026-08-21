//! LRU cache of open object readers (parsed index blocks).
//!
//! Repeated reads of one object parse its footer and index once. The cache is
//! size-bounded (`MAX_OBJECT_READER_SIZE`) and evicts least-recently-used
//! readers on overflow. `Arc<ObjectReader>` keeps evicted-but-in-use readers
//! alive, and `ObjectReader::cached_index_bytes` reports 0 until the index
//! actually loads, which makes the accounting eventually consistent.

use std::sync::{Arc, Mutex};

use s3stream_object::{ObjectReader, ObjectStorage, S3ObjectMetadata};

pub const MAX_OBJECT_READER_SIZE: usize = 100 * 1024 * 1024;

pub struct ObjectReaderCache {
    storage: Arc<dyn ObjectStorage>,
    max_index_bytes: usize,
    state: Mutex<lru::LruCache<u64, Arc<ObjectReader>>>,
}

impl ObjectReaderCache {
    pub fn new(storage: Arc<dyn ObjectStorage>) -> Self {
        Self::with_capacity(storage, MAX_OBJECT_READER_SIZE)
    }

    pub fn with_capacity(storage: Arc<dyn ObjectStorage>, max_index_bytes: usize) -> Self {
        Self {
            storage,
            max_index_bytes,
            state: Mutex::new(lru::LruCache::unbounded()),
        }
    }

    /// Get-or-open a reader for `metadata`.
    pub fn get(&self, metadata: &S3ObjectMetadata) -> Arc<ObjectReader> {
        let mut lru = self.state.lock().expect("reader cache poisoned");
        if let Some(reader) = lru.get(&metadata.object_id) {
            return Arc::clone(reader);
        }
        let reader = Arc::new(ObjectReader::new(
            metadata.clone(),
            Arc::clone(&self.storage),
        ));
        lru.push(metadata.object_id, Arc::clone(&reader));
        // Evict while cached index bytes exceed the cap (never evict the newest).
        while lru.len() > 1 {
            let total: usize = lru.iter().map(|(_, r)| r.cached_index_bytes()).sum();
            if total <= self.max_index_bytes {
                break;
            }
            lru.pop_lru();
        }
        reader
    }

    pub fn storage(&self) -> &Arc<dyn ObjectStorage> {
        &self.storage
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.state.lock().expect("reader cache poisoned").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3stream_object::MemoryObjectStorage;
    use s3stream_object::{ObjectAttributes, S3ObjectType};

    fn metadata(object_id: u64) -> S3ObjectMetadata {
        S3ObjectMetadata {
            object_id,
            object_type: S3ObjectType::StreamSet,
            offset_ranges: vec![],
            object_size: 0,
            attributes: ObjectAttributes::new(0, false, false),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        }
    }

    /// Same object id returns the same reader. Unrelated ids get their own.
    #[test]
    fn readers_are_shared_per_object() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let cache = ObjectReaderCache::new(storage as Arc<dyn ObjectStorage>);
        let a1 = cache.get(&metadata(1));
        let a2 = cache.get(&metadata(1));
        let b = cache.get(&metadata(2));
        assert!(Arc::ptr_eq(&a1, &a2));
        assert!(!Arc::ptr_eq(&a1, &b));
        assert_eq!(cache.len(), 2);
    }
}
