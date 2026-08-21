//! Object format and object-storage abstraction for s3stream.
//!
//! Contains the on-object byte format (data blocks, index block, footer, composite
//! objects. See `specification/object-format.md`), object key/metadata types, and the
//! `ObjectStorage` trait that everything above uses to talk to S3-compatible storage.
//!
//! Layering rule: crates above (`s3stream-wal`, `s3stream-core`) depend on the
//! `ObjectStorage` trait here and never on `object_store` directly, so test backends
//! (in-memory, fault-injecting, deterministic-simulation) are swappable everywhere.

pub mod composite;
pub mod error;
pub mod index;
pub mod memory;
pub mod metadata;
pub mod reader;
pub mod storage;
pub mod writer;

pub use error::ObjectError;
pub use index::{DataBlockIndex, FindIndexResult, IndexBlock, BLOCK_INDEX_SIZE};
pub use memory::MemoryObjectStorage;
pub use metadata::{
    gen_index_key, gen_index_key_in, gen_object_key, ObjectAttributes, S3ObjectMetadata,
    S3ObjectType, StreamOffsetRange, NOOP_OBJECT_ID, NOOP_OFFSET,
};
pub use reader::{decode_data_block, ObjectReader};
pub use storage::{
    IdUri, MultipartWriter, ObjectInfo, ObjectPath, ObjectStorage, ObjectStoreAdapter, ReadOptions,
    ThrottleStrategy, WriteOptions, WriteResult,
};
pub use writer::{ObjectStreamRange, ObjectWriter, FOOTER_MAGIC, FOOTER_SIZE};
