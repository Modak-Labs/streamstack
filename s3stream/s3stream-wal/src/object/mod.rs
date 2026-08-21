//! Object WAL: the WAL as a sequence of objects in a shared bucket.
//!
//! Specification: `specification/wal-protocol.md`.

pub mod config;
pub mod header;
pub mod keys;
pub mod recover;
pub mod reservation;
pub mod service;
pub mod writer;

pub use config::ObjectWalConfig;
pub use header::WalObjectHeader;
pub use keys::{
    ceil_align_offset, floor_align_offset, gen_object_path_v1, node_prefix, parse_wal_objects,
    skip_overlap_objects, WalObject, DATA_FILE_ALIGN_SIZE, TRIM_RECORD_SENTINEL,
};
pub use recover::{discover_wal_objects, recover_stream, RecoverIterator};
pub use reservation::ObjectReservationService;
pub use service::ObjectWalService;
pub use writer::ObjectWalWriter;
