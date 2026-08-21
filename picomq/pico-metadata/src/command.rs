//! The replicated command set: the *only* way metadata changes.
//!
//! Fourteen variants with stable type codes (see the codec).
//!
//! Why a closed command enum (and not methods mutating state): every mutation is
//! a value that goes through the consensus log, so the state machine is
//! deterministic and replayable, duplicate delivery is handled by idempotency
//! rules in `apply` (not by callers), and adding a command is an explicit,
//! versioned protocol change. Commands carry their own timestamps (`now_ms`).
//! Must not read the clock.

use s3stream::{CommitStreamSetObjectRequest, CompactStreamObjectRequest, StreamMetadata};

/// A command in the replicated metadata log.
///
/// Field types follow the Rust
/// as the "never opened" stream epoch (`StreamControlManager#createStream`).
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataCommand {
    /// Registers/refreshes a node
    /// epoch and its advertised HTTP address. An older epoch is fenced.
    RegisterNode {
        node_id: i32,
        node_epoch: i64,
        http_address: String,
        slots: u32,
    },

    PlaceStream {
        stream_id: u64,
    },

    /// Assigns the next stream id.
    /// The new stream starts CLOSED with epoch -1.
    CreateStream {
        node_id: i32,
        node_epoch: i64,
    },

    /// Bumps the stream epoch
    /// (fencing older writers) and records the owner node. Re-open with the same
    /// (epoch, node) is idempotent.
    OpenStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    /// Advances the retention
    /// watermark (start offset) of an opened stream.
    TrimStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
        new_start_offset: u64,
    },

    /// Releases ownership. A later
    /// `OpenStream` with a newer epoch may move the stream to another node.
    CloseStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    /// Removes a CLOSED stream and marks all of its stream objects destroyed.
    DeleteStream {
        node_id: i32,
        node_epoch: i64,
        stream_id: u64,
        epoch: i64,
    },

    /// Leases `count` consecutive
    /// object ids with a TTL. Uncommitted ids expire via [`Self::ExpirePreparedObjects`].
    PrepareObject {
        node_id: i32,
        node_epoch: i64,
        count: u32,
        ttl_ms: i64,
        now_ms: i64,
    },

    /// Atomically commits
    /// one delta-WAL upload (or stream-set compaction): the stream-set object, its
    /// split stream objects, end-offset advances, and compacted-object destruction.
    CommitStreamSetObject {
        node_id: i32,
        node_epoch: i64,
        request: CommitStreamSetObjectRequest,
        now_ms: i64,
    },

    /// Commits one
    /// stream-object compaction: replacement object in, source objects marked
    /// destroyed per their `CompactOperations`.
    CompactStreamObject {
        node_id: i32,
        node_epoch: i64,
        request: CompactStreamObjectRequest,
        now_ms: i64,
    },

    /// Reclaims prepared
    /// object ids whose deadline passed (driven by the leader's timer, `now_ms`
    /// rides in the command so replay is deterministic).
    ExpirePreparedObjects {
        now_ms: i64,
    },

    /// Acknowledges that
    /// the object cleaner physically deleted these ids.
    CleanDestroyedObjects {
        object_ids: Vec<u64>,
    },

    PutKv {
        key: String,
        value: bytes::Bytes,
    },

    /// Returns the existing value
    /// when present (no overwrite).
    PutKvIfAbsent {
        key: String,
        value: bytes::Bytes,
    },

    /// Returns the removed value.
    DeleteKv {
        key: String,
    },
    // ------------------------------------------------------------------
    // Reserved type codes. Do not reuse:
    //   15 = TransferStream   { stream_id, from_node, to_node }
    //   16 = CompleteTransfer { stream_id, epoch }
    //   17 = CreateStreams    { node_id, node_epoch, count }   (batched create)
    // live ownership transfer and bulk creation. See the
    // metadata/server design notes. Added in a later phase as versioned protocol
    // extensions. Reserving the codes here keeps old logs decodable forever.
    // ------------------------------------------------------------------
}

impl MetadataCommand {
    pub fn type_code(&self) -> u8 {
        match self {
            MetadataCommand::CreateStream { .. } => 1,
            MetadataCommand::OpenStream { .. } => 2,
            MetadataCommand::TrimStream { .. } => 3,
            MetadataCommand::CloseStream { .. } => 4,
            MetadataCommand::DeleteStream { .. } => 5,
            MetadataCommand::PrepareObject { .. } => 6,
            MetadataCommand::CommitStreamSetObject { .. } => 7,
            MetadataCommand::CompactStreamObject { .. } => 8,
            MetadataCommand::ExpirePreparedObjects { .. } => 9,
            MetadataCommand::RegisterNode { .. } => 10,
            MetadataCommand::CleanDestroyedObjects { .. } => 11,
            MetadataCommand::PutKv { .. } => 12,
            MetadataCommand::PutKvIfAbsent { .. } => 13,
            MetadataCommand::DeleteKv { .. } => 14,
            MetadataCommand::PlaceStream { .. } => 18,
        }
    }
}

/// The result of applying one command.
///
/// `Long`, `Integer`, `StreamMetadata`, `CommitStreamSetObjectResponse`, or
/// `byte[]`. This enum is the typed version of that union.
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataResult {
    Unit,
    /// `CreateStream` → assigned stream id.`PrepareObject` → first leased object
    Id(u64),
    Count(u64),
    Stream(StreamMetadata),
    /// `PutKVIfAbsent` → existing-or-inserted value.`DeleteKV` → removed value
    /// (`None` if the key was absent).
    Value(Option<bytes::Bytes>),
}
