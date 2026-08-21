//! Object metadata types shared between the engine and the metadata plane.
//!

/// Sentinel ids/offsets.
pub const NOOP_OBJECT_ID: u64 = u64::MAX;
pub const NOOP_OFFSET: u64 = u64::MAX;

/// Physical object type.
///
/// (STREAM_SET = multi-stream delta upload,
/// STREAM = single-stream object).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ObjectType {
    StreamSet,
    Stream,
}

/// Packed object attributes.
///
/// A bit-packed i32 that round-trips through the metadata plane. Bit layout:
/// - bits 0..=1: object type (0 = normal, 1 = composite)
/// - bits 2..=17: bucket index
/// - bit 18: deep-delete mark
/// - bits 19..=31: unused
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ObjectAttributes(pub u32);

const DEEP_DELETE_MASK: u32 = 1 << 18;

impl ObjectAttributes {
    pub fn new(bucket_id: i16, composite: bool, deep_delete: bool) -> Self {
        let mut attributes: u32 = 0;
        if composite {
            attributes |= 1;
        }
        attributes |= ((bucket_id as u16) as u32) << 2;
        if deep_delete {
            attributes |= DEEP_DELETE_MASK;
        }
        Self(attributes)
    }

    pub fn bucket_id(self) -> i16 {
        ((self.0 >> 2) & 0xFFFF) as u16 as i16
    }

    pub fn is_composite(self) -> bool {
        (self.0 & 0x3) == 1
    }

    pub fn deep_delete(self) -> bool {
        (self.0 & DEEP_DELETE_MASK) != 0
    }
}

/// A stream's offset range within an object, as tracked by the metadata plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOffsetRange {
    pub stream_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
}

/// Metadata describing one committed object, as returned by the metadata plane.
///
/// `getObjects` may return the same physical
/// object multiple times as different logical slices. Readers treat each instance
/// independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectMetadata {
    pub object_id: u64,
    pub object_type: S3ObjectType,
    /// Offset ranges of the streams inside this object (slice view).
    pub offset_ranges: Vec<StreamOffsetRange>,
    pub object_size: u64,
    pub attributes: ObjectAttributes,
    /// Commit timestamp (ms). Used by compaction policies.
    pub committed_timestamp_ms: i64,
    /// Data timestamp (ms). Earliest data in the object, for retention.
    pub data_timestamp_ms: i64,
}

impl S3ObjectMetadata {
    /// The object's storage key.
    pub fn key(&self) -> String {
        gen_object_key(0, self.object_id)
    }
}

/// The namespace baked into object keys. Hosts override it per call via
/// `gen_object_key_in`. There is no global mutable state.
pub const DEFAULT_NAMESPACE: &str = "DEFAULT";

/// Generate the storage key for a committed object (default namespace).
pub fn gen_object_key(version: u8, object_id: u64) -> String {
    gen_object_key_in(version, DEFAULT_NAMESPACE, object_id)
}

/// Generate the storage key for a committed object in `namespace`.
///
/// `reverse(format("%08x", objectId)) + "/" + namespace + "/" + objectId`.
/// whole hex string is reversed. Verified against `keys/object_keys.json` fixtures.
pub fn gen_object_key_in(version: u8, namespace: &str, object_id: u64) -> String {
    assert_eq!(
        version, 0,
        "only key scheme v0 exists (Java throws on others)"
    );
    let hex: String = format!("{object_id:08x}").chars().rev().collect();
    format!("{hex}/{namespace}/{object_id}")
}

/// Storage key of a node's persisted sparse range index (default namespace).
pub fn gen_index_key(version: u8, node_id: u64) -> String {
    gen_index_key_in(version, DEFAULT_NAMESPACE, node_id)
}

/// Key layout is pinned by the `range_index/manifest.json` fixtures.
pub fn gen_index_key_in(version: u8, namespace: &str, node_id: u64) -> String {
    assert_eq!(
        version, 0,
        "only index key scheme v0 exists (Java throws on others)"
    );
    let hash = java_string_hash(&format!("sparse-index{node_id}"));
    format!("{:08x}/{namespace}/node-{node_id}", hash as u32)
}

fn java_string_hash(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |h, c| h.wrapping_mul(31).wrapping_add(c as i32))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keys must match Java `ObjectUtils.genKey` byte-for-byte.
    /// Fixtures: conformance/fixtures/keys/object_keys.json.
    #[test]
    fn object_keys_match_java() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/keys/object_keys.json");
        let json = std::fs::read_to_string(path).expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&json).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let namespace = case["namespace"].as_str().unwrap();
            let object_id = case["object_id"].as_u64().unwrap();
            let expected = case["key"].as_str().unwrap();
            assert_eq!(gen_object_key_in(0, namespace, object_id), expected);
        }
    }

    /// Bit packing must match Java ObjectAttributes exactly.
    #[test]
    fn attributes_bit_layout_matches_java() {
        let attributes = ObjectAttributes::new(5, true, true);
        assert_eq!(attributes.0, 1 | (5 << 2) | (1 << 18));
        assert_eq!(attributes.bucket_id(), 5);
        assert!(attributes.is_composite());
        assert!(attributes.deep_delete());

        let plain = ObjectAttributes::new(0, false, false);
        assert_eq!(plain.0, 0);
        assert!(!plain.is_composite());
        assert!(!plain.deep_delete());
    }
}
