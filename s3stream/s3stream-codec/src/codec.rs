//! Byte-layout constants for the encoded `StreamRecordBatch`.
//!
//! All integers are big-endian.

/// Magic byte for the v0 record batch encoding.
pub const MAGIC_V0: u8 = 0x22;

/// Position of each header field within the encoded buffer.
pub const MAGIC_POS: usize = 0;
pub const STREAM_ID_POS: usize = 1;
pub const EPOCH_POS: usize = STREAM_ID_POS + 8;
pub const BASE_OFFSET_POS: usize = EPOCH_POS + 8;
pub const LAST_OFFSET_DELTA_POS: usize = BASE_OFFSET_POS + 8;
pub const PAYLOAD_LENGTH_POS: usize = LAST_OFFSET_DELTA_POS + 4;
pub const PAYLOAD_POS: usize = PAYLOAD_LENGTH_POS + 4;

/// Total header size: magic(1) + streamId(8) + epoch(8) + baseOffset(8)
/// + lastOffsetDelta(4) + payloadLength(4).
pub const HEADER_SIZE: usize = PAYLOAD_POS;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout_matches_java() {
        // StreamRecordBatchCodec constants. These are wire-format anchors. If this
        // test ever needs changing, the format changed and conformance breaks.
        assert_eq!(HEADER_SIZE, 33);
        assert_eq!(STREAM_ID_POS, 1);
        assert_eq!(EPOCH_POS, 9);
        assert_eq!(BASE_OFFSET_POS, 17);
        assert_eq!(LAST_OFFSET_DELTA_POS, 25);
        assert_eq!(PAYLOAD_LENGTH_POS, 29);
        assert_eq!(PAYLOAD_POS, 33);
    }
}
