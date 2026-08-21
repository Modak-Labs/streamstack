//! CRC used by the WAL record framing.
//!
//! (CRC-32/ISO-HDLC) and then masks the result with `0x7FFFFFFF`. The check value
//! for `"123456789"` is `0x4BF43926`, i.e. the standard `0xCBF43926` with the top
//! bit cleared. Any implementation MUST apply the same mask.

/// Compute the WAL CRC over `data`, matching `WALUtil.crc32` bit-for-bit
/// (CRC-32/ISO-HDLC masked to 31 bits).
pub fn wal_crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data) & 0x7FFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixtures: conformance/fixtures/crc/known_answers.json.
    #[test]
    fn known_answers_match_java() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../conformance/fixtures/crc/known_answers.json");
        let json = std::fs::read_to_string(path).expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&json).unwrap();
        let cases = cases.as_array().unwrap();
        assert!(!cases.is_empty());
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let input = hex::decode(case["input_hex"].as_str().unwrap()).unwrap();
            let expected = case["crc"].as_u64().unwrap() as u32;
            assert_eq!(wal_crc32(&input), expected, "case {name}");
        }
    }

    /// The 31-bit mask is part of the wire format: the raw ISO-HDLC check value has
    /// the top bit set, the masked one must not.
    #[test]
    fn mask_applied() {
        assert_eq!(crc32fast::hash(b"123456789"), 0xCBF4_3926);
        assert_eq!(wal_crc32(b"123456789"), 0x4BF4_3926);
    }
}
