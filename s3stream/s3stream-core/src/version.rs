//! Cluster feature-level gating.
//!
//! The engine gates format/behavior choices on the feature level so
//! mixed-version clusters stay compatible:
//! - `V1`: stream object compaction v1 (composite objects) + WAL registration.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Version {
    V0,
    V1,
}

impl Version {
    pub const LATEST: Version = Version::V1;

    pub fn from_feature_level(level: i16) -> Option<Version> {
        match level {
            1 => Some(Version::V0),
            2 => Some(Version::V1),
            _ => None,
        }
    }

    pub fn feature_level(self) -> i16 {
        match self {
            Version::V0 => 1,
            Version::V1 => 2,
        }
    }

    /// Composite-object-based stream object compaction allowed.
    pub fn stream_object_compact_v1_supported(self) -> bool {
        self >= Version::V1
    }

    /// WAL registration with the metadata plane allowed.
    pub fn wal_registration_supported(self) -> bool {
        self >= Version::V1
    }
}

#[cfg(test)]
mod tests {
    use super::Version;

    #[test]
    fn feature_levels_match_java() {
        assert_eq!(Version::from_feature_level(1), Some(Version::V0));
        assert_eq!(Version::from_feature_level(2), Some(Version::V1));
        assert_eq!(Version::from_feature_level(3), None);
        assert!(!Version::V0.stream_object_compact_v1_supported());
        assert!(Version::V1.wal_registration_supported());
        assert_eq!(Version::LATEST, Version::V1);
    }
}
