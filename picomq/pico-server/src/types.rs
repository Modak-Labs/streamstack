//! Service model types.

use bytes::Bytes;

use crate::error::{ErrorKind, ServiceError};

/// Opaque, order-preserving stream position token.
///
/// The wire form is the record offset zero-padded to 20 digits so
/// lexicographic order equals numeric order. `parse` accepts negative input
/// as "beginning".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OffsetToken {
    record_offset: u64,
}

impl OffsetToken {
    const WIDTH: usize = 20;

    pub fn beginning() -> Self {
        Self::of_record_offset(0)
    }

    pub fn of_record_offset(record_offset: u64) -> Self {
        Self { record_offset }
    }

    /// Any negative value such as `"-1"` means the beginning. Empty or
    /// non-numeric input is an error.
    pub fn parse(raw: Option<&str>) -> Result<Self, ServiceError> {
        let raw = match raw {
            None | Some("-1") => return Ok(Self::beginning()),
            Some(r) => r,
        };
        if raw.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "empty offset",
            ));
        }
        match raw.parse::<i64>() {
            Ok(offset) if offset < 0 => Ok(Self::beginning()),
            Ok(offset) => Ok(Self::of_record_offset(offset as u64)),
            Err(_) => Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                format!("invalid offset token: {raw}"),
            )),
        }
    }

    pub fn value(&self) -> String {
        format!("{:0width$}", self.record_offset, width = Self::WIDTH)
    }

    pub fn record_offset(&self) -> u64 {
        self.record_offset
    }
}

impl std::fmt::Display for OffsetToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.value())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Producer {
    pub producer_id: String,
    pub epoch: u64,
    pub seq: u64,
}

impl Producer {
    pub fn new(producer_id: impl Into<String>, epoch: u64, seq: u64) -> Result<Self, ServiceError> {
        let producer_id = producer_id.into();
        if producer_id.is_empty() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "producerId must not be empty",
            ));
        }
        Ok(Self {
            producer_id,
            epoch,
            seq,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: OffsetToken,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct CreateCommand {
    pub name: String,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub closed: bool,
    pub initial_payload: Bytes,
}

impl CreateCommand {
    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.ttl_seconds.is_some() && self.expires_at_ms.is_some() {
            return Err(ServiceError::with_message(
                ErrorKind::BadRequest,
                None,
                false,
                "ttlSeconds and expiresAt are mutually exclusive",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateResult {
    pub created: bool,
    pub meta: StreamMeta,
}

/// `streamSeq` stays a `String` compared
/// tail record offset.
#[derive(Debug, Clone, Default)]
pub struct AppendCommand {
    pub name: String,
    pub payloads: Vec<Bytes>,
    pub content_type: Option<String>,
    pub stream_seq: Option<String>,
    pub match_seq: Option<u64>,
    pub producer: Option<Producer>,
    pub close_after: bool,
    pub atomic: bool,
}

impl AppendCommand {
    pub fn normalized(mut self) -> Self {
        if self.stream_seq.as_deref() == Some("") {
            self.stream_seq = None;
        }
        self
    }

    pub fn payload_len(&self) -> usize {
        self.payloads.iter().map(|p| p.len()).sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResult {
    pub next_offset: OffsetToken,
    pub applied: bool,
    pub closed: bool,
    pub producer_epoch: Option<u64>,
    pub producer_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseResult {
    pub next_offset: OffsetToken,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub records: Vec<StreamRecord>,
    pub content_type: String,
    pub next_offset: OffsetToken,
    pub up_to_date: bool,
    pub closed: bool,
}

impl ReadResult {
    pub fn concatenated(&self) -> Bytes {
        let mut out = Vec::with_capacity(self.records.iter().map(|r| r.payload.len()).sum());
        for record in &self.records {
            out.extend_from_slice(&record.payload);
        }
        Bytes::from(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMeta {
    pub name: String,
    pub stream_id: u64,
    pub content_type: String,
    pub ttl_seconds: Option<u64>,
    pub expires_at_ms: Option<i64>,
    pub start_offset: OffsetToken,
    pub next_offset: OffsetToken,
    pub submitted_offset: OffsetToken,
    pub closed: bool,
}

#[derive(Debug, Clone)]
pub struct StreamList {
    pub streams: Vec<StreamMeta>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeMeta {
    pub node_id: i32,
    pub advertised_address: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Owner {
    pub stream_id: Option<u64>,
    pub local: bool,
    pub owner_node_id: Option<i32>,
    pub owner_advertised_address: Option<String>,
}

impl Owner {
    pub fn local(stream_id: Option<u64>) -> Self {
        Self {
            stream_id,
            local: true,
            owner_node_id: None,
            owner_advertised_address: None,
        }
    }

    pub fn remote(stream_id: u64, owner_node_id: i32, owner_advertised_address: String) -> Self {
        Self {
            stream_id: Some(stream_id),
            local: false,
            owner_node_id: Some(owner_node_id),
            owner_advertised_address: Some(owner_advertised_address),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_token_parse_and_format() {
        assert_eq!(OffsetToken::parse(None).unwrap(), OffsetToken::beginning());
        assert_eq!(
            OffsetToken::parse(Some("-1")).unwrap(),
            OffsetToken::beginning()
        );
        assert_eq!(
            OffsetToken::parse(Some("-7")).unwrap(),
            OffsetToken::beginning()
        );
        assert_eq!(OffsetToken::parse(Some("42")).unwrap().record_offset(), 42);
        assert!(OffsetToken::parse(Some("")).is_err());
        assert!(OffsetToken::parse(Some("nope")).is_err());
        assert_eq!(
            OffsetToken::of_record_offset(7).value(),
            "00000000000000000007"
        );
        assert_eq!(OffsetToken::of_record_offset(7).value().len(), 20);
        // Lexicographic order == numeric order (the point of the padding).
        assert!(
            OffsetToken::of_record_offset(9).value() < OffsetToken::of_record_offset(10).value()
        );
    }

    #[test]
    fn create_command_validation() {
        let cmd = CreateCommand {
            name: "/a".into(),
            content_type: "text/plain".into(),
            ttl_seconds: Some(5),
            expires_at_ms: Some(10),
            closed: false,
            initial_payload: Bytes::new(),
        };
        assert!(cmd.validate().is_err());
    }

    #[test]
    fn producer_rejects_empty_id() {
        assert!(Producer::new("", 0, 0).is_err());
        assert!(Producer::new("p", 0, 0).is_ok());
    }
}
