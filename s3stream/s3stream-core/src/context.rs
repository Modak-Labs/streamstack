//! Per-call contexts threaded through append/fetch.
//!
//! `api.ReadOptions`. Tracing spans are carried by `tracing` implicitly rather than an
//! explicit TraceContext field.

#[derive(Debug, Clone, Copy, Default)]
pub struct AppendContext {}

#[derive(Debug, Clone, Copy, Default)]
pub struct FetchContext {
    /// Fail fast instead of reading from the block cache (tail consumers retry slow
    pub fast_read: bool,
    pub snapshot_read: bool,
}
