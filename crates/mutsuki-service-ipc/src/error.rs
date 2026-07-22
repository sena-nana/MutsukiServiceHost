use thiserror::Error;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("ipc transport {0:?} is not supported on this platform")]
    UnsupportedTransport(mutsuki_service_config::IpcTransport),
    #[error("ipc io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ipc protocol error: {0}")]
    Protocol(String),
    #[error("json decode error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame oversized: actual={actual} limit={limit}")]
    FrameOversized { actual: usize, limit: usize },
    #[error("payload oversized: actual={actual} limit={limit}")]
    PayloadOversized { actual: usize, limit: usize },
    #[error("jsonl line oversized: actual={actual} limit={limit}")]
    JsonlLineOversized { actual: usize, limit: usize },
    #[error("truncated frame: expected={expected} actual={actual}")]
    Truncated { expected: usize, actual: usize },
    #[error("unknown opcode: {0:#06x}")]
    UnknownOpcode(u16),
    #[error("unknown flags: {0:#06x}")]
    UnknownFlags(u16),
    #[error("invalid magic: {0:#010x}")]
    InvalidMagic(u32),
    #[error("invalid request id")]
    InvalidRequestId,
    #[error("duplicate response for request {0}")]
    DuplicateResponse(u64),
    #[error("late response for unknown request {0}")]
    LateResponse(u64),
    #[error("pending request limit exceeded ({0})")]
    PendingLimitExceeded(usize),
    #[error("request timed out")]
    Timeout,
    #[error("request cancelled")]
    Cancelled,
    #[error("connection closed")]
    Closed,
    #[error("server is draining")]
    Draining,
}

pub type IpcResult<T> = Result<T, IpcError>;

impl From<rmp_serde::encode::Error> for IpcError {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self::Protocol(value.to_string())
    }
}

impl From<rmp_serde::decode::Error> for IpcError {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self::Protocol(value.to_string())
    }
}
