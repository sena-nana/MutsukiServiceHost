use mutsuki_service_config::{IpcCodec, IpcSection, ServiceConfig};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlIpcLimits {
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_jsonl_line_bytes: usize,
    pub max_in_flight: usize,
    pub idle_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_msgpack_nesting_depth: usize,
}

impl ControlIpcLimits {
    pub const DEFAULT_MAX_MSGPACK_NESTING_DEPTH: usize = 64;

    pub fn from_section(section: &IpcSection) -> Self {
        Self {
            max_frame_bytes: section.max_frame_bytes,
            max_payload_bytes: section.max_payload_bytes.min(section.max_frame_bytes),
            max_jsonl_line_bytes: section.max_jsonl_line_bytes,
            max_in_flight: section.max_in_flight.max(1),
            idle_timeout_ms: section.idle_timeout_ms,
            request_timeout_ms: section.request_timeout_ms,
            max_msgpack_nesting_depth: Self::DEFAULT_MAX_MSGPACK_NESTING_DEPTH,
        }
    }

    pub fn from_config(config: &ServiceConfig) -> Self {
        Self::from_section(&config.ipc)
    }
}

impl Default for ControlIpcLimits {
    fn default() -> Self {
        Self::from_section(&IpcSection::default())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlIpcProfile {
    pub codec: IpcCodec,
    pub limits: ControlIpcLimits,
}

impl ControlIpcProfile {
    pub fn from_config(config: &ServiceConfig) -> Self {
        Self {
            codec: config.ipc.codec,
            limits: ControlIpcLimits::from_config(config),
        }
    }
}
