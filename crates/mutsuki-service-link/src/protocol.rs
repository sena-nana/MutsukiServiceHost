use mutsuki_service_control::{ControlRequest, ControlResponse};
use serde::{Deserialize, Serialize};

/// Stable ServiceHost Link app id for standalone control consumers.
pub const SERVICE_LINK_APP_ID: &str = "mutsuki.servicehost";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkControlRejectCode {
    ProtocolIncompatible,
    InvalidRequest,
    Internal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum LinkControlClientFrame {
    ControlRequest(ControlRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "frame", rename_all = "snake_case")]
pub enum LinkControlServerFrame {
    ControlResponse(ControlResponse),
    Rejected {
        code: LinkControlRejectCode,
        message: String,
    },
}
