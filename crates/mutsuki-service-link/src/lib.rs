//! Local MutsukiLink control bridge for standalone WebHost consumers.
//!
//! ServiceRuntime exposes a stable `mutsuki.servicehost` app endpoint. WebHost
//! forwards typed [`ControlRequest`] frames over Link local IPC and receives
//! [`ControlResponse`] without direct access to ServiceHost IPC handles.

mod protocol;
mod server;

pub use protocol::{
    LinkControlClientFrame, LinkControlRejectCode, LinkControlServerFrame, SERVICE_LINK_APP_ID,
};
pub use server::{LinkControlServer, LinkControlServerError};
