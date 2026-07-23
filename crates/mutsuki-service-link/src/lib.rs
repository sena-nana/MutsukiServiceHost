//! Local MutsukiLink control bridge for standalone WebHost consumers.
//!
//! ServiceRuntime exposes a stable `mutsuki.servicehost` app endpoint. WebHost
//! forwards typed [`ControlRequest`] frames over Link local IPC and receives
//! [`ControlResponse`] without direct access to ServiceHost IPC handles.

mod client;
mod protocol;
mod server;
mod transport;

pub use client::{
    LinkControlHandler, STANDALONE_LINK_CONNECT_FAILED, STANDALONE_LINK_PROTOCOL_ERROR,
    STANDALONE_LINK_REJECTED,
};
pub use protocol::{
    LinkControlClientFrame, LinkControlRejectCode, LinkControlServerFrame, SERVICE_LINK_APP_ID,
};
pub use server::{LinkControlServer, LinkControlServerError};

#[cfg(test)]
pub(crate) static LINK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
