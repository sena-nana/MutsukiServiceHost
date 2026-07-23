//! MutsukiLink control bridge for standalone WebHost consumers.
//!
//! ServiceRuntime exposes a stable `mutsuki.servicehost` local app endpoint.
//! Authenticated QUIC is available via [`QuicLinkControlServer`] /
//! [`QuicLinkControlHandler`] with caller-injected TLS identity (see
//! [`server_config_from_pem`] / [`client_config_from_ca_pem`]). WebHost
//! forwards typed [`ControlRequest`] frames and receives [`ControlResponse`]
//! without copying the control protocol.

mod client;
mod protocol;
mod quic;
mod server;
mod tls_identity;
mod transport;

pub use client::{
    LinkControlHandler, STANDALONE_LINK_CONNECT_FAILED, STANDALONE_LINK_PROTOCOL_ERROR,
    STANDALONE_LINK_REJECTED,
};
pub use protocol::{
    LinkControlClientFrame, LinkControlRejectCode, LinkControlServerFrame, SERVICE_LINK_APP_ID,
};
pub use quic::{QuicLinkControlHandler, QuicLinkControlServer, STANDALONE_LINK_QUIC_UNAVAILABLE};
pub use server::{LinkControlServer, LinkControlServerError};
pub use tls_identity::{TlsIdentityError, client_config_from_ca_pem, server_config_from_pem};

#[cfg(test)]
pub(crate) static LINK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
