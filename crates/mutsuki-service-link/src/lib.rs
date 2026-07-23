//! MutsukiLink control bridge for in-process and library consumers.
//!
//! When IPC is enabled, ServiceRuntime exposes a stable `mutsuki.servicehost`
//! local app endpoint via [`LinkControlServer`]. Authenticated QUIC helpers
//! ([`QuicLinkControlServer`] / [`QuicLinkControlHandler`], plus
//! [`server_config_from_pem`] / [`client_config_from_ca_pem`]) remain available
//! as library APIs for tests and non-product callers; they are not started from
//! product Host configuration.

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
