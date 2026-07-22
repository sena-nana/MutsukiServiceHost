mod client;
mod codec;
mod error;
mod frame;
mod io;
mod limits;
mod server_conn;
mod session;
mod transport;

use std::path::Path;

pub use client::ControlClient;
pub use error::{IpcError, IpcResult};
pub use frame::{BINARY_HEADER_LEN, BINARY_LENGTH_PREFIX_LEN, CONTROL_WIRE_MAGIC};
pub use limits::{ControlIpcLimits, ControlIpcProfile};
pub use mutsuki_service_config::{IpcCodec, IpcTransport};
pub use session::{ControlClientConfig, ControlSession, request_oneshot};
pub use transport::{IpcServer, start_server};

pub fn default_control_endpoint(
    transport: IpcTransport,
    name: &str,
    run_dir: &Path,
    tcp_debug_addr: Option<&str>,
) -> String {
    match transport {
        IpcTransport::NamedPipe => name.to_string(),
        IpcTransport::UnixSocket => run_dir
            .join(format!("{name}.sock"))
            .to_string_lossy()
            .into_owned(),
        IpcTransport::TcpDebug => tcp_debug_addr.unwrap_or("127.0.0.1:7687").to_string(),
    }
}

#[cfg(test)]
mod tests;
