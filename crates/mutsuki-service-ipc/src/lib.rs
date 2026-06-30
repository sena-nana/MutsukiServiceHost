use std::sync::Arc;

use mutsuki_service_config::{IpcTransport, ServiceConfig};
use mutsuki_service_control::{ControlHandler, ControlRequest, ControlResponse};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::task::JoinHandle;

#[derive(Debug, thiserror::Error)]
pub enum IpcError {
    #[error("ipc transport {0:?} is not supported on this platform")]
    UnsupportedTransport(IpcTransport),
    #[error("ipc io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ipc protocol error: {0}")]
    Protocol(#[from] serde_json::Error),
}

pub type IpcResult<T> = Result<T, IpcError>;

pub struct IpcServer {
    handle: JoinHandle<()>,
}

impl IpcServer {
    pub fn abort(self) {
        self.handle.abort();
    }
}

pub async fn start_server(
    config: &ServiceConfig,
    handler: Arc<dyn ControlHandler>,
) -> IpcResult<Option<IpcServer>> {
    if !config.ipc.enabled {
        return Ok(None);
    }
    let endpoint = config.ipc_endpoint();
    let handle = match config.ipc.transport {
        IpcTransport::NamedPipe => start_named_pipe_server(endpoint, handler).await?,
        IpcTransport::UnixSocket => start_unix_socket_server(endpoint, handler).await?,
        IpcTransport::TcpDebug => start_tcp_debug_server(endpoint, handler).await?,
    };
    Ok(Some(IpcServer { handle }))
}

pub async fn request(
    config: &ServiceConfig,
    request: ControlRequest,
) -> IpcResult<ControlResponse> {
    let endpoint = config.ipc_endpoint();
    match config.ipc.transport {
        IpcTransport::NamedPipe => request_named_pipe(endpoint, request).await,
        IpcTransport::UnixSocket => request_unix_socket(endpoint, request).await,
        IpcTransport::TcpDebug => request_tcp_debug(endpoint, request).await,
    }
}

async fn serve_stream<S>(stream: S, handler: Arc<dyn ControlHandler>) -> IpcResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response = match serde_json::from_str::<ControlRequest>(&line) {
        Ok(request) => handler.handle(request).await,
        Err(error) => ControlResponse::err(mutsuki_service_control::ControlError::BadRequest(
            error.to_string(),
        )),
    };
    let payload = serde_json::to_vec(&response)?;
    writer.write_all(&payload).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

async fn send_stream<S>(stream: S, request: ControlRequest) -> IpcResult<ControlResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let payload = serde_json::to_vec(&request)?;
    writer.write_all(&payload).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    Ok(serde_json::from_str(&line)?)
}

#[cfg(windows)]
async fn start_named_pipe_server(
    name: String,
    handler: Arc<dyn ControlHandler>,
) -> IpcResult<JoinHandle<()>> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let path = named_pipe_path(&name);
    let handle = tokio::spawn(async move {
        loop {
            let server = match ServerOptions::new().create(&path) {
                Ok(server) => server,
                Err(error) => {
                    tracing::error!(error = %error, pipe = %path, "failed to create named pipe");
                    break;
                }
            };
            if let Err(error) = server.connect().await {
                tracing::warn!(error = %error, pipe = %path, "named pipe connect failed");
                continue;
            }
            let handler = handler.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_stream(server, handler).await {
                    tracing::warn!(error = %error, "named pipe request failed");
                }
            });
        }
    });
    Ok(handle)
}

#[cfg(not(windows))]
async fn start_named_pipe_server(
    _name: String,
    _handler: Arc<dyn ControlHandler>,
) -> IpcResult<JoinHandle<()>> {
    Err(IpcError::UnsupportedTransport(IpcTransport::NamedPipe))
}

#[cfg(windows)]
async fn request_named_pipe(name: String, request: ControlRequest) -> IpcResult<ControlResponse> {
    use tokio::net::windows::named_pipe::ClientOptions;
    let client = ClientOptions::new().open(named_pipe_path(&name))?;
    send_stream(client, request).await
}

#[cfg(not(windows))]
async fn request_named_pipe(_name: String, _request: ControlRequest) -> IpcResult<ControlResponse> {
    Err(IpcError::UnsupportedTransport(IpcTransport::NamedPipe))
}

#[cfg(windows)]
fn named_pipe_path(name: &str) -> String {
    if name.starts_with(r"\\.\pipe\") {
        name.to_string()
    } else {
        format!(r"\\.\pipe\{name}")
    }
}

#[cfg(unix)]
async fn start_unix_socket_server(
    path: String,
    handler: Arc<dyn ControlHandler>,
) -> IpcResult<JoinHandle<()>> {
    use tokio::net::UnixListener;
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_stream(stream, handler).await {
                            tracing::warn!(error = %error, "unix socket request failed");
                        }
                    });
                }
                Err(error) => {
                    tracing::error!(error = %error, "unix socket accept failed");
                    break;
                }
            }
        }
    });
    Ok(handle)
}

#[cfg(not(unix))]
async fn start_unix_socket_server(
    _path: String,
    _handler: Arc<dyn ControlHandler>,
) -> IpcResult<JoinHandle<()>> {
    Err(IpcError::UnsupportedTransport(IpcTransport::UnixSocket))
}

#[cfg(unix)]
async fn request_unix_socket(path: String, request: ControlRequest) -> IpcResult<ControlResponse> {
    use tokio::net::UnixStream;
    let stream = UnixStream::connect(path).await?;
    send_stream(stream, request).await
}

#[cfg(not(unix))]
async fn request_unix_socket(
    _path: String,
    _request: ControlRequest,
) -> IpcResult<ControlResponse> {
    Err(IpcError::UnsupportedTransport(IpcTransport::UnixSocket))
}

async fn start_tcp_debug_server(
    addr: String,
    handler: Arc<dyn ControlHandler>,
) -> IpcResult<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    if !peer.ip().is_loopback() {
                        tracing::warn!(peer = %peer, "rejected non-loopback debug control connection");
                        continue;
                    }
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_stream(stream, handler).await {
                            tracing::warn!(error = %error, "tcp debug request failed");
                        }
                    });
                }
                Err(error) => {
                    tracing::error!(error = %error, "tcp debug accept failed");
                    break;
                }
            }
        }
    });
    Ok(handle)
}

async fn request_tcp_debug(addr: String, request: ControlRequest) -> IpcResult<ControlResponse> {
    let stream = tokio::net::TcpStream::connect(addr).await?;
    send_stream(stream, request).await
}
