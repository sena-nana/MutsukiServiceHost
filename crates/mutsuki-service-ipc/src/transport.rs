use std::path::PathBuf;
use std::sync::Arc;

use mutsuki_service_config::{IpcTransport, ServiceConfig};
use mutsuki_service_control::ControlHandler;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{IpcError, IpcResult};
use crate::limits::ControlIpcProfile;
use crate::server_conn::serve_stream;

pub struct IpcServer {
    handle: JoinHandle<()>,
    cleanup_path: Option<PathBuf>,
    drain_tx: watch::Sender<bool>,
}

impl IpcServer {
    pub fn abort(self) {
        let _ = self.drain_tx.send(true);
        self.handle.abort();
        remove_cleanup_path(self.cleanup_path.as_deref());
    }

    pub async fn shutdown(self) {
        let _ = self.drain_tx.send(true);
        self.handle.abort();
        let _ = self.handle.await;
        remove_cleanup_path(self.cleanup_path.as_deref());
    }

    pub fn begin_drain(&self) {
        let _ = self.drain_tx.send(true);
    }
}

fn remove_cleanup_path(path: Option<&std::path::Path>) {
    let Some(path) = path else {
        return;
    };
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %error, path = %path.display(), "failed to remove IPC endpoint");
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
    let profile = ControlIpcProfile::from_config(config);
    let (drain_tx, drain_rx) = watch::channel(false);
    let (handle, cleanup_path) = match config.ipc.transport {
        IpcTransport::NamedPipe => (
            start_named_pipe_server(endpoint, handler, profile, drain_rx).await?,
            None,
        ),
        IpcTransport::UnixSocket => {
            let cleanup_path = PathBuf::from(&endpoint);
            (
                start_unix_socket_server(endpoint, handler, profile, drain_rx).await?,
                Some(cleanup_path),
            )
        }
        IpcTransport::TcpDebug => (
            start_tcp_debug_server(endpoint, handler, profile, drain_rx).await?,
            None,
        ),
    };
    Ok(Some(IpcServer {
        handle,
        cleanup_path,
        drain_tx,
    }))
}

fn spawn_connection<S>(
    stream: S,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    drain_rx: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = serve_stream(stream, handler, profile, drain_rx).await {
            tracing::warn!(error = %error, "control connection failed");
        }
    });
}

#[cfg(windows)]
async fn start_named_pipe_server(
    name: String,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    drain_rx: watch::Receiver<bool>,
) -> IpcResult<JoinHandle<()>> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let path = named_pipe_path(&name);
    let first_server = ServerOptions::new().create(&path)?;
    let handle = tokio::spawn(async move {
        let mut server = first_server;
        let mut drain_rx = drain_rx;
        loop {
            if *drain_rx.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = drain_rx.changed() => {
                    if changed.is_err() || *drain_rx.borrow() {
                        break;
                    }
                }
                connect = server.connect() => {
                    match connect {
                        Ok(()) => {
                            let handler = handler.clone();
                            let profile = profile;
                            let drain_rx = drain_rx.clone();
                            let connected = server;
                            tokio::spawn(async move {
                                if let Err(error) =
                                    serve_stream(connected, handler, profile, drain_rx).await
                                {
                                    tracing::warn!(error = %error, "named pipe request failed");
                                }
                            });
                            server = match ServerOptions::new().create(&path) {
                                Ok(server) => server,
                                Err(error) => {
                                    tracing::error!(error = %error, pipe = %path, "failed to create named pipe");
                                    break;
                                }
                            };
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, pipe = %path, "named pipe connect failed");
                        }
                    }
                }
            }
        }
    });
    Ok(handle)
}

#[cfg(not(windows))]
async fn start_named_pipe_server(
    _name: String,
    _handler: Arc<dyn ControlHandler>,
    _profile: ControlIpcProfile,
    _drain_rx: watch::Receiver<bool>,
) -> IpcResult<JoinHandle<()>> {
    Err(IpcError::UnsupportedTransport(IpcTransport::NamedPipe))
}

#[cfg(windows)]
pub(crate) fn named_pipe_path(name: &str) -> String {
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
    profile: ControlIpcProfile,
    drain_rx: watch::Receiver<bool>,
) -> IpcResult<JoinHandle<()>> {
    use tokio::net::UnixListener;
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    let handle = tokio::spawn(async move {
        let mut drain_rx = drain_rx;
        loop {
            if *drain_rx.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = drain_rx.changed() => {
                    if changed.is_err() || *drain_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            spawn_connection(stream, handler.clone(), profile, drain_rx.clone());
                        }
                        Err(error) => {
                            tracing::error!(error = %error, "unix socket accept failed");
                            break;
                        }
                    }
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
    _profile: ControlIpcProfile,
    _drain_rx: watch::Receiver<bool>,
) -> IpcResult<JoinHandle<()>> {
    Err(IpcError::UnsupportedTransport(IpcTransport::UnixSocket))
}

async fn start_tcp_debug_server(
    addr: String,
    handler: Arc<dyn ControlHandler>,
    profile: ControlIpcProfile,
    drain_rx: watch::Receiver<bool>,
) -> IpcResult<JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let handle = tokio::spawn(async move {
        let mut drain_rx = drain_rx;
        loop {
            if *drain_rx.borrow() {
                break;
            }
            tokio::select! {
                biased;
                changed = drain_rx.changed() => {
                    if changed.is_err() || *drain_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, peer)) => {
                            if !peer.ip().is_loopback() {
                                tracing::warn!(peer = %peer, "rejected non-loopback debug control connection");
                                continue;
                            }
                            spawn_connection(stream, handler.clone(), profile, drain_rx.clone());
                        }
                        Err(error) => {
                            tracing::error!(error = %error, "tcp debug accept failed");
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(handle)
}

pub async fn connect_transport(
    transport: IpcTransport,
    endpoint: &str,
) -> IpcResult<ControlStream> {
    match transport {
        IpcTransport::NamedPipe => connect_named_pipe(endpoint).await,
        IpcTransport::UnixSocket => connect_unix_socket(endpoint).await,
        IpcTransport::TcpDebug => {
            let stream = tokio::net::TcpStream::connect(endpoint).await?;
            Ok(ControlStream::Tcp(stream))
        }
    }
}

pub enum ControlStream {
    #[cfg(windows)]
    NamedPipe(tokio::net::windows::named_pipe::NamedPipeClient),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    Tcp(tokio::net::TcpStream),
}

impl AsyncRead for ControlStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipe(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
            Self::Tcp(stream) => std::pin::Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ControlStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipe(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
            Self::Tcp(stream) => std::pin::Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipe(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            Self::Tcp(stream) => std::pin::Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipe(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            Self::Tcp(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
        }
    }
}

#[cfg(windows)]
async fn connect_named_pipe(name: &str) -> IpcResult<ControlStream> {
    use tokio::net::windows::named_pipe::ClientOptions;
    let client = ClientOptions::new().open(named_pipe_path(name))?;
    Ok(ControlStream::NamedPipe(client))
}

#[cfg(not(windows))]
async fn connect_named_pipe(_name: &str) -> IpcResult<ControlStream> {
    Err(IpcError::UnsupportedTransport(IpcTransport::NamedPipe))
}

#[cfg(unix)]
async fn connect_unix_socket(path: &str) -> IpcResult<ControlStream> {
    let stream = tokio::net::UnixStream::connect(path).await?;
    Ok(ControlStream::Unix(stream))
}

#[cfg(not(unix))]
async fn connect_unix_socket(_path: &str) -> IpcResult<ControlStream> {
    Err(IpcError::UnsupportedTransport(IpcTransport::UnixSocket))
}
