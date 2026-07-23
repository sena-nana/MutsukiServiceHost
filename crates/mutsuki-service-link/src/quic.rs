//! Authenticated QUIC control bridge (caller-supplied TLS identity).
//!
//! Protocol frames stay identical to the local bridge; only the Link transport
//! changes. Production callers must inject Quinn TLS configs — this module never
//! embeds a default trust policy.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mutsuki_link_core::{ConnectContext, Connection, EndpointId, TransportBudget};
use mutsuki_link_quic::{QuicConnector, QuicListener, QuicOptions};
use mutsuki_service_control::{
    ControlError, ControlFuture, ControlHandler, ControlRequest, ControlResponse,
};
use quinn::{ClientConfig, ServerConfig};
use tokio::task::JoinHandle;

use crate::client::{
    STANDALONE_LINK_CONNECT_FAILED, STANDALONE_LINK_PROTOCOL_ERROR, STANDALONE_LINK_REJECTED,
};
use crate::protocol::{LinkControlClientFrame, LinkControlServerFrame};
use crate::transport::{recv_json, send_json};

pub const STANDALONE_LINK_QUIC_UNAVAILABLE: &str = "standalone.link_quic_unavailable";

const SERVER_ENDPOINT_ID: EndpointId = EndpointId::from_bytes([0x51; 16]);
const CLIENT_ENDPOINT_ID: EndpointId = EndpointId::from_bytes([0x52; 16]);

/// QUIC control server bound with an explicit TLS server identity.
pub struct QuicLinkControlServer {
    accept_task: JoinHandle<()>,
    local_addr: SocketAddr,
}

impl QuicLinkControlServer {
    pub fn start(
        bind: SocketAddr,
        server_config: ServerConfig,
        handler: Arc<dyn ControlHandler>,
    ) -> Result<Self, String> {
        let options = QuicOptions {
            budget: TransportBudget {
                idle_timeout: None,
                ..TransportBudget::default()
            },
            enable_datagrams: false,
            ..QuicOptions::default()
        };
        let listener = QuicListener::bind(bind, SERVER_ENDPOINT_ID, server_config, options)
            .map_err(|error| format!("failed to bind QUIC link control endpoint: {error}"))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| format!("failed to resolve QUIC local addr: {error}"))?;
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept(CLIENT_ENDPOINT_ID).await {
                    Ok(mut connection) => {
                        let handler = handler.clone();
                        tokio::spawn(async move {
                            serve_connection(&handler, &mut connection).await;
                            // Allow the peer to drain the response before teardown.
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            let _ = connection.close_write();
                        });
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        });
        Ok(Self {
            accept_task,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for QuicLinkControlServer {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Proxies control-plane RPC to a remote/loopback ServiceHost over QUIC.
#[derive(Clone)]
pub struct QuicLinkControlHandler {
    addr: SocketAddr,
    server_name: String,
    client_config: Arc<ClientConfig>,
}

impl QuicLinkControlHandler {
    pub fn new(
        addr: SocketAddr,
        server_name: impl Into<String>,
        client_config: ClientConfig,
    ) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
            client_config: Arc::new(client_config),
        }
    }
}

impl ControlHandler for QuicLinkControlHandler {
    fn handle(&self, request: ControlRequest) -> ControlFuture {
        let addr = self.addr;
        let server_name = self.server_name.clone();
        let client_config = self.client_config.clone();
        Box::pin(async move {
            match relay_quic(addr, &server_name, (*client_config).clone(), request).await {
                Ok(response) => response,
                Err(error) => ControlResponse::err(ControlError::Failed(error)),
            }
        })
    }
}

async fn serve_connection(
    handler: &Arc<dyn ControlHandler>,
    connection: &mut mutsuki_link_quic::QuicConnection,
) {
    let frame = match recv_json::<LinkControlClientFrame>(connection).await {
        Ok(frame) => frame,
        Err(message) => {
            let _ = send_json(
                connection,
                &LinkControlServerFrame::Rejected {
                    code: crate::protocol::LinkControlRejectCode::InvalidRequest,
                    message,
                },
            )
            .await;
            return;
        }
    };
    let LinkControlClientFrame::ControlRequest(request) = frame;
    let response = handler.handle(request).await;
    let _ = send_json(
        connection,
        &LinkControlServerFrame::ControlResponse(response),
    )
    .await;
}

async fn relay_quic(
    addr: SocketAddr,
    server_name: &str,
    client_config: ClientConfig,
    request: ControlRequest,
) -> Result<ControlResponse, String> {
    let options = QuicOptions {
        budget: TransportBudget {
            idle_timeout: None,
            ..TransportBudget::default()
        },
        enable_datagrams: false,
        ..QuicOptions::default()
    };
    let connector = QuicConnector::new("127.0.0.1:0".parse().unwrap(), client_config, options)
        .map_err(|error| {
            format!("{STANDALONE_LINK_QUIC_UNAVAILABLE}: failed to create QUIC connector ({error})")
        })?;
    let context = ConnectContext {
        deadline: Some(Instant::now() + Duration::from_secs(5)),
        ..ConnectContext::default()
    };
    let mut connection = connector
        .connect(
            addr,
            server_name,
            CLIENT_ENDPOINT_ID,
            SERVER_ENDPOINT_ID,
            &context,
        )
        .await
        .map_err(|error| {
            format!(
                "{STANDALONE_LINK_CONNECT_FAILED}: could not reach QUIC link endpoint `{addr}` ({error})"
            )
        })?;
    send_json(
        &mut connection,
        &LinkControlClientFrame::ControlRequest(request),
    )
    .await
    .map_err(|error| format!("{STANDALONE_LINK_PROTOCOL_ERROR}: send: {error}"))?;
    match recv_json::<LinkControlServerFrame>(&mut connection)
        .await
        .map_err(|error| format!("{STANDALONE_LINK_PROTOCOL_ERROR}: recv: {error}"))?
    {
        LinkControlServerFrame::ControlResponse(response) => Ok(response),
        LinkControlServerFrame::Rejected { code, message } => {
            Err(format!("{STANDALONE_LINK_REJECTED}: {code:?}: {message}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mutsuki_link_quic::{QuicConnector, QuicListener, QuicOptions};
    use mutsuki_service_control::{
        ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest,
        ControlResponse, HealthReport,
    };
    use rustls::RootCertStore;

    use super::*;
    use crate::protocol::LinkControlClientFrame;
    use crate::transport::{recv_json, send_json};

    struct HealthHandler;

    impl ControlHandler for HealthHandler {
        fn handle(&self, request: ControlRequest) -> ControlFuture {
            Box::pin(async move {
                if request.token != "local-dev" {
                    return ControlResponse::err(ControlError::Unauthorized);
                }
                match request.method {
                    ControlMethod::HealthCheck => ControlResponse::ok(HealthReport {
                        service: "ok".into(),
                        core: "ok".into(),
                        plugins: "ok".into(),
                        runners: "ok".into(),
                        event_sources: "ok".into(),
                        event_source_details: Vec::new(),
                        recent_errors: Vec::new(),
                        components: Default::default(),
                    }),
                    other => ControlResponse::err(ControlError::Unsupported(format!("{other:?}"))),
                }
            })
        }
    }

    fn crypto_configs() -> (ServerConfig, ClientConfig) {
        let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = generated.cert.der().clone();
        let private_key =
            rustls::pki_types::PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der());
        let server =
            ServerConfig::with_single_cert(vec![certificate.clone()], private_key.into()).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(certificate).unwrap();
        let client = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        (server, client)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_link_control_roundtrip() {
        let _guard = match crate::LINK_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let (server_config, client_config) = crypto_configs();
        let options = QuicOptions {
            budget: TransportBudget {
                idle_timeout: None,
                ..TransportBudget::default()
            },
            enable_datagrams: false,
            ..QuicOptions::default()
        };
        let listener = QuicListener::bind(
            "127.0.0.1:0".parse().unwrap(),
            SERVER_ENDPOINT_ID,
            server_config,
            options,
        )
        .unwrap();
        let addr = listener.local_addr().unwrap();
        let connector =
            QuicConnector::new("127.0.0.1:0".parse().unwrap(), client_config, options).unwrap();
        let context = ConnectContext::default();
        let (server_conn, client_conn) = tokio::join!(
            listener.accept(CLIENT_ENDPOINT_ID),
            connector.connect(
                addr,
                "localhost",
                CLIENT_ENDPOINT_ID,
                SERVER_ENDPOINT_ID,
                &context,
            )
        );
        let mut server_conn = server_conn.unwrap();
        let mut client_conn = client_conn.unwrap();

        let request = ControlRequest {
            token: "local-dev".into(),
            method: ControlMethod::HealthCheck,
            params: serde_json::Value::Null,
        };
        send_json(
            &mut client_conn,
            &LinkControlClientFrame::ControlRequest(request),
        )
        .await
        .expect("client send");
        let frame = recv_json::<LinkControlClientFrame>(&mut server_conn)
            .await
            .expect("server recv");
        let LinkControlClientFrame::ControlRequest(received) = frame;
        let response = HealthHandler.handle(received).await;
        send_json(
            &mut server_conn,
            &LinkControlServerFrame::ControlResponse(response),
        )
        .await
        .expect("server send");
        let frame = recv_json::<LinkControlServerFrame>(&mut client_conn)
            .await
            .expect("client recv");
        let LinkControlServerFrame::ControlResponse(response) = frame else {
            panic!("expected control response");
        };
        assert!(response.ok, "{response:?}");
        assert_eq!(response.result.unwrap()["service"], "ok");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_link_control_server_and_handler() {
        let _guard = match crate::LINK_TEST_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let (server_config, client_config) = crypto_configs();
        let server = QuicLinkControlServer::start(
            "127.0.0.1:0".parse().unwrap(),
            server_config,
            Arc::new(HealthHandler),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let client = QuicLinkControlHandler::new(server.local_addr(), "localhost", client_config);
        let response = client
            .handle(ControlRequest {
                token: "local-dev".into(),
                method: ControlMethod::HealthCheck,
                params: serde_json::Value::Null,
            })
            .await;
        assert!(response.ok, "{response:?}");
    }
}
