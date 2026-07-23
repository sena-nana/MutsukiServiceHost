//! Standalone Link control client: forwards typed control RPC to ServiceHost.

use std::time::{Duration, Instant};

use mutsuki_link_core::{ConnectContext, EndpointId};
use mutsuki_link_local::{
    AppId, SessionIdentity, connect, endpoint_id_for_app, local_address_for_app,
};
use mutsuki_service_control::{
    ControlError, ControlFuture, ControlHandler, ControlRequest, ControlResponse,
};

use crate::protocol::{LinkControlClientFrame, LinkControlServerFrame, SERVICE_LINK_APP_ID};
use crate::transport::{recv_json, send_json};

pub const STANDALONE_LINK_CONNECT_FAILED: &str = "standalone.link_connect_failed";
pub const STANDALONE_LINK_PROTOCOL_ERROR: &str = "standalone.link_protocol_error";
pub const STANDALONE_LINK_REJECTED: &str = "standalone.link_rejected";

const CLIENT_ENDPOINT_ID: EndpointId = EndpointId::from_bytes([0x02; 16]);

/// Proxies control-plane RPC to ServiceHost over MutsukiLink local transport.
#[derive(Clone, Debug, Default)]
pub struct LinkControlHandler {
    app_id: String,
}

impl LinkControlHandler {
    /// Connect to the stable ServiceHost Link app (`mutsuki.servicehost`).
    pub fn service_host() -> Self {
        Self {
            app_id: SERVICE_LINK_APP_ID.into(),
        }
    }

    /// Connect to a specific Link app id (tests / alternate hosts).
    pub fn for_app(app_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
        }
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }
}

impl ControlHandler for LinkControlHandler {
    fn handle(&self, request: ControlRequest) -> ControlFuture {
        let app_id = self.app_id.clone();
        Box::pin(async move { forward_request(&app_id, request).await })
    }
}

async fn forward_request(app_id: &str, request: ControlRequest) -> ControlResponse {
    match relay_request(app_id, request).await {
        Ok(response) => response,
        Err(error) => ControlResponse::err(ControlError::Failed(error)),
    }
}

async fn relay_request(app_id: &str, request: ControlRequest) -> Result<ControlResponse, String> {
    let link_app = AppId::new(app_id)
        .map_err(|_| format!("{STANDALONE_LINK_CONNECT_FAILED}: invalid link app id `{app_id}`"))?;
    let session = SessionIdentity::current();
    let local = local_address_for_app(&link_app, &session);
    let remote = endpoint_id_for_app(&link_app, &session);
    let budget = mutsuki_link_core::TransportBudget {
        idle_timeout: None,
        ..mutsuki_link_core::TransportBudget::default()
    };
    let context = ConnectContext {
        deadline: Some(Instant::now() + Duration::from_secs(5)),
        ..ConnectContext::default()
    };
    let mut connection = connect(&local, CLIENT_ENDPOINT_ID, remote, budget, &context)
        .await
        .map_err(|error| {
            format!(
                "{STANDALONE_LINK_CONNECT_FAILED}: could not reach ServiceHost link endpoint `{app_id}` ({error})"
            )
        })?;
    send_json(
        &mut connection,
        &LinkControlClientFrame::ControlRequest(request),
    )
    .await
    .map_err(|error| format!("{STANDALONE_LINK_PROTOCOL_ERROR}: {error}"))?;
    // Give the server accept loop a tick before tearing down on drop.
    tokio::time::sleep(Duration::from_millis(5)).await;
    match recv_json::<LinkControlServerFrame>(&mut connection)
        .await
        .map_err(|error| format!("{STANDALONE_LINK_PROTOCOL_ERROR}: {error}"))?
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

    use mutsuki_service_control::{
        ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest,
        ControlResponse, HealthReport,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::server::LinkControlServer;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn link_control_handler_reaches_server() {
        let _guard = crate::LINK_TEST_LOCK.lock().expect("link test lock");
        let dir = tempdir().unwrap();
        let _server =
            LinkControlServer::start(dir.path(), "client-smoke", Arc::new(HealthHandler)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = LinkControlHandler::service_host();
        let response = client
            .handle(ControlRequest {
                token: "local-dev".into(),
                method: ControlMethod::HealthCheck,
                params: serde_json::Value::Null,
            })
            .await;
        assert!(response.ok, "{response:?}");
        assert_eq!(response.result.unwrap()["service"], "ok");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn link_control_handler_fails_loud_when_absent() {
        let _guard = crate::LINK_TEST_LOCK.lock().expect("link test lock");
        let client = LinkControlHandler::for_app("mutsuki.nolink.test");
        let response = client
            .handle(ControlRequest {
                token: "local-dev".into(),
                method: ControlMethod::HealthCheck,
                params: serde_json::Value::Null,
            })
            .await;
        assert!(!response.ok);
        let message = response.error.unwrap().message;
        assert!(
            message.contains(STANDALONE_LINK_CONNECT_FAILED),
            "{message}"
        );
    }
}
