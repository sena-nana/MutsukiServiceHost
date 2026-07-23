use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use mutsuki_link_core::{Connection, EndpointId, TransportBudget};
use mutsuki_link_local::{
    AppId, EndpointLease, LocalConnection, LocalListener, SessionIdentity, endpoint_id_for_app,
    local_address_for_app, reclaim_stale_lease,
};
use mutsuki_service_control::ControlHandler;
use tokio::task::JoinHandle;

use crate::protocol::{
    LinkControlClientFrame, LinkControlRejectCode, LinkControlServerFrame, SERVICE_LINK_APP_ID,
};
use crate::transport::{recv_json, send_json};

#[derive(Debug, thiserror::Error)]
pub enum LinkControlServerError {
    #[error("invalid service link app id")]
    InvalidAppId,
    #[error("failed to bind local link control endpoint: {0}")]
    BindFailed(String),
    #[error("failed to create endpoint lease: {0}")]
    LeaseFailed(String),
}

pub struct LinkControlServer {
    accept_task: JoinHandle<()>,
    lease: Option<EndpointLease>,
}

impl LinkControlServer {
    pub fn start(
        lease_dir: impl AsRef<Path>,
        instance_id: impl Into<String>,
        handler: Arc<dyn ControlHandler>,
    ) -> Result<Self, LinkControlServerError> {
        let lease_dir = lease_dir.as_ref();
        let app =
            AppId::new(SERVICE_LINK_APP_ID).map_err(|_| LinkControlServerError::InvalidAppId)?;
        let _ = reclaim_stale_lease(lease_dir, &app, Duration::from_secs(0));
        let lease = EndpointLease::create(lease_dir, &app, instance_id)
            .map_err(|error| LinkControlServerError::LeaseFailed(error.to_string()))?;
        let session = SessionIdentity::current();
        let address = local_address_for_app(&app, &session);
        let endpoint_id = endpoint_id_for_app(&app, &session);
        let budget = TransportBudget {
            idle_timeout: None,
            ..TransportBudget::default()
        };
        let listener = LocalListener::bind(&address, endpoint_id, budget)
            .map_err(|error| LinkControlServerError::BindFailed(error.to_string()))?;
        let handler = handler.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok(mut connection) = listener.accept(EndpointId::from_bytes([0x01; 16])).await
                else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                serve_connection(&handler, &mut connection).await;
                let _ = connection.close_write();
            }
        });
        Ok(Self {
            accept_task,
            lease: Some(lease),
        })
    }
}

impl Drop for LinkControlServer {
    fn drop(&mut self) {
        self.accept_task.abort();
        if let Some(lease) = self.lease.take() {
            let _ = lease.clear();
        }
    }
}

async fn serve_connection(handler: &Arc<dyn ControlHandler>, connection: &mut LocalConnection) {
    let frame = match recv_json::<LinkControlClientFrame>(connection).await {
        Ok(frame) => frame,
        Err(message) => {
            let _ = send_json(
                connection,
                &LinkControlServerFrame::Rejected {
                    code: LinkControlRejectCode::InvalidRequest,
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
    tokio::time::sleep(Duration::from_millis(1)).await;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mutsuki_link_core::{ConnectContext, EndpointId, TransportBudget};
    use mutsuki_link_local::{
        AppId, SessionIdentity, connect, endpoint_id_for_app, local_address_for_app,
    };
    use mutsuki_service_control::{
        ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest,
        ControlResponse, HealthReport,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::protocol::LinkControlClientFrame;

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
    async fn link_control_round_trip_and_auth() {
        let _guard = match crate::LINK_TEST_LOCK.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let dir = tempdir().unwrap();
        let _server =
            LinkControlServer::start(dir.path(), "test-instance", Arc::new(HealthHandler)).unwrap();
        let app = AppId::new(SERVICE_LINK_APP_ID).unwrap();
        let session = SessionIdentity::current();
        let address = local_address_for_app(&app, &session);
        let remote = endpoint_id_for_app(&app, &session);
        let budget = TransportBudget {
            idle_timeout: None,
            ..TransportBudget::default()
        };

        let mut connection = connect(
            &address,
            EndpointId::from_bytes([0x01; 16]),
            remote,
            budget,
            &ConnectContext::default(),
        )
        .await
        .unwrap();
        send_json(
            &mut connection,
            &LinkControlClientFrame::ControlRequest(ControlRequest {
                token: "local-dev".into(),
                method: ControlMethod::HealthCheck,
                params: serde_json::Value::Null,
            }),
        )
        .await
        .unwrap();
        let frame = recv_json::<LinkControlServerFrame>(&mut connection)
            .await
            .unwrap();
        let LinkControlServerFrame::ControlResponse(response) = frame else {
            panic!("unexpected frame");
        };
        assert!(response.ok);
        assert_eq!(response.result.unwrap()["service"], "ok");

        let mut bad_connection = connect(
            &address,
            EndpointId::from_bytes([0x03; 16]),
            remote,
            budget,
            &ConnectContext::default(),
        )
        .await
        .unwrap();
        send_json(
            &mut bad_connection,
            &LinkControlClientFrame::ControlRequest(ControlRequest {
                token: "wrong".into(),
                method: ControlMethod::HealthCheck,
                params: serde_json::Value::Null,
            }),
        )
        .await
        .unwrap();
        let frame = recv_json::<LinkControlServerFrame>(&mut bad_connection)
            .await
            .unwrap();
        let LinkControlServerFrame::ControlResponse(response) = frame else {
            panic!("unexpected frame");
        };
        assert!(!response.ok);
        assert_eq!(response.error.unwrap().code, "unauthorized");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn link_control_from_separate_runtime_thread() {
        let _guard = match crate::LINK_TEST_LOCK.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let dir = tempdir().unwrap();
        let _server =
            LinkControlServer::start(dir.path(), "test-instance", Arc::new(HealthHandler)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let app = AppId::new(SERVICE_LINK_APP_ID).unwrap();
        let session = SessionIdentity::current();
        let address = local_address_for_app(&app, &session);
        let remote = endpoint_id_for_app(&app, &session);
        let budget = TransportBudget {
            idle_timeout: None,
            ..TransportBudget::default()
        };
        let frame = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut connection = connect(
                        &address,
                        EndpointId::from_bytes([0x04; 16]),
                        remote,
                        budget,
                        &ConnectContext::default(),
                    )
                    .await
                    .unwrap();
                    send_json(
                        &mut connection,
                        &LinkControlClientFrame::ControlRequest(ControlRequest {
                            token: "local-dev".into(),
                            method: ControlMethod::HealthCheck,
                            params: serde_json::Value::Null,
                        }),
                    )
                    .await
                    .unwrap();
                    recv_json::<LinkControlServerFrame>(&mut connection)
                        .await
                        .unwrap()
                })
        })
        .join()
        .unwrap();
        let LinkControlServerFrame::ControlResponse(response) = frame else {
            panic!("unexpected frame");
        };
        assert!(response.ok);
    }
}
