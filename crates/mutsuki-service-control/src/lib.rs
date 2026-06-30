use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type ControlFuture = Pin<Box<dyn Future<Output = ControlResponse> + Send>>;

pub trait ControlHandler: Send + Sync + 'static {
    fn handle(&self, request: ControlRequest) -> ControlFuture;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlRequest {
    pub token: String,
    pub method: ControlMethod,
    #[serde(default)]
    pub params: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlMethod {
    ServiceStatus,
    ServiceShutdown,
    CoreStatus,
    PluginList,
    PluginReload,
    RunnerList,
    RunnerRestart,
    RunnerStop,
    TaskList,
    TaskCancel,
    HealthCheck,
    LogTail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ControlErrorBody>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("unauthorized control request")]
    Unauthorized,
    #[error("unsupported control method: {0}")]
    Unsupported(&'static str),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("operation failed: {0}")]
    Failed(String),
}

impl ControlResponse {
    pub fn ok<T: Serialize>(result: T) -> Self {
        match serde_json::to_value(result) {
            Ok(value) => Self {
                ok: true,
                result: Some(value),
                error: None,
            },
            Err(error) => Self::err(ControlError::Failed(error.to_string())),
        }
    }

    pub fn empty_ok() -> Self {
        Self {
            ok: true,
            result: Some(Value::Null),
            error: None,
        }
    }

    pub fn err(error: ControlError) -> Self {
        let (code, message) = match error {
            ControlError::Unauthorized => ("unauthorized".into(), error.to_string()),
            ControlError::Unsupported(method) => (
                "unsupported".into(),
                format!("{method} is not supported by the current runtime API"),
            ),
            ControlError::BadRequest(message) => ("bad_request".into(), message),
            ControlError::Failed(message) => ("failed".into(), message),
        };
        Self {
            ok: false,
            result: None,
            error: Some(ControlErrorBody { code, message }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub instance_id: String,
    pub profile: String,
    pub uptime_ms: u128,
    pub ipc_endpoint: String,
    pub core_running: bool,
    pub plugin_count: usize,
    pub runner_count: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoreStatus {
    pub running: bool,
    pub profile_id: Option<String>,
    pub registry_generation: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginStatus {
    pub plugin_id: String,
    pub version: String,
    pub api_version: String,
    pub deployment: String,
    pub enabled: bool,
    pub runner_link: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerStatus {
    pub runner_id: String,
    pub plugin_id: String,
    pub state: String,
    pub pid: Option<u32>,
    pub restarts: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthReport {
    pub service: String,
    pub core: String,
    pub plugins: String,
    pub runners: String,
    pub recent_errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdParam {
    pub id: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LogTailParams {
    pub lines: Option<usize>,
    #[serde(default)]
    pub filters: BTreeMap<String, String>,
}
