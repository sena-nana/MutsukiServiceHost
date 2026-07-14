use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use mutsuki_runtime_contracts::{RuntimeEvent, TaskBatch, TaskHandle};

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
    PluginDeploymentSet,
    PluginDeploymentClear,
    RunnerList,
    RunnerRestart,
    RunnerStop,
    EventSourceList,
    EventSourceRestart,
    CoreBeginDrain,
    TaskSubmitBatch,
    TaskList,
    TaskCancel,
    TaskOutcome,
    TaskEventsAfter,
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
    Unsupported(String),
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
pub struct PluginListResponse {
    pub plugins: Vec<PluginStatus>,
    pub diagnostics: Vec<PluginInventoryDiagnostic>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginStatus {
    pub plugin_id: String,
    pub configured: bool,
    pub active_deployment: Option<String>,
    pub preferred_deployment: Option<String>,
    pub candidates: Vec<PluginCandidateStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginCandidateStatus {
    pub deployment: String,
    pub version: String,
    pub api_version: String,
    pub sha256: String,
    pub path: String,
    pub available: bool,
    pub runner_link: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginInventoryDiagnostic {
    pub manifest_path: String,
    pub plugin_id: Option<String>,
    pub deployment: Option<String>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginDeploymentParam {
    pub plugin_id: String,
    pub deployment: mutsuki_runtime_contracts::PluginDeploymentKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginDeploymentClearParam {
    pub plugin_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginReloadResponse {
    pub previous_generation: u64,
    pub registry_generation: u64,
    pub plugin_count: usize,
    pub changes: Vec<PluginReloadChange>,
    pub runner_errors: Vec<String>,
    /// Event sources are product-scoped and remain running across plugin generation reloads.
    pub event_sources: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginReloadChange {
    pub surface_id: String,
    pub compatibility: String,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventSourceStatus {
    pub source_id: String,
    pub plugin_id: String,
    pub instance_id: String,
    pub state: String,
    pub health: String,
    pub last_error: Option<String>,
    pub reconnects: u32,
    pub last_event_unix_ms: Option<u128>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskFailureSummary {
    pub code: String,
    pub source: String,
    pub route: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskOutcomeView {
    pub task_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evidence: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub protocol_id: String,
    pub status: String,
    pub priority: i64,
    pub ready_at_step: Option<u64>,
    pub created_sequence: u64,
    pub registry_generation: u64,
    pub target_binding_id: Option<String>,
    pub runner_hint: Option<String>,
    pub claimed_by: Option<String>,
    pub owner_runner: Option<String>,
    pub lease_id: Option<String>,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub input_refs: Vec<String>,
    pub output_ref: Option<String>,
    pub continuation_ref: Option<String>,
    pub required_surfaces: Vec<String>,
    pub failure: Option<TaskFailureSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthReport {
    pub service: String,
    pub core: String,
    pub plugins: String,
    pub runners: String,
    pub event_sources: String,
    pub event_source_details: Vec<EventSourceStatus>,
    pub recent_errors: Vec<String>,
    #[serde(default)]
    pub components: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdParam {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskSubmitBatchParam {
    pub batch: TaskBatch,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskSubmitBatchResponse {
    pub handles: Vec<TaskHandle>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskEventsAfterParam {
    pub sequence: u64,
    pub limit: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskEventPage {
    pub next_sequence: u64,
    pub has_more: bool,
    pub events: Vec<RuntimeEvent>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreDrainResponse {
    pub state: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LogTailParams {
    pub cursor: Option<u64>,
    pub lines: Option<usize>,
    #[serde(default)]
    pub filters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogTailEntry {
    pub offset: u64,
    pub line: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogTailResponse {
    pub cursor: u64,
    pub entries: Vec<LogTailEntry>,
}
