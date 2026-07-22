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

#[repr(u16)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ControlMethod {
    ServiceStatus = 0x0001,
    ServiceShutdown = 0x0002,
    CoreStatus = 0x0003,
    PluginList = 0x0004,
    PluginReload = 0x0005,
    PluginDeploymentSet = 0x0006,
    PluginDeploymentClear = 0x0007,
    RunnerList = 0x0008,
    RunnerRestart = 0x0009,
    RunnerStop = 0x000A,
    EventSourceList = 0x000B,
    EventSourceRestart = 0x000C,
    CoreBeginDrain = 0x000D,
    TaskSubmitBatch = 0x000E,
    TaskList = 0x000F,
    TaskCancel = 0x0010,
    TaskOutcome = 0x0011,
    TaskEventsAfter = 0x0012,
    HealthCheck = 0x0013,
    LogTail = 0x0014,
    TaskOutcomesBatch = 0x0015,
    TaskWait = 0x0016,
}

impl ControlMethod {
    pub const fn opcode(self) -> u16 {
        self as u16
    }

    pub fn from_opcode(opcode: u16) -> Option<Self> {
        Some(match opcode {
            0x0001 => Self::ServiceStatus,
            0x0002 => Self::ServiceShutdown,
            0x0003 => Self::CoreStatus,
            0x0004 => Self::PluginList,
            0x0005 => Self::PluginReload,
            0x0006 => Self::PluginDeploymentSet,
            0x0007 => Self::PluginDeploymentClear,
            0x0008 => Self::RunnerList,
            0x0009 => Self::RunnerRestart,
            0x000A => Self::RunnerStop,
            0x000B => Self::EventSourceList,
            0x000C => Self::EventSourceRestart,
            0x000D => Self::CoreBeginDrain,
            0x000E => Self::TaskSubmitBatch,
            0x000F => Self::TaskList,
            0x0010 => Self::TaskCancel,
            0x0011 => Self::TaskOutcome,
            0x0012 => Self::TaskEventsAfter,
            0x0013 => Self::HealthCheck,
            0x0014 => Self::LogTail,
            0x0015 => Self::TaskOutcomesBatch,
            0x0016 => Self::TaskWait,
            _ => return None,
        })
    }

    /// Mutating ops stay ordered on a connection under multiplex.
    pub const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::ServiceShutdown
                | Self::PluginReload
                | Self::PluginDeploymentSet
                | Self::PluginDeploymentClear
                | Self::RunnerRestart
                | Self::RunnerStop
                | Self::EventSourceRestart
                | Self::CoreBeginDrain
                | Self::TaskSubmitBatch
                | Self::TaskCancel
        )
    }
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
    pub output: Option<Value>,
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
pub struct TaskOutcomesBatchParam {
    pub ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskOutcomesBatchResponse {
    pub outcomes: Vec<TaskOutcomeView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskWaitParam {
    pub ids: Vec<String>,
    #[serde(default = "default_task_wait_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_task_wait_timeout_ms() -> u64 {
    5_000
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskWaitResponse {
    pub outcomes: Vec<TaskOutcomeView>,
    pub timed_out: bool,
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
    pub earliest_available_sequence: Option<u64>,
    pub latest_sequence: u64,
    pub lost: u64,
    pub dropped: u64,
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
