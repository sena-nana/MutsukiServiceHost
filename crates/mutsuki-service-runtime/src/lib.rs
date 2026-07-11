use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
#[cfg(test)]
use std::io::Write;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mutsuki_runtime_contracts::{
    CancelPolicy, PluginDeploymentKind, RunnerDescriptor, RuntimeProfile, RuntimeProfileMode,
    SurfaceCompatibility, TaskHandle, TaskOutcome, TaskStatus,
};
use mutsuki_runtime_core::{Runner, RuntimeFailure, RuntimeResult};
use mutsuki_runtime_host::{
    HostRuntime, HostRuntimeCommand, HostRuntimeConfig, HostRuntimeReply, HostTaskSnapshot,
    JsonlRunner, RuntimeBootstrapper,
};
use mutsuki_service_config::{ServiceConfig, filtered_environment};
use mutsuki_service_control::{
    ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest, ControlResponse,
    CoreStatus, HealthReport, IdParam, LogTailEntry, LogTailParams, LogTailResponse,
    PluginCallParams, PluginReloadChange, PluginReloadResponse, PluginStatus,
    RunnerStatus as ControlRunnerStatus, ServiceStatus,
    TaskFailureSummary as ControlTaskFailureSummary, TaskOutcomeView,
    TaskSnapshot as ControlTaskSnapshot,
};
use mutsuki_service_ipc::IpcServer;
use mutsuki_service_plugin_loader::{
    BuiltinRegistry, ExternalRuntimeSpec, HostPluginCallError, PluginCatalog, PluginLoaderError,
    PluginRecord,
};
use mutsuki_service_runner_supervisor::{
    ManagedRunnerSpec, RunnerProcessState, RunnerSnapshot, RunnerSupervisor,
};
use serde_json::Value;
use tokio::sync::{oneshot, watch};

mod event_source;

use event_source::EventSourceSupervisor;
pub use event_source::{
    HostEventSource, HostEventSourceConfig, HostEventSourceContext, HostEventSourceDescriptor,
    HostEventSourceError, HostEventSourceFuture, HostEventSourceHealth, HostEventSourceLogger,
    HostShutdownToken,
};

type NativeRunnerFactory = Arc<dyn Fn() -> Box<dyn Runner> + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum ServiceRuntimeError {
    #[error(transparent)]
    Plugin(#[from] PluginLoaderError),
    #[error(transparent)]
    Core(#[from] RuntimeFailure),
    #[error(transparent)]
    Ipc(#[from] mutsuki_service_ipc::IpcError),
    #[error("external runner link {link} for plugin {plugin_id} is not supported")]
    UnsupportedRunnerLink { plugin_id: String, link: String },
    #[error("external runner {runner_id} failed to start: {source}")]
    ExternalRunnerSpawn {
        runner_id: String,
        source: std::io::Error,
    },
    #[error("service runtime already started")]
    AlreadyStarted,
    #[error("event source registration failed: {0}")]
    EventSource(String),
}

pub type ServiceRuntimeResult<T> = Result<T, ServiceRuntimeError>;

pub struct ServiceRuntime {
    inner: Arc<ServiceRuntimeInner>,
    shutdown_rx: Option<oneshot::Receiver<String>>,
    ipc_server: Option<IpcServer>,
    core_pump_shutdown: watch::Sender<bool>,
    core_pump: Option<tokio::task::JoinHandle<()>>,
    _observe: mutsuki_service_observe::ObserveGuard,
}

/// Product assembly boundary. All manifests, native runners and event sources are frozen at boot.
pub struct ServiceRuntimeBuilder {
    config: ServiceConfig,
    builtin_registry: BuiltinRegistry,
    native_runner_factories: Vec<NativeRunnerFactory>,
    event_sources: Vec<Box<dyn HostEventSource>>,
}

struct ServiceRuntimeInner {
    config: ServiceConfig,
    started_at: Instant,
    catalog: Mutex<PluginCatalog>,
    host_runtime: Mutex<Option<HostRuntime>>,
    supervisor: RunnerSupervisor,
    event_sources: EventSourceSupervisor,
    builtin_registry: BuiltinRegistry,
    native_runner_factories: Vec<NativeRunnerFactory>,
    shutdown_tx: Mutex<Option<oneshot::Sender<String>>>,
}

impl ServiceRuntime {
    pub async fn start(config: ServiceConfig) -> ServiceRuntimeResult<Self> {
        ServiceRuntimeBuilder::new(config).start().await
    }
}

impl Drop for ServiceRuntime {
    fn drop(&mut self) {
        if let Some(server) = self.ipc_server.take() {
            server.abort();
        }
        let _ = self.core_pump_shutdown.send(true);
        self.inner.event_sources.abort();
        if let Some(task) = self.core_pump.take() {
            task.abort();
        }
    }
}

impl ServiceRuntimeBuilder {
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            config,
            builtin_registry: builtin_registry(),
            native_runner_factories: Vec::new(),
            event_sources: Vec::new(),
        }
    }

    /// Registers and enables a product-provided builtin manifest before the load plan is built.
    pub fn register_builtin_plugin(
        mut self,
        manifest: mutsuki_runtime_contracts::PluginManifest,
    ) -> Self {
        if !self.config.plugins.builtin.contains(&manifest.plugin_id) {
            self.config.plugins.builtin.push(manifest.plugin_id.clone());
        }
        self.builtin_registry.register_manifest(manifest);
        self
    }

    /// Registers a recreatable native runner factory for initial boot and every Core reload.
    pub fn register_builtin_runner<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> Box<dyn Runner> + Send + Sync + 'static,
    {
        self.native_runner_factories.push(Arc::new(factory));
        self
    }

    pub fn register_event_source(mut self, source: Box<dyn HostEventSource>) -> Self {
        self.event_sources.push(source);
        self
    }

    pub async fn start(self) -> ServiceRuntimeResult<ServiceRuntime> {
        let ServiceRuntimeBuilder {
            config,
            builtin_registry,
            native_runner_factories,
            event_sources,
        } = self;
        validate_event_sources(&event_sources)?;
        let observe = mutsuki_service_observe::init_observe(&config);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (core_pump_shutdown, core_pump_rx) = watch::channel(false);
        let supervisor = RunnerSupervisor::new();
        let event_source_supervisor = EventSourceSupervisor::default();
        let catalog = load_catalog(&config, &builtin_registry)?;
        let host_runtime = boot_core(&config, &catalog, &native_runner_factories)?;
        let task_submitter = host_runtime.host_context().task_submitter_ref();
        start_supervised_sidecars(&config, &catalog, &supervisor).await;

        let inner = Arc::new(ServiceRuntimeInner {
            config: config.clone(),
            started_at: Instant::now(),
            catalog: Mutex::new(catalog),
            host_runtime: Mutex::new(Some(host_runtime)),
            supervisor,
            event_sources: event_source_supervisor.clone(),
            builtin_registry,
            native_runner_factories,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        });
        let core_pump = spawn_core_pump(Arc::downgrade(&inner), core_pump_rx);
        let ipc_server = mutsuki_service_ipc::start_server(
            &inner.config,
            Arc::new(RuntimeControl {
                inner: inner.clone(),
            }),
        )
        .await?;
        let graceful = Duration::from_millis(config.runners.graceful_shutdown_ms);
        for source in event_sources {
            event_source_supervisor.start(source, task_submitter.clone(), &config, graceful);
        }
        Ok(ServiceRuntime {
            inner,
            shutdown_rx: Some(shutdown_rx),
            ipc_server,
            core_pump_shutdown,
            core_pump: Some(core_pump),
            _observe: observe,
        })
    }
}

fn validate_event_sources(sources: &[Box<dyn HostEventSource>]) -> ServiceRuntimeResult<()> {
    let mut ids = BTreeSet::new();
    for source in sources {
        let descriptor = source.descriptor();
        if descriptor.source_id.trim().is_empty()
            || descriptor.plugin_id.trim().is_empty()
            || descriptor.instance_id.trim().is_empty()
        {
            return Err(ServiceRuntimeError::EventSource(
                "source id, plugin id and instance id must not be empty".into(),
            ));
        }
        if !ids.insert(descriptor.source_id.clone()) {
            return Err(ServiceRuntimeError::EventSource(format!(
                "duplicate event source id {}",
                descriptor.source_id
            )));
        }
    }
    Ok(())
}

impl ServiceRuntime {
    pub async fn run_foreground(self) -> ServiceRuntimeResult<()> {
        let ctrl_c = async {
            match tokio::signal::ctrl_c().await {
                Ok(()) => "ctrl-c".to_string(),
                Err(error) => {
                    tracing::warn!(error = %error, "failed to listen for ctrl-c");
                    "ctrl-c-listener-error".to_string()
                }
            }
        };
        self.run_until_shutdown_signal(ctrl_c).await
    }

    pub async fn run_until_shutdown_signal<F>(
        mut self,
        shutdown_signal: F,
    ) -> ServiceRuntimeResult<()>
    where
        F: Future<Output = String>,
    {
        let shutdown_rx = self
            .shutdown_rx
            .take()
            .ok_or(ServiceRuntimeError::AlreadyStarted)?;
        tokio::pin!(shutdown_signal);
        tokio::select! {
            reason = shutdown_rx => {
                tracing::info!(reason = ?reason, "service shutdown requested");
            }
            reason = &mut shutdown_signal => {
                tracing::info!(reason, "service shutdown signal received");
            }
        }
        self.shutdown().await;
        Ok(())
    }

    pub async fn shutdown(mut self) {
        if let Some(server) = self.ipc_server.take() {
            server.abort();
        }
        let graceful = Duration::from_millis(self.inner.config.runners.graceful_shutdown_ms);
        self.inner.event_sources.shutdown(graceful).await;
        let _ = self.core_pump_shutdown.send(true);
        if let Some(core_pump) = self.core_pump.take() {
            let _ = core_pump.await;
        }
        self.inner.supervisor.shutdown(graceful).await;
        let _ = self
            .inner
            .host_runtime
            .lock()
            .expect("host runtime mutex")
            .take();
    }
}

fn spawn_core_pump(
    inner: std::sync::Weak<ServiceRuntimeInner>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = interval.tick() => {
                    let Some(inner) = inner.upgrade() else { break; };
                    let result = inner
                        .host_runtime
                        .lock()
                        .expect("host runtime mutex")
                        .as_ref()
                        .map(|runtime| runtime.dispatch(HostRuntimeCommand::TickOnce));
                    if let Some(Err(error)) = result {
                        tracing::error!(error = %error, "core service tick failed");
                    }
                }
            }
        }
    })
}

struct RuntimeControl {
    inner: Arc<ServiceRuntimeInner>,
}

impl ControlHandler for RuntimeControl {
    fn handle(&self, request: ControlRequest) -> ControlFuture {
        let inner = self.inner.clone();
        Box::pin(async move { inner.handle_request(request).await })
    }
}

impl ServiceRuntimeInner {
    async fn handle_request(&self, request: ControlRequest) -> ControlResponse {
        if request.token != self.config.control_token() {
            return ControlResponse::err(ControlError::Unauthorized);
        }
        match request.method {
            ControlMethod::ServiceStatus => self.service_status().await,
            ControlMethod::ServiceShutdown => self.service_shutdown(),
            ControlMethod::CoreStatus => self.core_status(),
            ControlMethod::PluginList => self.plugin_list(),
            ControlMethod::PluginReload => self.plugin_reload().await,
            ControlMethod::PluginCall => self.plugin_call(request.params),
            ControlMethod::RunnerList => self.runner_list().await,
            ControlMethod::RunnerRestart => self.runner_restart(request.params).await,
            ControlMethod::RunnerStop => self.runner_stop(request.params).await,
            ControlMethod::EventSourceList => self.event_source_list(),
            ControlMethod::EventSourceRestart => self.event_source_restart(request.params).await,
            ControlMethod::TaskList => self.task_list(),
            ControlMethod::TaskCancel => self.task_cancel(request.params),
            ControlMethod::TaskOutcome => self.task_outcome(request.params),
            ControlMethod::HealthCheck => self.health_check().await,
            ControlMethod::LogTail => self.log_tail(request.params),
        }
    }

    async fn service_status(&self) -> ControlResponse {
        let runners = self.supervisor.list().await;
        let core_running = self
            .host_runtime
            .lock()
            .expect("host runtime mutex")
            .is_some();
        ControlResponse::ok(ServiceStatus {
            instance_id: self.config.service.instance_id.clone(),
            profile: self.config.service.profile.clone(),
            uptime_ms: self.started_at.elapsed().as_millis(),
            ipc_endpoint: self.config.ipc_endpoint(),
            core_running,
            plugin_count: self.catalog.lock().expect("catalog mutex").records.len(),
            runner_count: runners.len(),
        })
    }

    fn service_shutdown(&self) -> ControlResponse {
        if let Some(tx) = self.shutdown_tx.lock().expect("shutdown mutex").take() {
            let _ = tx.send("control-api".into());
        }
        ControlResponse::empty_ok()
    }

    fn core_status(&self) -> ControlResponse {
        let guard = self.host_runtime.lock().expect("host runtime mutex");
        let status = guard.as_ref().map(|runtime| CoreStatus {
            running: true,
            profile_id: Some(runtime.host_context().profile_id().into()),
            registry_generation: Some(runtime.host_context().registry_generation()),
        });
        ControlResponse::ok(status.unwrap_or(CoreStatus {
            running: false,
            profile_id: None,
            registry_generation: None,
        }))
    }

    fn plugin_list(&self) -> ControlResponse {
        let catalog = self.catalog.lock().expect("catalog mutex");
        let plugins = catalog
            .records
            .iter()
            .map(|record| PluginStatus {
                plugin_id: record.manifest.plugin_id.clone(),
                version: record.manifest.version.clone(),
                api_version: record.manifest.api_version.clone(),
                deployment: format!(
                    "{:?}",
                    PluginDeploymentKind::default_for_artifact(
                        &record.manifest.artifact.artifact_type
                    )
                )
                .to_ascii_lowercase(),
                enabled: record.enabled,
                runner_link: record
                    .runtime
                    .as_ref()
                    .map(|runtime| runtime.runner_link.clone()),
            })
            .collect::<Vec<_>>();
        ControlResponse::ok(plugins)
    }

    async fn plugin_reload(&self) -> ControlResponse {
        let new_catalog = match load_catalog(&self.config, &self.builtin_registry) {
            Ok(catalog) => catalog,
            Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
        };
        let previous_generation = {
            let guard = self.host_runtime.lock().expect("host runtime mutex");
            let Some(runtime) = guard.as_ref() else {
                return ControlResponse::err(ControlError::Failed("core is not running".into()));
            };
            runtime.host_context().registry_generation()
        };
        let registry_generation = previous_generation.saturating_add(1);
        let prepared =
            match runtime_bootstrapper(&self.config, &new_catalog, &self.native_runner_factories)
                .and_then(|(bootstrapper, profile)| {
                    Ok(bootstrapper.prepare_reload(profile, registry_generation)?)
                }) {
                Ok(reload) => reload,
                Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
            };
        let drain_timeout = reload_drain_timeout(&self.config, &new_catalog);
        let plugin_count = new_catalog.records.len();
        let sidecars = sidecar_specs(&self.config, &new_catalog);
        let decision = {
            let mut guard = self.host_runtime.lock().expect("host runtime mutex");
            let Some(runtime) = guard.as_mut() else {
                return ControlResponse::err(ControlError::Failed("core is not running".into()));
            };
            match runtime.reload(prepared, drain_timeout) {
                Ok(decision) => decision,
                Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
            }
        };

        *self.catalog.lock().expect("catalog mutex") = new_catalog;
        let runner_errors = reconcile_supervised_sidecars(
            sidecars,
            &self.supervisor,
            Duration::from_millis(self.config.runners.graceful_shutdown_ms),
        )
        .await;
        ControlResponse::ok(PluginReloadResponse {
            previous_generation,
            registry_generation,
            plugin_count,
            changes: decision
                .changes
                .into_iter()
                .map(|change| PluginReloadChange {
                    surface_id: change.surface_id,
                    compatibility: surface_compatibility(change.compatibility),
                })
                .collect(),
            runner_errors,
            event_sources: "kept".into(),
        })
    }

    fn plugin_call(&self, params: Value) -> ControlResponse {
        let params = match serde_json::from_value::<PluginCallParams>(params) {
            Ok(params) => params,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        let plugin = {
            let catalog = self.catalog.lock().expect("catalog mutex");
            catalog.host_plugins.get(&params.plugin_id).cloned()
        };
        let Some(plugin) = plugin else {
            return ControlResponse::err(ControlError::Failed(format!(
                "plugin {} is not loaded or does not expose host operations",
                params.plugin_id
            )));
        };
        // HostPlugin is a control-plane facade only; Core task/resource work must use HostContext.
        match plugin.call(&params.operation, params.payload) {
            Ok(value) => ControlResponse::ok(value),
            Err(HostPluginCallError::UnsupportedOperation(operation)) => ControlResponse::err(
                ControlError::Unsupported(format!("plugin operation {operation}")),
            ),
            Err(HostPluginCallError::BadRequest(message)) => {
                ControlResponse::err(ControlError::BadRequest(message))
            }
            Err(HostPluginCallError::Failed(message)) => {
                ControlResponse::err(ControlError::Failed(message))
            }
        }
    }

    async fn runner_list(&self) -> ControlResponse {
        let snapshots = self.supervisor.list().await;
        ControlResponse::ok(to_control_runner_status(snapshots))
    }

    async fn runner_restart(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        match self.supervisor.restart(&param.id).await {
            Ok(()) => ControlResponse::empty_ok(),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    async fn runner_stop(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        match self.supervisor.stop(&param.id).await {
            Ok(()) => ControlResponse::empty_ok(),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    fn event_source_list(&self) -> ControlResponse {
        ControlResponse::ok(self.event_sources.list())
    }

    async fn event_source_restart(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        match self.event_sources.restart(&param.id).await {
            Ok(()) => ControlResponse::empty_ok(),
            Err(error) => ControlResponse::err(ControlError::Failed(error)),
        }
    }

    fn task_list(&self) -> ControlResponse {
        let mut guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_mut() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        match runtime.task_snapshots() {
            Ok(snapshots) => ControlResponse::ok(to_control_task_snapshots(snapshots)),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    fn task_cancel(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        let mut guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_mut() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        let handle = match resolve_task_handle(runtime, &param.id) {
            Ok(handle) => handle,
            Err(error) => return ControlResponse::err(error),
        };
        match runtime.dispatch(HostRuntimeCommand::CancelTask(handle)) {
            Ok(_) => ControlResponse::empty_ok(),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    fn task_outcome(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        let mut guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_mut() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        let handle = match resolve_task_handle(runtime, &param.id) {
            Ok(handle) => handle,
            Err(error) => return ControlResponse::err(error),
        };
        match runtime.dispatch(HostRuntimeCommand::TaskOutcome(handle.clone())) {
            Ok(HostRuntimeReply::TaskOutcome(outcome)) => {
                ControlResponse::ok(to_control_task_outcome(&handle.task_id, outcome))
            }
            Ok(other) => ControlResponse::err(ControlError::Failed(format!(
                "unexpected task outcome reply: {other:?}"
            ))),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    async fn health_check(&self) -> ControlResponse {
        let runners = self.supervisor.list().await;
        let event_source_details = self.event_sources.list();
        let runner_health = if runners
            .iter()
            .any(|runner| matches!(runner.state, RunnerProcessState::Failed))
        {
            "degraded"
        } else {
            "ok"
        };
        let event_source_health = if event_source_details
            .iter()
            .any(|source| source.state == "failed" || source.health == "unhealthy")
        {
            "degraded"
        } else {
            "ok"
        };
        let recent_errors = event_source_details
            .iter()
            .filter_map(|source| {
                source
                    .last_error
                    .as_ref()
                    .map(|error| format!("event_source:{}:{error}", source.source_id))
            })
            .collect();
        let report = HealthReport {
            service: "ok".into(),
            core: if self
                .host_runtime
                .lock()
                .expect("host runtime mutex")
                .is_some()
            {
                "ok".into()
            } else {
                "stopped".into()
            },
            plugins: "ok".into(),
            runners: runner_health.into(),
            event_sources: event_source_health.into(),
            event_source_details,
            recent_errors,
        };
        ControlResponse::ok(report)
    }

    fn log_tail(&self, params: Value) -> ControlResponse {
        let params = match serde_json::from_value::<LogTailParams>(params) {
            Ok(params) => params,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        match read_log_tail(
            self.config
                .service
                .log_dir
                .join(&self.config.observe.log_file),
            params,
        ) {
            Ok(response) => ControlResponse::ok(response),
            Err(error) => ControlResponse::err(error),
        }
    }
}

fn load_catalog(
    config: &ServiceConfig,
    builtin_registry: &BuiltinRegistry,
) -> ServiceRuntimeResult<PluginCatalog> {
    let builtin = builtin_registry.load_requested(&config.plugins.builtin)?;
    Ok(PluginCatalog::scan(
        &config.plugins.dynamic_dirs,
        &config.plugins.disabled_dir,
        builtin,
    )?)
}

fn builtin_registry() -> BuiltinRegistry {
    let mut registry = BuiltinRegistry::new();
    #[cfg(feature = "conversation-sim")]
    registry.register(mutsuki_service_plugin_conversation_sim::plugin());
    #[cfg(feature = "terminal-tui")]
    registry.register(mutsuki_service_plugin_terminal_tui::plugin());
    registry
}

fn read_log_tail(
    path: impl AsRef<std::path::Path>,
    params: LogTailParams,
) -> Result<LogTailResponse, ControlError> {
    if !params.filters.is_empty() {
        return Err(ControlError::BadRequest(
            "log_tail filters are not supported by this runtime".into(),
        ));
    }

    let path = path.as_ref();
    let Ok(metadata) = std::fs::metadata(path) else {
        return Ok(LogTailResponse {
            cursor: 0,
            entries: Vec::new(),
        });
    };
    let len = metadata.len();
    let start = params.cursor.filter(|cursor| *cursor <= len).unwrap_or(0);
    let file =
        std::fs::File::open(path).map_err(|error| ControlError::Failed(error.to_string()))?;
    let mut reader = BufReader::new(file);
    reader
        .seek(SeekFrom::Start(start))
        .map_err(|error| ControlError::Failed(error.to_string()))?;

    let max_lines = params.lines.unwrap_or(100);
    let mut entries = Vec::new();
    let mut cursor = start;
    loop {
        let offset = cursor;
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| ControlError::Failed(error.to_string()))?;
        if bytes == 0 {
            break;
        }
        cursor += bytes as u64;
        entries.push(LogTailEntry {
            offset,
            line: line.trim_end_matches(['\r', '\n']).to_string(),
        });
    }
    if entries.len() > max_lines {
        entries.drain(0..entries.len() - max_lines);
    }

    Ok(LogTailResponse { cursor, entries })
}

fn boot_core(
    config: &ServiceConfig,
    catalog: &PluginCatalog,
    native_runner_factories: &[NativeRunnerFactory],
) -> ServiceRuntimeResult<HostRuntime> {
    let (bootstrapper, profile) = runtime_bootstrapper(config, catalog, native_runner_factories)?;
    let host_config = HostRuntimeConfig {
        worker_threads: config.core.worker_threads,
        blocking_threads: config.core.blocking_threads,
        ..HostRuntimeConfig::default()
    };
    Ok(bootstrapper.into_host_runtime_with_config(profile, host_config)?)
}

fn runtime_bootstrapper(
    config: &ServiceConfig,
    catalog: &PluginCatalog,
    native_runner_factories: &[NativeRunnerFactory],
) -> ServiceRuntimeResult<(RuntimeBootstrapper, RuntimeProfile)> {
    let mut bootstrapper = RuntimeBootstrapper::new();
    let mut enabled_plugins = Vec::new();
    let mut deployments = BTreeMap::new();

    for record in &catalog.records {
        if !record.enabled {
            continue;
        }
        if !is_bootable_record(record) {
            continue;
        }

        let deployment =
            PluginDeploymentKind::default_for_artifact(&record.manifest.artifact.artifact_type);
        deployments.insert(record.manifest.plugin_id.clone(), deployment.clone());
        enabled_plugins.push(record.manifest.plugin_id.clone());
        bootstrapper.register_manifest(record.manifest.clone());

        if let Some(runtime) = &record.runtime {
            register_stdio_runners(config, &mut bootstrapper, record, runtime, deployment)?;
        }
    }

    for factory in native_runner_factories {
        bootstrapper.register_builtin_runner(factory());
    }

    Ok((
        bootstrapper,
        RuntimeProfile {
            profile_id: config.service.profile.clone(),
            mode: RuntimeProfileMode::ExtensibleRuntime,
            enabled_plugins,
            bindings: BTreeMap::new(),
            plugin_deployments: deployments,
            allow_dynamic_registration: false,
            allow_hot_reload: true,
        },
    ))
}

fn is_bootable_record(record: &PluginRecord) -> bool {
    record.runtime.is_none()
        || record
            .runtime
            .as_ref()
            .map(|runtime| runtime.runner_link == "jsonl-stdio")
            .unwrap_or(false)
}

fn register_stdio_runners(
    config: &ServiceConfig,
    bootstrapper: &mut RuntimeBootstrapper,
    record: &PluginRecord,
    runtime: &ExternalRuntimeSpec,
    deployment: PluginDeploymentKind,
) -> ServiceRuntimeResult<()> {
    if runtime.runner_link != "jsonl-stdio" {
        return Err(ServiceRuntimeError::UnsupportedRunnerLink {
            plugin_id: record.manifest.plugin_id.clone(),
            link: runtime.runner_link.clone(),
        });
    }
    for descriptor in &record.manifest.provides.runners {
        let runner = OwnedJsonlRunner::spawn(
            descriptor.clone(),
            runtime.clone(),
            config.runners.env_allowlist.clone(),
            config
                .service
                .home_dir
                .clone()
                .to_string_lossy()
                .into_owned(),
        )
        .map_err(|source| ServiceRuntimeError::ExternalRunnerSpawn {
            runner_id: descriptor.runner_id.clone(),
            source,
        })?;
        bootstrapper.register_external_runner(deployment.clone(), Box::new(runner));
    }
    Ok(())
}

async fn start_supervised_sidecars(
    config: &ServiceConfig,
    catalog: &PluginCatalog,
    supervisor: &RunnerSupervisor,
) {
    for spec in sidecar_specs(config, catalog) {
        if let Err(error) = supervisor.start(spec).await {
            tracing::error!(error = %error, "failed to start supervised runner");
        }
    }
}

async fn reconcile_supervised_sidecars(
    desired: Vec<ManagedRunnerSpec>,
    supervisor: &RunnerSupervisor,
    graceful: Duration,
) -> Vec<String> {
    supervisor
        .reconcile(desired, graceful)
        .await
        .into_iter()
        .map(|error| error.to_string())
        .collect()
}

fn sidecar_specs(config: &ServiceConfig, catalog: &PluginCatalog) -> Vec<ManagedRunnerSpec> {
    catalog
        .external_records()
        .filter_map(|record| {
            let runtime = record.runtime.as_ref()?;
            if runtime.runner_link == "jsonl-stdio" && !record.manifest.provides.runners.is_empty()
            {
                return None;
            }
            let runner_id = record
                .manifest
                .provides
                .runners
                .first()
                .map(|runner| runner.runner_id.clone())
                .unwrap_or_else(|| format!("sidecar:{}", record.manifest.plugin_id));
            Some(ManagedRunnerSpec {
                runner_id,
                plugin_id: record.manifest.plugin_id.clone(),
                runtime: runtime.clone(),
                env_allowlist: config.runners.env_allowlist.clone(),
                service_home: config.service.home_dir.clone(),
                session_token: config.control_token().to_string(),
            })
        })
        .collect()
}

fn reload_drain_timeout(config: &ServiceConfig, catalog: &PluginCatalog) -> Duration {
    let max_plugin_timeout = catalog
        .records
        .iter()
        .filter(|record| record.enabled)
        .map(|record| record.manifest.lifecycle.unload_timeout_ms)
        .max()
        .unwrap_or(0);
    Duration::from_millis(config.runners.graceful_shutdown_ms.max(max_plugin_timeout))
}

fn surface_compatibility(compatibility: SurfaceCompatibility) -> String {
    match compatibility {
        SurfaceCompatibility::Identical => "identical",
        SurfaceCompatibility::Additive => "additive",
        SurfaceCompatibility::Deprecated => "deprecated",
        SurfaceCompatibility::Removed => "removed",
        SurfaceCompatibility::Breaking => "breaking",
    }
    .into()
}

fn to_control_runner_status(snapshots: Vec<RunnerSnapshot>) -> Vec<ControlRunnerStatus> {
    snapshots
        .into_iter()
        .map(|snapshot| ControlRunnerStatus {
            runner_id: snapshot.runner_id,
            plugin_id: snapshot.plugin_id,
            state: match snapshot.state {
                RunnerProcessState::Running => "running".into(),
                RunnerProcessState::Exited(code) => format!("exited:{code}"),
                RunnerProcessState::Failed => "failed".into(),
                RunnerProcessState::Stopped => "stopped".into(),
            },
            pid: snapshot.pid,
            restarts: snapshot.restarts,
            last_error: snapshot.last_error,
        })
        .collect()
}

fn to_control_task_snapshots(snapshots: Vec<HostTaskSnapshot>) -> Vec<ControlTaskSnapshot> {
    snapshots
        .into_iter()
        .map(|snapshot| ControlTaskSnapshot {
            task_id: snapshot.task_id,
            protocol_id: snapshot.protocol_id,
            status: task_status_name(&snapshot.status).into(),
            priority: snapshot.priority,
            ready_at_step: snapshot.ready_at_step,
            created_sequence: snapshot.created_sequence,
            registry_generation: snapshot.registry_generation,
            target_binding_id: snapshot.target_binding_id,
            runner_hint: snapshot.runner_hint,
            claimed_by: snapshot.claimed_by,
            owner_runner: snapshot.owner_runner,
            lease_id: snapshot.lease_id,
            trace_id: snapshot.trace_id,
            correlation_id: snapshot.correlation_id,
            input_refs: snapshot.input_refs,
            output_ref: snapshot.output_ref,
            continuation_ref: snapshot.continuation_ref,
            required_surfaces: snapshot.required_surfaces,
            failure: snapshot.failure.map(|failure| ControlTaskFailureSummary {
                code: failure.code,
                source: failure.source,
                route: failure.route,
            }),
        })
        .collect()
}

fn to_control_task_outcome(task_id: &str, outcome: Option<TaskOutcome>) -> TaskOutcomeView {
    match outcome {
        None => TaskOutcomeView {
            task_id: task_id.into(),
            status: "pending".into(),
            output_ref: None,
            reason: None,
            error_code: None,
        },
        Some(TaskOutcome::Completed {
            task_id,
            output_ref,
        }) => TaskOutcomeView {
            task_id,
            status: "completed".into(),
            output_ref,
            reason: None,
            error_code: None,
        },
        Some(TaskOutcome::Failed { task_id, error }) => TaskOutcomeView {
            task_id,
            status: "failed".into(),
            output_ref: None,
            reason: Some(error.route.clone()),
            error_code: Some(error.code),
        },
        Some(TaskOutcome::Cancelled { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "cancelled".into(),
            output_ref: None,
            reason,
            error_code: None,
        },
        Some(TaskOutcome::Expired { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "expired".into(),
            output_ref: None,
            reason,
            error_code: None,
        },
        Some(TaskOutcome::DeadLetter { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "dead_letter".into(),
            output_ref: None,
            reason,
            error_code: None,
        },
    }
}

fn task_status_name(status: &TaskStatus) -> &'static str {
    match status {
        TaskStatus::Created => "created",
        TaskStatus::Ready => "ready",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Blocked => "blocked",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::Expired => "expired",
        TaskStatus::DeadLetter => "dead_letter",
    }
}

fn resolve_task_handle(
    runtime: &mut HostRuntime,
    task_id: &str,
) -> Result<TaskHandle, ControlError> {
    let snapshots = runtime
        .task_snapshots()
        .map_err(|error| ControlError::Failed(error.to_string()))?;
    let Some(snapshot) = snapshots
        .into_iter()
        .find(|snapshot| snapshot.task_id == task_id)
    else {
        return Err(ControlError::Failed(format!(
            "task {task_id} was not found"
        )));
    };
    Ok(TaskHandle {
        task_id: snapshot.task_id,
        protocol_id: snapshot.protocol_id,
        target_binding_id: snapshot.target_binding_id,
        cancel_policy: CancelPolicy::Cascade,
        trace_id: snapshot.trace_id,
        correlation_id: snapshot.correlation_id,
    })
}

/// Owns an external JSONL stdio child and delegates protocol to Core `JsonlRunner`.
struct OwnedJsonlRunner {
    child: Child,
    inner: JsonlRunner<BufReader<std::process::ChildStdout>, std::process::ChildStdin>,
}

impl OwnedJsonlRunner {
    fn spawn(
        descriptor: RunnerDescriptor,
        runtime: ExternalRuntimeSpec,
        env_allowlist: Vec<String>,
        home: String,
    ) -> std::io::Result<Self> {
        let mut extra_env = runtime.env.clone();
        extra_env.insert("MUTSUKI_HOME".into(), home);
        extra_env.insert("MUTSUKI_RUNNER_ID".into(), descriptor.runner_id.clone());
        extra_env.insert("MUTSUKI_PLUGIN_ID".into(), descriptor.plugin_id.clone());
        let envs = filtered_environment(&env_allowlist, extra_env);
        let mut command = Command::new(runtime.command);
        command
            .args(runtime.args)
            .env_clear()
            .envs(envs)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = runtime.cwd {
            command.current_dir(cwd);
        }
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("runner stdout unavailable"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("runner stdin unavailable"))?;
        if let Some(stderr) = child.stderr.take() {
            let runner_id = descriptor.runner_id.clone();
            std::thread::spawn(move || drain_blocking_stderr(runner_id, stderr));
        }
        Ok(Self {
            child,
            inner: JsonlRunner::new(descriptor, BufReader::new(stdout), stdin),
        })
    }
}

impl Runner for OwnedJsonlRunner {
    fn descriptor(&self) -> &RunnerDescriptor {
        Runner::descriptor(&self.inner)
    }

    fn run_batch(
        &mut self,
        ctx: mutsuki_runtime_contracts::RunnerContext,
        batch: mutsuki_runtime_contracts::WorkBatch,
    ) -> RuntimeResult<mutsuki_runtime_contracts::CompletionBatch> {
        self.inner.run_batch(ctx, batch)
    }

    fn cancel(&mut self, invocation_id: &str) -> RuntimeResult<()> {
        self.inner.cancel(invocation_id)
    }

    fn dispose(&mut self) -> RuntimeResult<()> {
        let result = self.inner.dispose();
        let _ = self.child.kill();
        result
    }
}

fn drain_blocking_stderr(runner_id: String, stderr: std::process::ChildStderr) {
    let reader = BufReader::new(stderr);
    for line in reader.lines().map_while(Result::ok) {
        tracing::warn!(runner_id, stream = "stderr", line);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mutsuki_runtime_contracts::{
        ArtifactType, CompletionBatch, ExecutionClass, LifecyclePolicy, PermissionGrant,
        PluginArtifact, PluginManifest, PluginProvides, RunnerBatchCapability,
        RunnerControlCapability, RunnerOrderingCapability, RunnerPayloadCapability, RunnerPurity,
        RunnerResourceCapability, Task, WorkBatch,
    };
    use mutsuki_runtime_sdk::map_work_batch_entries;
    use mutsuki_service_control::{
        ConversationSendResponse, PluginCallParams, PluginReloadResponse, TaskOutcomeView,
        TaskSnapshot,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn log_tail_reads_recent_lines_and_advances_cursor() {
        let dir = tempdir().expect("temp dir");
        let log_path = dir.path().join("service.log");
        std::fs::write(&log_path, "one\ntwo\nthree\n").expect("write log");

        let response = read_log_tail(
            &log_path,
            LogTailParams {
                cursor: None,
                lines: Some(2),
                filters: Default::default(),
            },
        )
        .expect("tail succeeds");

        assert_eq!(response.entries.len(), 2);
        assert_eq!(response.entries[0].line, "two");
        assert_eq!(response.entries[1].line, "three");

        std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open log")
            .write_all(b"four\n")
            .expect("append log");
        let next = read_log_tail(
            &log_path,
            LogTailParams {
                cursor: Some(response.cursor),
                lines: Some(10),
                filters: Default::default(),
            },
        )
        .expect("incremental tail succeeds");

        assert_eq!(next.entries.len(), 1);
        assert_eq!(next.entries[0].line, "four");
        assert!(next.cursor > response.cursor);
    }

    #[test]
    fn log_tail_resets_cursor_after_truncation() {
        let dir = tempdir().expect("temp dir");
        let log_path = dir.path().join("service.log");
        std::fs::write(&log_path, "fresh\n").expect("write log");

        let response = read_log_tail(
            &log_path,
            LogTailParams {
                cursor: Some(10_000),
                lines: Some(10),
                filters: Default::default(),
            },
        )
        .expect("tail succeeds");

        assert_eq!(response.entries.len(), 1);
        assert_eq!(response.entries[0].line, "fresh");
    }

    #[test]
    fn log_tail_rejects_filters() {
        let dir = tempdir().expect("temp dir");
        let log_path = dir.path().join("service.log");
        std::fs::write(&log_path, "line\n").expect("write log");
        let mut filters = BTreeMap::new();
        filters.insert("level".into(), "info".into());

        let error = read_log_tail(
            &log_path,
            LogTailParams {
                cursor: None,
                lines: None,
                filters,
            },
        )
        .expect_err("filters rejected");

        assert!(matches!(error, ControlError::BadRequest(_)));
    }

    #[tokio::test]
    async fn plugin_call_checks_auth_and_dispatches_loaded_builtin() {
        let inner = test_runtime_inner("token");

        let unauthorized = inner
            .handle_request(ControlRequest {
                token: "wrong".into(),
                method: ControlMethod::PluginCall,
                params: Value::Null,
            })
            .await;
        assert!(!unauthorized.ok);
        assert_eq!(unauthorized.error.expect("error").code, "unauthorized");

        let success = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginCall,
                params: json!(PluginCallParams {
                    plugin_id: mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into(),
                    operation: "send".into(),
                    payload: json!({ "message": "hello" }),
                }),
            })
            .await;
        assert!(success.ok);
        let response: ConversationSendResponse =
            serde_json::from_value(success.result.expect("result")).expect("send response");
        assert_eq!(response.turns.len(), 2);
        assert_eq!(response.reply.role, "assistant");
    }

    #[tokio::test]
    async fn plugin_call_reports_unknown_plugin_and_operation() {
        let inner = test_runtime_inner("token");

        let missing = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginCall,
                params: json!(PluginCallParams {
                    plugin_id: "missing".into(),
                    operation: "send".into(),
                    payload: json!({ "message": "hello" }),
                }),
            })
            .await;
        assert!(!missing.ok);
        assert_eq!(missing.error.expect("error").code, "failed");

        let unsupported = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginCall,
                params: json!(PluginCallParams {
                    plugin_id: mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into(),
                    operation: "missing".into(),
                    payload: Value::Null,
                }),
            })
            .await;
        assert!(!unsupported.ok);
        assert_eq!(unsupported.error.expect("error").code, "unsupported");
    }

    #[tokio::test]
    async fn task_list_returns_live_runtime_snapshots() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        {
            let mut guard = inner.host_runtime.lock().expect("host runtime mutex");
            let runtime = guard.as_mut().expect("runtime started");
            let mut task = Task::new("control-task-1", "control.input", json!({ "hidden": true }));
            task.priority = 3;
            task.trace_id = Some("trace-control".into());
            task.required_surfaces = vec!["surface:control".into()];
            runtime
                .dispatch(HostRuntimeCommand::SubmitTask(Box::new(task)))
                .expect("submit task");
        }

        let response = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskList,
                params: Value::Null,
            })
            .await;

        assert!(response.ok);
        let snapshots: Vec<TaskSnapshot> =
            serde_json::from_value(response.result.expect("result")).expect("task snapshots");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].task_id, "control-task-1");
        assert_eq!(snapshots[0].protocol_id, "control.input");
        assert_eq!(snapshots[0].status, "ready");
        assert_eq!(snapshots[0].priority, 3);
        assert_eq!(snapshots[0].trace_id.as_deref(), Some("trace-control"));
        assert_eq!(
            snapshots[0].required_surfaces,
            vec!["surface:control".to_string()]
        );
        assert!(snapshots[0].lease_id.is_none());
        assert!(snapshots[0].failure.is_none());
    }

    #[tokio::test]
    async fn task_cancel_and_outcome_use_task_handle() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        {
            let mut guard = inner.host_runtime.lock().expect("host runtime mutex");
            let runtime = guard.as_mut().expect("runtime started");
            runtime
                .dispatch(HostRuntimeCommand::SubmitTask(Box::new(Task::new(
                    "cancel-task-1",
                    "control.input",
                    json!({}),
                ))))
                .expect("submit task");
        }

        let cancel = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskCancel,
                params: json!({ "id": "cancel-task-1" }),
            })
            .await;
        assert!(cancel.ok);

        let outcome = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskOutcome,
                params: json!({ "id": "cancel-task-1" }),
            })
            .await;
        assert!(outcome.ok);
        let view: TaskOutcomeView =
            serde_json::from_value(outcome.result.expect("result")).expect("outcome");
        assert_eq!(view.task_id, "cancel-task-1");
        assert_eq!(view.status, "cancelled");
    }

    #[test]
    fn service_host_uses_jsonl_run_batch_not_step() {
        use std::io::Cursor;

        use mutsuki_runtime_contracts::{
            BatchEntry, BatchPayload, CompletionBatch, DispatchLane, EntryCompletion,
            OrderingRequirement, RunnerContext, RunnerResult, TaskLease, WorkBatch,
            WorkResourcePlan,
        };

        let descriptor = RunnerDescriptor {
            runner_id: "jsonl.test".into(),
            plugin_id: "plugin.test".into(),
            plugin_generation: 1,
            accepted_protocol_ids: vec!["raw.input".into()],
            purity: mutsuki_runtime_contracts::RunnerPurity::Pure,
            execution_class: mutsuki_runtime_contracts::ExecutionClass::Cpu,
            input_schema: json!({}),
            output_schema: json!({}),
            batch: Default::default(),
            payload: Default::default(),
            resources: Default::default(),
            ordering: Default::default(),
            control: Default::default(),
            metadata: BTreeMap::new(),
            contract_surfaces: vec!["runner:jsonl.test".into()],
        };
        let mut task = Task::new("task-1", "raw.input", json!({}));
        task.lease_id = Some("lease-1".into());
        let batch = WorkBatch {
            batch_id: "batch-1".into(),
            tick_id: "tick-1".into(),
            batch_key: "jsonl.test".into(),
            entries: vec![BatchEntry {
                entry_id: "task-1".into(),
                task_id: "task-1".into(),
                trace_id: None,
                parent_id: None,
                payload_index: 0,
                resource_requirement_indices: Vec::new(),
                cancel_index: Some(0),
                deadline_tick: None,
                priority: 0,
                lane: DispatchLane::Normal,
                ordering: OrderingRequirement::None,
            }],
            payload: BatchPayload::from_tasks(&[task.clone()]),
            resource_plan: WorkResourcePlan::empty(),
            task_leases: vec![TaskLease {
                lease_id: "lease-1".into(),
                task_id: "task-1".into(),
                runner_id: "jsonl.test".into(),
                executor_id: "executor:test".into(),
                registry_generation: 1,
                acquired_at_step: 1,
                expires_at_step: None,
            }],
        };
        let completion = CompletionBatch {
            batch_id: "batch-1".into(),
            tick_id: "tick-1".into(),
            results: vec![EntryCompletion {
                entry_id: "task-1".into(),
                task_id: "task-1".into(),
                result: Some(RunnerResult::completed("task-1")),
                error: None,
            }],
            metadata: Vec::new(),
        };
        let response = format!("{}\n", json!({"id":"req-1","ok":true,"result": completion}));
        let reader = Cursor::new(response.into_bytes());
        let writer = Cursor::new(Vec::<u8>::new());
        let mut runner = JsonlRunner::new(descriptor, reader, writer);
        let result = runner
            .run_batch(
                RunnerContext::new(
                    1,
                    1,
                    "executor:test",
                    Some("lease-1".into()),
                    "invocation:test",
                ),
                batch,
            )
            .expect("run_batch");
        let (_reader, writer) = runner.into_inner();
        let request = String::from_utf8(writer.into_inner()).expect("utf8");
        assert_eq!(result.batch_id, "batch-1");
        assert!(request.contains("\"method\":\"runner.run_batch\""));
        assert!(request.contains("\"batch\":"));
        assert!(!request.contains("\"method\":\"runner.step\""));
    }

    #[tokio::test]
    async fn plugin_reload_requires_auth_and_swaps_generation() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());

        let unauthorized = inner
            .handle_request(ControlRequest {
                token: "wrong".into(),
                method: ControlMethod::PluginReload,
                params: Value::Null,
            })
            .await;
        assert!(!unauthorized.ok);
        assert_eq!(unauthorized.error.expect("error").code, "unauthorized");

        let response = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginReload,
                params: Value::Null,
            })
            .await;
        assert!(response.ok);
        let reload: PluginReloadResponse =
            serde_json::from_value(response.result.expect("result")).expect("reload response");
        assert_eq!(reload.previous_generation, 1);
        assert_eq!(reload.registry_generation, 2);
        assert_eq!(reload.plugin_count, 1);

        let status = inner.core_status();
        let status: CoreStatus =
            serde_json::from_value(status.result.expect("status")).expect("core status");
        assert_eq!(status.registry_generation, Some(2));
        let guard = inner.host_runtime.lock().expect("host runtime mutex");
        assert_eq!(
            guard
                .as_ref()
                .expect("runtime")
                .host_context()
                .registry_generation(),
            2
        );
    }

    #[tokio::test]
    async fn plugin_reload_failure_preserves_catalog_and_generation() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        std::fs::create_dir_all(dir.path().join("installed").join("bad")).expect("plugin dir");
        std::fs::write(
            dir.path().join("installed").join("bad").join("plugin.toml"),
            "not valid toml",
        )
        .expect("write invalid manifest");

        let response = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginReload,
                params: Value::Null,
            })
            .await;
        assert!(!response.ok);
        assert_eq!(response.error.expect("error").code, "failed");

        let status = inner.core_status();
        let status: CoreStatus =
            serde_json::from_value(status.result.expect("status")).expect("core status");
        assert_eq!(status.registry_generation, Some(1));
        let plugins = inner.plugin_list();
        let plugins: Vec<PluginStatus> =
            serde_json::from_value(plugins.result.expect("plugins")).expect("plugin list");
        assert_eq!(plugins.len(), 1);
        assert_eq!(
            plugins[0].plugin_id,
            mutsuki_service_plugin_conversation_sim::PLUGIN_ID
        );
    }

    #[tokio::test]
    async fn plugin_list_reflects_catalog_after_successful_reload() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        let plugin_dir = dir.path().join("installed").join("dynamic");
        std::fs::create_dir_all(&plugin_dir).expect("plugin dir");
        let plugin = mutsuki_service_plugin_loader::PluginToml {
            manifest: minimal_manifest("mutsuki.dynamic.test"),
            runtime: None,
            enabled: Some(true),
        };
        std::fs::write(
            plugin_dir.join("plugin.toml"),
            toml::to_string(&plugin).expect("manifest toml"),
        )
        .expect("write manifest");

        let response = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginReload,
                params: Value::Null,
            })
            .await;
        assert!(response.ok);
        let reload: PluginReloadResponse =
            serde_json::from_value(response.result.expect("result")).expect("reload response");
        assert_eq!(reload.plugin_count, 2);

        let plugins = inner.plugin_list();
        let plugins: Vec<PluginStatus> =
            serde_json::from_value(plugins.result.expect("plugins")).expect("plugin list");
        assert!(
            plugins
                .iter()
                .any(|plugin| plugin.plugin_id == "mutsuki.dynamic.test")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn product_builder_event_source_runs_real_three_stage_echo_chain() {
        let dir = tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("logs")).expect("logs");
        std::fs::create_dir_all(dir.path().join("run")).expect("run");
        let mut config = ServiceConfig::default();
        config.ipc.enabled = false;
        config.ipc.token = Some("test-token".into());
        config.observe.console = false;
        config.service.home_dir = dir.path().to_path_buf();
        config.service.log_dir = dir.path().join("logs");
        config.service.run_dir = dir.path().join("run");
        config.plugins.builtin.clear();
        config.plugins.dynamic_dirs.clear();
        config.plugins.disabled_dir = dir.path().join("disabled");
        config.runners.graceful_shutdown_ms = 250;

        let echo_count = Arc::new(AtomicUsize::new(0));
        let source_starts = Arc::new(AtomicUsize::new(0));
        let source_stops = Arc::new(AtomicUsize::new(0));
        let source = MockGatewaySource {
            descriptor: HostEventSourceDescriptor::new("mock-gateway", "test.gateway"),
            starts: source_starts.clone(),
            stops: source_stops.clone(),
        };
        let router_descriptor =
            chain_descriptor("test.router", "test.router.runner", "bot.gateway");
        let command_descriptor =
            chain_descriptor("test.command", "test.command.runner", "bot.command.parse");
        let echo_descriptor = chain_descriptor("test.echo", "test.echo.runner", "bot.command.echo");

        let router_factory_descriptor = router_descriptor.clone();
        let command_factory_descriptor = command_descriptor.clone();
        let echo_factory_descriptor = echo_descriptor.clone();
        let echo_factory_count = echo_count.clone();
        let runtime = ServiceRuntimeBuilder::new(config)
            .register_builtin_plugin(runner_manifest("test.router", router_descriptor))
            .register_builtin_plugin(runner_manifest("test.command", command_descriptor))
            .register_builtin_plugin(runner_manifest("test.echo", echo_descriptor))
            .register_builtin_runner(move || {
                Box::new(ChainRunner::next(
                    router_factory_descriptor.clone(),
                    "bot.command.parse",
                ))
            })
            .register_builtin_runner(move || {
                Box::new(ChainRunner::next(
                    command_factory_descriptor.clone(),
                    "bot.command.echo",
                ))
            })
            .register_builtin_runner(move || {
                Box::new(ChainRunner::terminal(
                    echo_factory_descriptor.clone(),
                    echo_factory_count.clone(),
                ))
            })
            .register_event_source(Box::new(source))
            .start()
            .await
            .expect("real service runtime starts");

        wait_for_count(&echo_count, 1).await;
        let snapshots = runtime
            .inner
            .host_runtime
            .lock()
            .expect("runtime mutex")
            .as_ref()
            .expect("runtime")
            .task_snapshots()
            .expect("task snapshots");
        assert_eq!(snapshots.len(), 3);
        assert!(
            snapshots
                .iter()
                .all(|task| task.status == TaskStatus::Completed)
        );
        assert!(
            snapshots
                .iter()
                .all(|task| task.correlation_id.as_deref() == Some("corr-mock-1"))
        );

        let unauthorized_sources = runtime
            .inner
            .handle_request(ControlRequest {
                token: "wrong".into(),
                method: ControlMethod::EventSourceList,
                params: Value::Null,
            })
            .await;
        assert_eq!(
            unauthorized_sources.error.expect("unauthorized").code,
            "unauthorized"
        );
        let sources = runtime
            .inner
            .handle_request(ControlRequest {
                token: "test-token".into(),
                method: ControlMethod::EventSourceList,
                params: Value::Null,
            })
            .await;
        let sources: Vec<mutsuki_service_control::EventSourceStatus> =
            serde_json::from_value(sources.result.expect("sources")).expect("source statuses");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].state, "running");
        assert_eq!(sources[0].health, "healthy");
        assert!(sources[0].last_event_unix_ms.is_some());

        let health = runtime.inner.health_check().await;
        let health: HealthReport =
            serde_json::from_value(health.result.expect("health")).expect("health report");
        assert_eq!(health.event_sources, "ok");
        assert_eq!(health.event_source_details[0].source_id, "mock-gateway");

        let reload = runtime.inner.plugin_reload().await;
        assert!(reload.ok);
        let reload: PluginReloadResponse =
            serde_json::from_value(reload.result.expect("reload")).expect("reload response");
        assert_eq!(reload.event_sources, "kept");
        assert_eq!(source_starts.load(Ordering::SeqCst), 1);

        let restart = runtime
            .inner
            .handle_request(ControlRequest {
                token: "test-token".into(),
                method: ControlMethod::EventSourceRestart,
                params: json!({ "id": "mock-gateway" }),
            })
            .await;
        assert!(restart.ok);
        wait_for_count(&echo_count, 2).await;
        let sources = runtime.inner.event_sources.list();
        assert_eq!(sources[0].reconnects, 1);
        assert_eq!(sources[0].state, "running");

        runtime.shutdown().await;
        assert_eq!(source_starts.load(Ordering::SeqCst), 2);
        assert_eq!(source_stops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn health_reports_event_source_runtime_failure_without_stopping_service() {
        let dir = tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("logs")).expect("logs");
        let mut config = ServiceConfig::default();
        config.ipc.enabled = false;
        config.ipc.token = Some("test-token".into());
        config.observe.console = false;
        config.service.log_dir = dir.path().join("logs");
        config.plugins.builtin.clear();
        config.plugins.dynamic_dirs.clear();
        config.runners.graceful_shutdown_ms = 50;
        let runtime = ServiceRuntimeBuilder::new(config)
            .register_event_source(Box::new(FailingGatewaySource {
                descriptor: HostEventSourceDescriptor::new("failed-gateway", "test.gateway"),
            }))
            .start()
            .await
            .expect("service stays available");
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.inner.event_sources.list()[0].state != "failed" {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("source fails");

        let health = runtime.inner.health_check().await;
        let health: HealthReport =
            serde_json::from_value(health.result.expect("health")).expect("health report");
        assert_eq!(health.service, "ok");
        assert_eq!(health.core, "ok");
        assert_eq!(health.event_sources, "degraded");
        assert!(health.recent_errors[0].contains("failed-gateway"));
        runtime.shutdown().await;
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("runtime completed chain");
    }

    struct MockGatewaySource {
        descriptor: HostEventSourceDescriptor,
        starts: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
    }

    struct FailingGatewaySource {
        descriptor: HostEventSourceDescriptor,
    }

    impl HostEventSource for FailingGatewaySource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, _ctx: HostEventSourceContext) -> HostEventSourceFuture {
            Box::pin(async { Err(std::io::Error::other("gateway disconnected").into()) })
        }

        fn shutdown(&mut self) -> HostEventSourceFuture {
            Box::pin(async { Ok(()) })
        }

        fn health(&self) -> HostEventSourceHealth {
            HostEventSourceHealth::Unhealthy("gateway disconnected".into())
        }
    }

    impl HostEventSource for MockGatewaySource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, ctx: HostEventSourceContext) -> HostEventSourceFuture {
            let sequence = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                let mut task = Task::new(
                    format!("mock-gateway-{sequence}"),
                    "bot.gateway",
                    json!({ "text": "/echo hello" }),
                );
                task.correlation_id = Some(format!("corr-mock-{sequence}"));
                ctx.task_submitter.submit_task(task)?;
                let mut shutdown = ctx.shutdown;
                shutdown.cancelled().await;
                Ok(())
            })
        }

        fn shutdown(&mut self) -> HostEventSourceFuture {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }

        fn health(&self) -> HostEventSourceHealth {
            HostEventSourceHealth::Healthy
        }
    }

    struct ChainRunner {
        descriptor: RunnerDescriptor,
        next_protocol: Option<&'static str>,
        terminal_count: Option<Arc<AtomicUsize>>,
    }

    impl ChainRunner {
        fn next(descriptor: RunnerDescriptor, next_protocol: &'static str) -> Self {
            Self {
                descriptor,
                next_protocol: Some(next_protocol),
                terminal_count: None,
            }
        }

        fn terminal(descriptor: RunnerDescriptor, count: Arc<AtomicUsize>) -> Self {
            Self {
                descriptor,
                next_protocol: None,
                terminal_count: Some(count),
            }
        }
    }

    impl Runner for ChainRunner {
        fn descriptor(&self) -> &RunnerDescriptor {
            &self.descriptor
        }

        fn run_batch(
            &mut self,
            ctx: mutsuki_runtime_contracts::RunnerContext,
            batch: WorkBatch,
        ) -> RuntimeResult<CompletionBatch> {
            let next_protocol = self.next_protocol;
            let terminal_count = self.terminal_count.clone();
            map_work_batch_entries(&batch, |task| {
                let mut result = mutsuki_runtime_contracts::RunnerResult::completed(&task.task_id);
                if let Some(protocol) = next_protocol {
                    let mut next = Task::new(
                        format!("{}:{protocol}", task.task_id),
                        protocol,
                        task.payload.clone(),
                    );
                    next.registry_generation = ctx.registry_generation;
                    next.correlation_id = task.correlation_id.clone();
                    result.tasks.push(next);
                } else if let Some(count) = &terminal_count {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                Ok(result)
            })
        }
    }

    fn chain_descriptor(plugin_id: &str, runner_id: &str, protocol: &str) -> RunnerDescriptor {
        RunnerDescriptor {
            runner_id: runner_id.into(),
            plugin_id: plugin_id.into(),
            plugin_generation: 1,
            accepted_protocol_ids: vec![protocol.into()],
            purity: RunnerPurity::Pure,
            execution_class: ExecutionClass::Orchestration,
            input_schema: json!({}),
            output_schema: json!({}),
            batch: RunnerBatchCapability::default(),
            payload: RunnerPayloadCapability::default(),
            resources: RunnerResourceCapability::default(),
            ordering: RunnerOrderingCapability::default(),
            control: RunnerControlCapability::default(),
            metadata: BTreeMap::new(),
            contract_surfaces: vec![
                format!("runner:{runner_id}"),
                format!("task_protocol:{protocol}"),
            ],
        }
    }

    fn runner_manifest(plugin_id: &str, descriptor: RunnerDescriptor) -> PluginManifest {
        mutsuki_runtime_host::runner_manifest(plugin_id, vec![descriptor])
    }

    fn test_runtime_inner(token: &str) -> ServiceRuntimeInner {
        let mut config = ServiceConfig::default();
        config.ipc.token = Some(token.into());
        let mut registry = BuiltinRegistry::new();
        registry.register(mutsuki_service_plugin_conversation_sim::plugin());
        let selection = registry
            .load_requested(&[mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into()])
            .expect("builtin available");
        let catalog = PluginCatalog::scan(&[], Path::new("missing-disabled"), selection)
            .expect("catalog scan");

        ServiceRuntimeInner {
            config,
            started_at: Instant::now(),
            catalog: Mutex::new(catalog),
            host_runtime: Mutex::new(None),
            supervisor: RunnerSupervisor::new(),
            event_sources: EventSourceSupervisor::default(),
            builtin_registry: registry,
            native_runner_factories: Vec::new(),
            shutdown_tx: Mutex::new(None),
        }
    }

    fn test_started_runtime_inner(token: &str, root: &Path) -> ServiceRuntimeInner {
        let mut config = ServiceConfig::default();
        config.ipc.token = Some(token.into());
        config.service.home_dir = root.to_path_buf();
        config.service.log_dir = root.join("logs");
        config.service.run_dir = root.join("run");
        config.plugins.builtin = vec![mutsuki_service_plugin_conversation_sim::PLUGIN_ID.into()];
        config.plugins.dynamic_dirs = vec![root.join("installed")];
        config.plugins.disabled_dir = root.join("disabled");
        let registry = builtin_registry();
        let catalog = load_catalog(&config, &registry).expect("catalog");
        let host_runtime = boot_core(&config, &catalog, &[]).expect("core");
        ServiceRuntimeInner {
            config,
            started_at: Instant::now(),
            catalog: Mutex::new(catalog),
            host_runtime: Mutex::new(Some(host_runtime)),
            supervisor: RunnerSupervisor::new(),
            event_sources: EventSourceSupervisor::default(),
            builtin_registry: registry,
            native_runner_factories: Vec::new(),
            shutdown_tx: Mutex::new(None),
        }
    }

    fn minimal_manifest(plugin_id: &str) -> PluginManifest {
        PluginManifest {
            plugin_id: plugin_id.into(),
            version: "0.1.0".into(),
            api_version: "mutsuki-plugin-v1".into(),
            artifact: PluginArtifact {
                artifact_type: ArtifactType::Native,
                path: "native".into(),
                sha256: "sha256:native".into(),
            },
            provides: PluginProvides::default(),
            requires: Vec::new(),
            permissions: PermissionGrant {
                effects: Vec::new(),
                resources: Vec::new(),
            },
            lifecycle: LifecyclePolicy {
                reload_policy: "drain_and_swap".into(),
                unload_timeout_ms: 100,
                supports_cancel: true,
                supports_dispose: true,
                supports_snapshot: false,
            },
            metadata: BTreeMap::new(),
        }
    }
}
