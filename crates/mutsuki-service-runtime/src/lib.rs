use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mutsuki_runtime_contracts::{
    PluginDeploymentKind, RunnerContext, RunnerDescriptor, RunnerResult, RuntimeProfile,
    RuntimeProfileMode, Task,
};
use mutsuki_runtime_core::{Runner, RuntimeFailure, RuntimeResult};
use mutsuki_runtime_host::{
    HostRuntime, HostRuntimeCommand, HostRuntimeConfig, RuntimeBootstrapper,
};
use mutsuki_service_config::{ServiceConfig, filtered_environment};
use mutsuki_service_control::{
    ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest, ControlResponse,
    CoreStatus, HealthReport, IdParam, PluginStatus, RunnerStatus as ControlRunnerStatus,
    ServiceStatus,
};
use mutsuki_service_ipc::IpcServer;
use mutsuki_service_plugin_loader::{
    BuiltinRegistry, ExternalRuntimeSpec, PluginCatalog, PluginLoaderError, PluginRecord,
};
use mutsuki_service_runner_supervisor::{
    ManagedRunnerSpec, RunnerProcessState, RunnerSnapshot, RunnerSupervisor,
};
use serde_json::{Value, json};
use tokio::sync::oneshot;

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
}

pub type ServiceRuntimeResult<T> = Result<T, ServiceRuntimeError>;

pub struct ServiceRuntime {
    inner: Arc<ServiceRuntimeInner>,
    shutdown_rx: Option<oneshot::Receiver<String>>,
    ipc_server: Option<IpcServer>,
    _observe: mutsuki_service_observe::ObserveGuard,
}

struct ServiceRuntimeInner {
    config: ServiceConfig,
    started_at: Instant,
    catalog: PluginCatalog,
    host_runtime: Mutex<Option<HostRuntime>>,
    supervisor: RunnerSupervisor,
    shutdown_tx: Mutex<Option<oneshot::Sender<String>>>,
}

impl ServiceRuntime {
    pub async fn start(config: ServiceConfig) -> ServiceRuntimeResult<Self> {
        let observe = mutsuki_service_observe::init_observe(&config);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let supervisor = RunnerSupervisor::new();
        let catalog = load_catalog(&config)?;
        let host_runtime = boot_core(&config, &catalog)?;
        start_supervised_sidecars(&config, &catalog, &supervisor).await;

        let inner = Arc::new(ServiceRuntimeInner {
            config,
            started_at: Instant::now(),
            catalog,
            host_runtime: Mutex::new(Some(host_runtime)),
            supervisor,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
        });
        let ipc_server = mutsuki_service_ipc::start_server(
            &inner.config,
            Arc::new(RuntimeControl {
                inner: inner.clone(),
            }),
        )
        .await?;
        Ok(Self {
            inner,
            shutdown_rx: Some(shutdown_rx),
            ipc_server,
            _observe: observe,
        })
    }

    pub async fn run_foreground(mut self) -> ServiceRuntimeResult<()> {
        let shutdown_rx = self
            .shutdown_rx
            .take()
            .ok_or(ServiceRuntimeError::AlreadyStarted)?;
        tokio::select! {
            reason = shutdown_rx => {
                tracing::info!(reason = ?reason, "service shutdown requested");
            }
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "failed to listen for ctrl-c");
                }
                tracing::info!("ctrl-c received");
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
        self.inner.supervisor.shutdown(graceful).await;
        let _ = self
            .inner
            .host_runtime
            .lock()
            .expect("host runtime mutex")
            .take();
    }
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
            ControlMethod::PluginReload => {
                ControlResponse::err(ControlError::Unsupported("plugin.reload generation swap"))
            }
            ControlMethod::RunnerList => self.runner_list().await,
            ControlMethod::RunnerRestart => self.runner_restart(request.params).await,
            ControlMethod::RunnerStop => self.runner_stop(request.params).await,
            ControlMethod::TaskList => {
                ControlResponse::err(ControlError::Unsupported("task.list snapshot"))
            }
            ControlMethod::TaskCancel => self.task_cancel(request.params),
            ControlMethod::HealthCheck => self.health_check().await,
            ControlMethod::LogTail => ControlResponse::err(ControlError::Unsupported("log.tail")),
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
            plugin_count: self.catalog.records.len(),
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
        let plugins = self
            .catalog
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

    fn task_cancel(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<IdParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        let mut guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_mut() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        match runtime.dispatch(HostRuntimeCommand::CancelTask(param.id)) {
            Ok(_) => ControlResponse::empty_ok(),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    async fn health_check(&self) -> ControlResponse {
        let runners = self.supervisor.list().await;
        let runner_health = if runners
            .iter()
            .any(|runner| matches!(runner.state, RunnerProcessState::Failed))
        {
            "degraded"
        } else {
            "ok"
        };
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
            recent_errors: Vec::new(),
        };
        ControlResponse::ok(report)
    }
}

fn load_catalog(config: &ServiceConfig) -> ServiceRuntimeResult<PluginCatalog> {
    let builtin = BuiltinRegistry::new().load_requested(&config.plugins.builtin)?;
    Ok(PluginCatalog::scan(
        &config.plugins.dynamic_dirs,
        &config.plugins.disabled_dir,
        builtin,
    )?)
}

fn boot_core(config: &ServiceConfig, catalog: &PluginCatalog) -> ServiceRuntimeResult<HostRuntime> {
    let mut bootstrapper = RuntimeBootstrapper::new();
    let mut enabled_plugins = Vec::new();
    let mut deployments = BTreeMap::new();

    for record in &catalog.records {
        if !record.enabled {
            continue;
        }
        if is_bootable_record(record) {
            bootstrapper.register_manifest(record.manifest.clone());
            enabled_plugins.push(record.manifest.plugin_id.clone());
            let deployment =
                PluginDeploymentKind::default_for_artifact(&record.manifest.artifact.artifact_type);
            deployments.insert(record.manifest.plugin_id.clone(), deployment.clone());
            if let Some(runtime) = &record.runtime {
                register_stdio_runners(config, &mut bootstrapper, record, runtime, deployment)?;
            }
        }
    }

    let profile = RuntimeProfile {
        profile_id: config.service.profile.clone(),
        mode: RuntimeProfileMode::ExtensibleRuntime,
        enabled_plugins,
        bindings: BTreeMap::new(),
        plugin_deployments: deployments,
        allow_dynamic_registration: false,
        allow_hot_reload: true,
    };
    let mut host_config = HostRuntimeConfig::default();
    host_config.worker_threads = config.core.worker_threads;
    host_config.blocking_threads = config.core.blocking_threads;
    Ok(bootstrapper.into_host_runtime_with_config(profile, host_config)?)
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
        let runner = StdioJsonlRunner::spawn(
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
    for record in catalog.external_records() {
        let Some(runtime) = &record.runtime else {
            continue;
        };
        if runtime.runner_link == "jsonl-stdio" && !record.manifest.provides.runners.is_empty() {
            continue;
        }
        let runner_id = record
            .manifest
            .provides
            .runners
            .first()
            .map(|runner| runner.runner_id.clone())
            .unwrap_or_else(|| format!("sidecar:{}", record.manifest.plugin_id));
        let spec = ManagedRunnerSpec {
            runner_id,
            plugin_id: record.manifest.plugin_id.clone(),
            runtime: runtime.clone(),
            env_allowlist: config.runners.env_allowlist.clone(),
            service_home: config.service.home_dir.clone(),
            session_token: config.control_token().to_string(),
        };
        if let Err(error) = supervisor.start(spec).await {
            tracing::error!(error = %error, "failed to start supervised runner");
        }
    }
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

struct StdioJsonlRunner {
    descriptor: RunnerDescriptor,
    child: Child,
    reader: BufReader<ChildStdout>,
    writer: ChildStdin,
    next_request: u64,
}

impl StdioJsonlRunner {
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
            descriptor,
            child,
            reader: BufReader::new(stdout),
            writer: stdin,
            next_request: 0,
        })
    }

    fn request(&mut self, method: &str, params: Value) -> RuntimeResult<Value> {
        self.next_request += 1;
        let id = format!("req-{}", self.next_request);
        let request = json!({"id": id, "method": method, "params": params});
        serde_json::to_writer(&mut self.writer, &request).map_err(jsonl_failure)?;
        self.writer.write_all(b"\n").map_err(io_failure)?;
        self.writer.flush().map_err(io_failure)?;
        let mut line = String::new();
        self.reader.read_line(&mut line).map_err(io_failure)?;
        if line.trim().is_empty() {
            return Err(runtime_failure("jsonl.protocol", "empty response"));
        }
        let response: Value = serde_json::from_str(&line).map_err(jsonl_failure)?;
        if response.get("id") != Some(&Value::String(id)) {
            return Err(runtime_failure("jsonl.protocol", "response id mismatch"));
        }
        match response.get("ok").and_then(Value::as_bool) {
            Some(true) => Ok(response.get("result").cloned().unwrap_or(Value::Null)),
            Some(false) => {
                let error_value = response
                    .get("error")
                    .cloned()
                    .ok_or_else(|| runtime_failure("jsonl.protocol", "missing error"))?;
                let error = serde_json::from_value(error_value).map_err(jsonl_failure)?;
                Err(RuntimeFailure::new(error))
            }
            None => Err(runtime_failure("jsonl.protocol", "missing ok flag")),
        }
    }
}

impl Runner for StdioJsonlRunner {
    fn descriptor(&self) -> &RunnerDescriptor {
        &self.descriptor
    }

    fn step(&mut self, ctx: RunnerContext, tasks: Vec<Task>) -> RuntimeResult<Vec<RunnerResult>> {
        let result = self.request(
            "runner.step",
            json!({
                "runner_id": self.descriptor.runner_id,
                "ctx": ctx,
                "tasks": tasks,
            }),
        )?;
        serde_json::from_value(result).map_err(jsonl_failure)
    }

    fn cancel(&mut self, invocation_id: &str) -> RuntimeResult<()> {
        self.request(
            "runner.cancel",
            json!({
                "runner_id": self.descriptor.runner_id,
                "invocation_id": invocation_id,
            }),
        )?;
        Ok(())
    }

    fn dispose(&mut self) -> RuntimeResult<()> {
        self.request(
            "runner.dispose",
            json!({"runner_id": self.descriptor.runner_id}),
        )?;
        let _ = self.child.kill();
        Ok(())
    }
}

fn drain_blocking_stderr(runner_id: String, stderr: std::process::ChildStderr) {
    let reader = BufReader::new(stderr);
    for line in reader.lines().map_while(Result::ok) {
        tracing::warn!(runner_id, stream = "stderr", line);
    }
}

fn io_failure(error: std::io::Error) -> RuntimeFailure {
    runtime_failure("jsonl.io", error.to_string())
}

fn jsonl_failure(error: serde_json::Error) -> RuntimeFailure {
    runtime_failure("jsonl.decode", error.to_string())
}

fn runtime_failure(route: impl Into<String>, reason: impl Into<String>) -> RuntimeFailure {
    let mut error = mutsuki_runtime_contracts::RuntimeError::new(
        mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
        "mutsuki_service_host",
        route,
    );
    error.evidence.insert(
        "reason".into(),
        mutsuki_runtime_contracts::ScalarValue::String(reason.into()),
    );
    RuntimeFailure::new(error)
}
