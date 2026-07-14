use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
#[cfg(test)]
use std::io::Write;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Waker;
use std::time::{Duration, Instant};

use mutsuki_runtime_contracts::resource::experimental::{CommandBatch, SagaPlan};
use mutsuki_runtime_contracts::{
    CancelPolicy, CommandPlan, ExportPlan, PlanReceipt, PluginDeploymentKind, ReadPlan,
    ResourceRef, RuntimeProfile, RuntimeProfileMode, SnapshotDescriptor, StreamPlan,
    SurfaceCompatibility, TaskBatch, TaskHandle, TaskOutcome, TaskStatus, WritePlan,
};
use mutsuki_runtime_core::{Runner, RuntimeFailure, RuntimeResult, RuntimeStopState};
#[cfg(test)]
use mutsuki_runtime_host::JsonlRunner;
use mutsuki_runtime_host::{
    HostRuntime, HostRuntimeCommand, HostRuntimeConfig, HostRuntimeReply, HostTaskSnapshot,
    ProcessRunnerSpec, RuntimeBootstrapper, SpawnedJsonlRunner, resolve_load_plan,
};
use mutsuki_runtime_sdk::{
    LoadedPlugin, ResourcePlanGateway, ResourceRegistryGateway, RuntimeClient, RuntimeClientRef,
    TaskSubmitter, TaskSubmitterRuntimeClient,
};
#[cfg(test)]
use mutsuki_service_config::ConfiguredPluginSelection;
use mutsuki_service_config::{
    ConfiguredPluginStore, HostSecretStore, ServiceConfig, filtered_environment,
};
use mutsuki_service_control::{
    ControlError, ControlFuture, ControlHandler, ControlMethod, ControlRequest, ControlResponse,
    CoreDrainResponse, CoreStatus, HealthReport, IdParam, LogTailEntry, LogTailParams,
    LogTailResponse, PluginCandidateStatus, PluginDeploymentClearParam, PluginDeploymentParam,
    PluginInventoryDiagnostic, PluginListResponse, PluginReloadChange, PluginReloadResponse,
    PluginStatus, RunnerStatus as ControlRunnerStatus, ServiceStatus, TaskEventPage,
    TaskEventsAfterParam, TaskFailureSummary as ControlTaskFailureSummary, TaskOutcomeView,
    TaskSnapshot as ControlTaskSnapshot, TaskSubmitBatchParam, TaskSubmitBatchResponse,
};
use mutsuki_service_ipc::IpcServer;
use mutsuki_service_plugin_loader::{
    BuiltinRegistry, ExternalRuntimeSpec, PluginCatalog, PluginInventory, PluginLoaderError,
    PluginRecord,
};
use mutsuki_service_runner_supervisor::{
    ManagedRunnerSpec, RunnerProcessState, RunnerSnapshot, RunnerSupervisor,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{oneshot, watch};

mod abi_plugin;
mod event_source;

use event_source::EventSourceSupervisor;
pub use event_source::{
    HostEventSource, HostEventSourceConfig, HostEventSourceContext, HostEventSourceDescriptor,
    HostEventSourceError, HostEventSourceFuture, HostEventSourceHealth, HostEventSourceLogger,
    HostShutdownToken,
};

type NativeRunnerFactory = Arc<dyn Fn() -> Result<Box<dyn Runner>, String> + Send + Sync>;
type LoadedPluginFactory = Arc<dyn Fn() -> Result<LoadedPlugin, String> + Send + Sync>;
type HealthProbe = Arc<dyn Fn() -> Value + Send + Sync>;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PluginDeploymentState {
    #[serde(default = "deployment_state_version")]
    version: u32,
    #[serde(default)]
    plugins: BTreeMap<String, PluginDeploymentKind>,
}

impl Default for PluginDeploymentState {
    fn default() -> Self {
        Self {
            version: deployment_state_version(),
            plugins: BTreeMap::new(),
        }
    }
}

fn deployment_state_version() -> u32 {
    1
}

#[derive(Default)]
struct DeferredRuntimeClient {
    runtime: OnceLock<RuntimeClientRef>,
    task_submitter: OnceLock<Arc<dyn TaskSubmitter>>,
    resource_gateway: OnceLock<Arc<dyn ResourcePlanGateway>>,
    resource_registry: OnceLock<Arc<dyn ResourceRegistryGateway>>,
}

impl DeferredRuntimeClient {
    fn bind(
        &self,
        task_submitter: Arc<dyn TaskSubmitter>,
        resource_gateway: Arc<dyn ResourcePlanGateway>,
        resource_registry: Arc<dyn ResourceRegistryGateway>,
    ) {
        let runtime = TaskSubmitterRuntimeClient::new(task_submitter.clone()).into_runtime_client();
        assert!(
            self.runtime.set(runtime).is_ok(),
            "runtime client already bound"
        );
        assert!(
            self.task_submitter.set(task_submitter).is_ok(),
            "task submitter already bound"
        );
        assert!(
            self.resource_gateway.set(resource_gateway).is_ok(),
            "resource gateway already bound"
        );
        assert!(
            self.resource_registry.set(resource_registry).is_ok(),
            "resource registry already bound"
        );
    }

    fn client(&self) -> RuntimeResult<RuntimeClientRef> {
        self.runtime.get().cloned().ok_or_else(|| {
            RuntimeFailure::new(mutsuki_runtime_contracts::RuntimeError::new(
                mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
                "mutsuki.service.runtime",
                "runtime_client.not_bound",
            ))
        })
    }

    fn task_submitter(&self) -> RuntimeResult<Arc<dyn TaskSubmitter>> {
        self.task_submitter
            .get()
            .cloned()
            .ok_or_else(deferred_not_bound)
    }

    fn resource_gateway(&self) -> RuntimeResult<Arc<dyn ResourcePlanGateway>> {
        self.resource_gateway
            .get()
            .cloned()
            .ok_or_else(deferred_not_bound)
    }

    fn resource_registry(&self) -> RuntimeResult<Arc<dyn ResourceRegistryGateway>> {
        self.resource_registry
            .get()
            .cloned()
            .ok_or_else(deferred_not_bound)
    }
}

fn deferred_not_bound() -> RuntimeFailure {
    RuntimeFailure::new(mutsuki_runtime_contracts::RuntimeError::new(
        mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
        "mutsuki.service.runtime",
        "runtime_client.not_bound",
    ))
}

impl RuntimeClient for DeferredRuntimeClient {
    fn submit_batch(&self, batch: TaskBatch) -> RuntimeResult<Vec<TaskHandle>> {
        self.client()?.submit_batch(batch)
    }

    fn task_outcome(&self, handle: &TaskHandle) -> RuntimeResult<Option<TaskOutcome>> {
        self.client()?.task_outcome(handle)
    }

    fn register_waker(&self, handle: &TaskHandle, waker: &Waker) {
        if let Ok(client) = self.client() {
            client.register_waker(handle, waker);
        }
    }
}

impl TaskSubmitter for DeferredRuntimeClient {
    fn submit_batch(&self, batch: TaskBatch) -> RuntimeResult<Vec<TaskHandle>> {
        self.task_submitter()?.submit_batch(batch)
    }

    fn cancel_task(&self, handle: &TaskHandle) -> RuntimeResult<()> {
        self.task_submitter()?.cancel_task(handle)
    }

    fn task_outcome(&self, handle: &TaskHandle) -> RuntimeResult<Option<TaskOutcome>> {
        self.task_submitter()?.task_outcome(handle)
    }
}

impl ResourcePlanGateway for DeferredRuntimeClient {
    fn collect_read_plan(&self, plan: &ReadPlan) -> RuntimeResult<Vec<u8>> {
        self.resource_gateway()?.collect_read_plan(plan)
    }

    fn snapshot_read_plan(
        &self,
        plan: &ReadPlan,
        kind_id: &str,
        schema: &str,
    ) -> RuntimeResult<SnapshotDescriptor> {
        self.resource_gateway()?
            .snapshot_read_plan(plan, kind_id, schema)
    }

    fn open_stream_plan(&self, plan: &ReadPlan) -> RuntimeResult<StreamPlan> {
        self.resource_gateway()?.open_stream_plan(plan)
    }

    fn execute_export_plan(&self, plan: &ExportPlan) -> RuntimeResult<PlanReceipt> {
        self.resource_gateway()?.execute_export_plan(plan)
    }

    fn commit_write_plan(&self, plan: &WritePlan, bytes: Vec<u8>) -> RuntimeResult<PlanReceipt> {
        self.resource_gateway()?.commit_write_plan(plan, bytes)
    }

    fn execute_command_plan(&self, plan: &CommandPlan) -> RuntimeResult<PlanReceipt> {
        self.resource_gateway()?.execute_command_plan(plan)
    }

    fn execute_command_batch(&self, batch: &CommandBatch) -> RuntimeResult<Vec<PlanReceipt>> {
        self.resource_gateway()?.execute_command_batch(batch)
    }

    fn execute_saga_plan(&self, saga: &SagaPlan) -> RuntimeResult<Vec<PlanReceipt>> {
        self.resource_gateway()?.execute_saga_plan(saga)
    }
}

impl ResourceRegistryGateway for DeferredRuntimeClient {
    fn open_resource_descriptor(&self, ref_id: &str) -> RuntimeResult<ResourceRef> {
        self.resource_registry()?.open_resource_descriptor(ref_id)
    }

    fn create_blob_resource(
        &self,
        provider_id: &str,
        schema: &str,
        bytes: Vec<u8>,
    ) -> RuntimeResult<ResourceRef> {
        self.resource_registry()?
            .create_blob_resource(provider_id, schema, bytes)
    }

    fn create_cow_state_resource(
        &self,
        provider_id: &str,
        kind_id: &str,
        schema: &str,
        bytes: Vec<u8>,
    ) -> RuntimeResult<ResourceRef> {
        self.resource_registry()?
            .create_cow_state_resource(provider_id, kind_id, schema, bytes)
    }

    fn create_capability_resource(
        &self,
        provider_id: &str,
        kind_id: &str,
        schema: &str,
    ) -> RuntimeResult<ResourceRef> {
        self.resource_registry()?
            .create_capability_resource(provider_id, kind_id, schema)
    }
}

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
    #[error("external runner {runner_id} failed to start: {detail}")]
    ExternalRunnerSpawn { runner_id: String, detail: String },
    #[error("service runtime already started")]
    AlreadyStarted,
    #[error("event source registration failed: {0}")]
    EventSource(String),
    #[error("native runner factory failed: {0}")]
    NativeRunnerFactory(String),
    #[error("configured plugin id must not be empty")]
    EmptyConfiguredPluginId,
    #[error("configured plugin {0} is selected more than once")]
    DuplicateConfiguredPlugin(String),
    #[error("configured plugin factory is not registered: {0}")]
    UnknownConfiguredPlugin(String),
    #[error("configured plugin {plugin_id} contains raw credential field {field}")]
    RawConfiguredPluginSecret { plugin_id: String, field: String },
    #[error("configured plugin {plugin_id} failed to install: {detail}")]
    ConfiguredPluginInstall { plugin_id: String, detail: String },
    #[error("ABI plugin {plugin_id} failed to load: {detail}")]
    AbiPlugin { plugin_id: String, detail: String },
    #[error("configured plugin {plugin_id} has no available artifact: {detail}")]
    PluginUnavailable { plugin_id: String, detail: String },
    #[error("configured plugin {plugin_id} has multiple deployments and requires a Host selection")]
    PluginDeploymentAmbiguous { plugin_id: String },
    #[error("configured plugin {plugin_id} deployment {deployment:?} is unavailable: {detail}")]
    PluginDeploymentUnavailable {
        plugin_id: String,
        deployment: PluginDeploymentKind,
        detail: String,
    },
    #[error(
        "plugin {plugin_id} builtin and selected deployment expose different business surfaces"
    )]
    PluginBusinessSurfaceMismatch { plugin_id: String },
    #[error("failed to read or write plugin deployment state {path}: {detail}")]
    PluginDeploymentState { path: String, detail: String },
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
    configured_plugins: ConfiguredPluginCatalog,
    builtin_registry: BuiltinRegistry,
    native_runner_factories: Vec<NativeRunnerFactory>,
    loaded_plugin_factories: BTreeMap<String, LoadedPluginFactory>,
    runtime_client: Arc<DeferredRuntimeClient>,
    health_probes: BTreeMap<String, HealthProbe>,
    event_sources: Vec<Box<dyn HostEventSource>>,
}

/// Domain-neutral factory for a native product plugin selected by Host configuration.
///
/// Domain repositories own config decoding and installation. ServiceHost only resolves the
/// selection before the runtime profile and load plan are frozen.
pub trait ConfiguredPluginFactory: Send + Sync {
    fn plugin_id(&self) -> &str;

    fn prepare(
        &self,
        config: &Value,
        builder: ServiceRuntimeBuilder,
    ) -> Result<ServiceRuntimeBuilder, String>;
}

#[derive(Clone, Default)]
pub struct ConfiguredPluginCatalog {
    factories: BTreeMap<String, Arc<dyn ConfiguredPluginFactory>>,
}

impl ConfiguredPluginCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, factory: F) -> ServiceRuntimeResult<()>
    where
        F: ConfiguredPluginFactory + 'static,
    {
        let plugin_id = factory.plugin_id().trim();
        if plugin_id.is_empty() {
            return Err(ServiceRuntimeError::EmptyConfiguredPluginId);
        }
        if self.factories.contains_key(plugin_id) {
            return Err(ServiceRuntimeError::DuplicateConfiguredPlugin(
                plugin_id.into(),
            ));
        }
        self.factories.insert(plugin_id.into(), Arc::new(factory));
        Ok(())
    }

    /// Atomically merges another owner catalog and rejects any duplicate plugin id.
    pub fn merge(&mut self, other: Self) -> ServiceRuntimeResult<()> {
        if let Some(plugin_id) = other
            .factories
            .keys()
            .find(|plugin_id| self.factories.contains_key(*plugin_id))
        {
            return Err(ServiceRuntimeError::DuplicateConfiguredPlugin(
                plugin_id.clone(),
            ));
        }
        self.factories.extend(other.factories);
        Ok(())
    }

    fn factory(&self, plugin_id: &str) -> Option<Arc<dyn ConfiguredPluginFactory>> {
        self.factories.get(plugin_id).cloned()
    }
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
    loaded_plugin_factories: BTreeMap<String, LoadedPluginFactory>,
    health_probes: BTreeMap<String, HealthProbe>,
    runtime_client: Arc<DeferredRuntimeClient>,
    deployment_state: Mutex<PluginDeploymentState>,
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
            configured_plugins: ConfiguredPluginCatalog::new(),
            builtin_registry: builtin_registry(),
            native_runner_factories: Vec::new(),
            loaded_plugin_factories: BTreeMap::new(),
            runtime_client: Arc::new(DeferredRuntimeClient::default()),
            health_probes: BTreeMap::new(),
            event_sources: Vec::new(),
        }
    }

    pub fn with_configured_plugin_catalog(mut self, catalog: ConfiguredPluginCatalog) -> Self {
        self.configured_plugins = catalog;
        self
    }

    /// Product-data root used by owner plugins for their private durable state.
    pub fn data_dir(&self) -> &std::path::Path {
        &self.config.service.data_dir
    }

    /// Host-owned mutable secret boundary for integration crates that rotate credentials.
    pub fn host_secret_store(&self) -> HostSecretStore {
        self.config.host_secret_store()
    }

    /// Host-owned persistence boundary for owner-defined configured plugin data.
    pub fn configured_plugin_store(&self) -> Option<ConfiguredPluginStore> {
        self.config.configured_plugin_store()
    }

    /// Registers and enables a product-provided builtin manifest before the load plan is built.
    pub fn register_builtin_plugin(
        mut self,
        manifest: mutsuki_runtime_contracts::PluginManifest,
    ) -> Self {
        self.builtin_registry.register_manifest(manifest);
        self
    }

    /// Registers a recreatable native runner factory for initial boot and every Core reload.
    pub fn register_builtin_runner<F>(mut self, factory: F) -> Self
    where
        F: Fn() -> Box<dyn Runner> + Send + Sync + 'static,
    {
        self.native_runner_factories
            .push(Arc::new(move || Ok(factory())));
        self
    }

    /// Registers a recreatable native runner whose external client or other
    /// fallible dependency is initialized at boot and every Core reload.
    pub fn register_fallible_builtin_runner<F, E>(mut self, factory: F) -> Self
    where
        F: Fn() -> Result<Box<dyn Runner>, E> + Send + Sync + 'static,
        E: std::fmt::Display,
    {
        self.native_runner_factories.push(Arc::new(move || {
            factory().map_err(|error| error.to_string())
        }));
        self
    }

    /// Registers a recreatable runner that needs the booted runtime client for nested calls.
    pub fn register_runtime_client_runner<F>(mut self, factory: F) -> Self
    where
        F: Fn(RuntimeClientRef) -> Box<dyn Runner> + Send + Sync + 'static,
    {
        let client = self.runtime_client.clone();
        self.native_runner_factories
            .push(Arc::new(move || Ok(factory(client.clone()))));
        self
    }

    /// Registers a recreatable runner with both nested-task and host resource services.
    pub fn register_runtime_services_runner<F>(mut self, factory: F) -> Self
    where
        F: Fn(RuntimeClientRef, Arc<dyn ResourceRegistryGateway>) -> Box<dyn Runner>
            + Send
            + Sync
            + 'static,
    {
        let services = self.runtime_client.clone();
        self.native_runner_factories.push(Arc::new(move || {
            Ok(factory(services.clone(), services.clone()))
        }));
        self
    }

    /// Registers a recreatable runtime-services runner with fallible artifact or client setup.
    pub fn register_fallible_runtime_services_runner<F, E>(mut self, factory: F) -> Self
    where
        F: Fn(RuntimeClientRef, Arc<dyn ResourceRegistryGateway>) -> Result<Box<dyn Runner>, E>
            + Send
            + Sync
            + 'static,
        E: std::fmt::Display,
    {
        let services = self.runtime_client.clone();
        self.native_runner_factories.push(Arc::new(move || {
            factory(services.clone(), services.clone()).map_err(|error| error.to_string())
        }));
        self
    }

    /// Registers a builtin plugin that must be reconstructed at boot and reload.
    pub fn register_builtin_loaded_plugin_factory<F, E>(
        mut self,
        manifest: mutsuki_runtime_contracts::PluginManifest,
        factory: F,
    ) -> Self
    where
        F: Fn() -> Result<LoadedPlugin, E> + Send + Sync + 'static,
        E: std::fmt::Display,
    {
        let plugin_id = manifest.plugin_id.clone();
        self.builtin_registry.register_manifest(manifest);
        self.loaded_plugin_factories.insert(
            plugin_id,
            Arc::new(move || factory().map_err(|error| error.to_string())),
        );
        self
    }

    /// Registers a domain-neutral product component snapshot for `health`.
    pub fn register_health_probe<F>(mut self, component_id: impl Into<String>, probe: F) -> Self
    where
        F: Fn() -> Value + Send + Sync + 'static,
    {
        self.health_probes
            .insert(component_id.into(), Arc::new(probe));
        self
    }

    pub fn register_event_source(mut self, source: Box<dyn HostEventSource>) -> Self {
        self.event_sources.push(source);
        self
    }

    pub async fn start(self) -> ServiceRuntimeResult<ServiceRuntime> {
        let self_ = self.install_configured_plugins()?;
        let ServiceRuntimeBuilder {
            config,
            configured_plugins: _,
            builtin_registry,
            native_runner_factories,
            loaded_plugin_factories,
            runtime_client,
            health_probes,
            event_sources,
        } = self_;
        validate_event_sources(&event_sources, &config)?;
        let observe = mutsuki_service_observe::init_observe(&config);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (core_pump_shutdown, core_pump_rx) = watch::channel(false);
        let supervisor = RunnerSupervisor::new();
        let event_source_supervisor = EventSourceSupervisor::default();
        let deployment_state = load_deployment_state(&config)?;
        let catalog = load_catalog_with_state(&config, &builtin_registry, &deployment_state)?;
        let host_runtime = boot_core(
            &config,
            &catalog,
            &native_runner_factories,
            &loaded_plugin_factories,
            runtime_client.clone(),
        )?;
        let task_submitter = host_runtime.host_context().task_submitter_ref();
        let resource_gateway = host_runtime.host_context().resource_gateway_ref();
        let resource_registry = host_runtime.host_context().resource_registry_ref();
        runtime_client.bind(task_submitter.clone(), resource_gateway, resource_registry);
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
            loaded_plugin_factories,
            health_probes,
            runtime_client,
            deployment_state: Mutex::new(deployment_state),
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

    fn install_configured_plugins(mut self) -> ServiceRuntimeResult<Self> {
        let selections = self
            .config
            .plugins
            .configured
            .iter()
            .filter(|selection| selection.enabled)
            .cloned()
            .collect::<Vec<_>>();
        let mut seen = BTreeSet::new();
        let mut resolved = Vec::with_capacity(selections.len());
        for selection in selections {
            let plugin_id = selection.id.trim();
            if plugin_id.is_empty() {
                return Err(ServiceRuntimeError::EmptyConfiguredPluginId);
            }
            if !seen.insert(plugin_id.to_owned()) {
                return Err(ServiceRuntimeError::DuplicateConfiguredPlugin(
                    plugin_id.into(),
                ));
            }
            if let Some(field) = raw_credential_field(&selection.config, "") {
                return Err(ServiceRuntimeError::RawConfiguredPluginSecret {
                    plugin_id: plugin_id.into(),
                    field,
                });
            }
            if let Some(factory) = self.configured_plugins.factory(plugin_id) {
                resolved.push((selection, factory));
            }
        }
        for (selection, factory) in resolved {
            let plugin_id = selection.id.clone();
            self = factory.prepare(&selection.config, self).map_err(|detail| {
                ServiceRuntimeError::ConfiguredPluginInstall { plugin_id, detail }
            })?;
        }
        Ok(self)
    }
}

fn raw_credential_field(value: &Value, path: &str) -> Option<String> {
    match value {
        Value::Object(object) => object.iter().find_map(|(key, value)| {
            let field = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            let normalized = key.to_ascii_lowercase().replace('-', "_");
            let is_reference = normalized.ends_with("_key") || normalized.ends_with("_ref");
            let is_credential = matches!(
                normalized.as_str(),
                "secret" | "client_secret" | "token" | "access_token" | "password" | "api_key"
            );
            if is_credential && !is_reference && !value.is_null() {
                Some(field)
            } else {
                raw_credential_field(value, &field)
            }
        }),
        Value::Array(values) => values
            .iter()
            .enumerate()
            .find_map(|(index, value)| raw_credential_field(value, &format!("{path}[{index}]"))),
        _ => None,
    }
}

fn validate_event_sources(
    sources: &[Box<dyn HostEventSource>],
    config: &ServiceConfig,
) -> ServiceRuntimeResult<()> {
    let mut ids = BTreeSet::new();
    let source_config = HostEventSourceConfig::from_service(config);
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
        let mut secret_keys = BTreeSet::new();
        for key in &descriptor.required_secrets {
            if key.trim().is_empty() || !secret_keys.insert(key) {
                return Err(ServiceRuntimeError::EventSource(format!(
                    "event source {} must declare non-empty unique required secret keys",
                    descriptor.source_id
                )));
            }
            if !source_config.contains_secret(key) {
                return Err(ServiceRuntimeError::EventSource(format!(
                    "event source {} requires missing Host secret {}",
                    descriptor.source_id, key
                )));
            }
        }
    }
    Ok(())
}

impl ServiceRuntime {
    pub async fn run_foreground(self) -> ServiceRuntimeResult<()> {
        self.run_until_shutdown_signal(platform_shutdown_signal())
            .await
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
            server.shutdown().await;
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

async fn ctrl_c_signal() -> String {
    match tokio::signal::ctrl_c().await {
        Ok(()) => "ctrl-c".to_string(),
        Err(error) => {
            tracing::warn!(error = %error, "failed to listen for ctrl-c");
            "ctrl-c-listener-error".to_string()
        }
    }
}

#[cfg(unix)]
async fn platform_shutdown_signal() -> String {
    use tokio::signal::unix::{SignalKind, signal};

    let Ok(mut terminate) = signal(SignalKind::terminate()) else {
        tracing::warn!("failed to listen for SIGTERM; falling back to ctrl-c");
        return ctrl_c_signal().await;
    };
    tokio::select! {
        reason = ctrl_c_signal() => reason,
        _ = terminate.recv() => "sigterm".to_string(),
    }
}

#[cfg(not(unix))]
async fn platform_shutdown_signal() -> String {
    ctrl_c_signal().await
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
            ControlMethod::PluginDeploymentSet => self.plugin_deployment_set(request.params).await,
            ControlMethod::PluginDeploymentClear => {
                self.plugin_deployment_clear(request.params).await
            }
            ControlMethod::RunnerList => self.runner_list().await,
            ControlMethod::RunnerRestart => self.runner_restart(request.params).await,
            ControlMethod::RunnerStop => self.runner_stop(request.params).await,
            ControlMethod::EventSourceList => self.event_source_list(),
            ControlMethod::EventSourceRestart => self.event_source_restart(request.params).await,
            ControlMethod::CoreBeginDrain => self.core_begin_drain(),
            ControlMethod::TaskSubmitBatch => self.task_submit_batch(request.params),
            ControlMethod::TaskList => self.task_list(),
            ControlMethod::TaskCancel => self.task_cancel(request.params),
            ControlMethod::TaskOutcome => self.task_outcome(request.params),
            ControlMethod::TaskEventsAfter => self.task_events_after(request.params),
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
        let state = self
            .deployment_state
            .lock()
            .expect("deployment state mutex");
        let mut plugin_ids = self
            .config
            .plugins
            .configured
            .iter()
            .map(|selection| selection.id.trim().to_string())
            .collect::<BTreeSet<_>>();
        plugin_ids.extend(
            catalog
                .candidates
                .iter()
                .map(|record| record.manifest.plugin_id.clone()),
        );
        let plugins = plugin_ids
            .into_iter()
            .map(|plugin_id| {
                let active = catalog
                    .records
                    .iter()
                    .find(|record| record.manifest.plugin_id == plugin_id);
                PluginStatus {
                    configured: self
                        .config
                        .plugins
                        .configured
                        .iter()
                        .any(|selection| selection.enabled && selection.id.trim() == plugin_id),
                    active_deployment: active
                        .map(|record| deployment_name(&deployment_for(record))),
                    preferred_deployment: state.plugins.get(&plugin_id).map(deployment_name),
                    candidates: catalog
                        .candidates
                        .iter()
                        .filter(|record| record.manifest.plugin_id == plugin_id)
                        .map(|record| PluginCandidateStatus {
                            deployment: deployment_name(&deployment_for(record)),
                            version: record.manifest.version.clone(),
                            api_version: record.manifest.api_version.clone(),
                            sha256: record.manifest.artifact.sha256.clone(),
                            path: record.manifest_path.display().to_string(),
                            available: true,
                            runner_link: record
                                .runtime
                                .as_ref()
                                .map(|runtime| runtime.runner_link.clone()),
                        })
                        .collect(),
                    plugin_id,
                }
            })
            .collect();
        let diagnostics = catalog
            .diagnostics
            .iter()
            .map(|item| PluginInventoryDiagnostic {
                manifest_path: item.manifest_path.display().to_string(),
                plugin_id: item.plugin_id.clone(),
                deployment: item.deployment.as_ref().map(deployment_name),
                detail: item.detail.clone(),
            })
            .collect();
        ControlResponse::ok(PluginListResponse {
            plugins,
            diagnostics,
        })
    }

    async fn plugin_reload(&self) -> ControlResponse {
        let state = self
            .deployment_state
            .lock()
            .expect("deployment state mutex")
            .clone();
        let new_catalog =
            match load_catalog_with_state(&self.config, &self.builtin_registry, &state) {
                Ok(catalog) => catalog,
                Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
            };
        self.reload_catalog(new_catalog).await
    }

    async fn plugin_deployment_set(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<PluginDeploymentParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        self.change_plugin_deployment(param.plugin_id, Some(param.deployment))
            .await
    }

    async fn plugin_deployment_clear(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<PluginDeploymentClearParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        self.change_plugin_deployment(param.plugin_id, None).await
    }

    async fn change_plugin_deployment(
        &self,
        plugin_id: String,
        deployment: Option<PluginDeploymentKind>,
    ) -> ControlResponse {
        if !self
            .config
            .plugins
            .configured
            .iter()
            .any(|selection| selection.enabled && selection.id.trim() == plugin_id.trim())
        {
            return ControlResponse::err(ControlError::BadRequest(format!(
                "plugin {plugin_id} is not enabled by plugins.configured"
            )));
        }
        let previous = self
            .deployment_state
            .lock()
            .expect("deployment state mutex")
            .clone();
        let mut next = previous.clone();
        match deployment {
            Some(deployment) => {
                next.plugins.insert(plugin_id.clone(), deployment);
            }
            None => {
                next.plugins.remove(&plugin_id);
            }
        }
        let new_catalog = match load_catalog_with_state(&self.config, &self.builtin_registry, &next)
        {
            Ok(catalog) => catalog,
            Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
        };
        if let Err(error) = save_deployment_state(&self.config, &next) {
            return ControlResponse::err(ControlError::Failed(error.to_string()));
        }
        let response = self.reload_catalog(new_catalog).await;
        if response.ok {
            *self
                .deployment_state
                .lock()
                .expect("deployment state mutex") = next;
        } else if let Err(error) = save_deployment_state(&self.config, &previous) {
            return ControlResponse::err(ControlError::Failed(format!(
                "deployment reload failed and restoring management state failed: {error}"
            )));
        }
        response
    }

    async fn reload_catalog(&self, new_catalog: PluginCatalog) -> ControlResponse {
        let previous_generation = {
            let guard = self.host_runtime.lock().expect("host runtime mutex");
            let Some(runtime) = guard.as_ref() else {
                return ControlResponse::err(ControlError::Failed("core is not running".into()));
            };
            runtime.host_context().registry_generation()
        };
        let registry_generation = previous_generation.saturating_add(1);
        let (prepared, runtime_lock) = match runtime_bootstrapper(
            &self.config,
            &new_catalog,
            &self.native_runner_factories,
            &self.loaded_plugin_factories,
            self.runtime_client.clone(),
        )
        .and_then(|(bootstrapper, profile)| {
            let mut lock = resolve_load_plan(
                &new_catalog
                    .records
                    .iter()
                    .map(|record| record.manifest.clone())
                    .collect::<Vec<_>>(),
                &profile,
            )?;
            lock.registry_generation = registry_generation;
            Ok((
                bootstrapper.prepare_reload(profile, registry_generation)?,
                lock,
            ))
        }) {
            Ok(reload) => reload,
            Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
        };
        let drain_timeout = reload_drain_timeout(&self.config, &new_catalog);
        let plugin_count = new_catalog.records.len();
        let sidecars = sidecar_specs(&self.config, &new_catalog);
        let runtime_lock_path = self.config.service.run_dir.join("runtime.lock.json");
        let previous_runtime_lock = fs::read(&runtime_lock_path).ok();
        if let Err(error) = write_runtime_lock(&self.config, &runtime_lock) {
            return ControlResponse::err(ControlError::Failed(error.to_string()));
        }
        let decision = {
            let mut guard = self.host_runtime.lock().expect("host runtime mutex");
            let Some(runtime) = guard.as_mut() else {
                return ControlResponse::err(ControlError::Failed("core is not running".into()));
            };
            match runtime.reload(prepared, drain_timeout) {
                Ok(decision) => decision,
                Err(error) => {
                    let restore = restore_runtime_lock(&runtime_lock_path, previous_runtime_lock);
                    return ControlResponse::err(ControlError::Failed(match restore {
                        Ok(()) => error.to_string(),
                        Err(restore_error) => {
                            format!("{error}; restoring runtime lock failed: {restore_error}")
                        }
                    }));
                }
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

    fn core_begin_drain(&self) -> ControlResponse {
        let guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_ref() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        match runtime.begin_drain() {
            Ok(state) => ControlResponse::ok(CoreDrainResponse {
                state: runtime_stop_state(state).to_owned(),
            }),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
        }
    }

    fn task_submit_batch(&self, params: Value) -> ControlResponse {
        let param = match serde_json::from_value::<TaskSubmitBatchParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        if param.batch.tasks.is_empty() {
            return ControlResponse::err(ControlError::BadRequest(
                "task batch must contain at least one task".into(),
            ));
        }
        let guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_ref() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        match runtime.dispatch(HostRuntimeCommand::SubmitBatch(Box::new(param.batch))) {
            Ok(HostRuntimeReply::TaskBatchSubmitted(handles)) => {
                ControlResponse::ok(TaskSubmitBatchResponse { handles })
            }
            Ok(other) => ControlResponse::err(ControlError::Failed(format!(
                "unexpected task submit reply: {other:?}"
            ))),
            Err(error) => ControlResponse::err(ControlError::Failed(error.to_string())),
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

    fn task_events_after(&self, params: Value) -> ControlResponse {
        const MAX_EVENTS_PER_PAGE: usize = 1_024;

        let param = match serde_json::from_value::<TaskEventsAfterParam>(params) {
            Ok(param) => param,
            Err(error) => return ControlResponse::err(ControlError::BadRequest(error.to_string())),
        };
        if param.limit == 0 || param.limit > MAX_EVENTS_PER_PAGE {
            return ControlResponse::err(ControlError::BadRequest(format!(
                "event page limit must be in 1..={MAX_EVENTS_PER_PAGE}"
            )));
        }
        let guard = self.host_runtime.lock().expect("host runtime mutex");
        let Some(runtime) = guard.as_ref() else {
            return ControlResponse::err(ControlError::Failed("core is not running".into()));
        };
        let mut events = match runtime.events_after(param.sequence) {
            Ok(events) => events,
            Err(error) => return ControlResponse::err(ControlError::Failed(error.to_string())),
        };
        let has_more = events.len() > param.limit;
        events.truncate(param.limit);
        let next_sequence = events.last().map_or(param.sequence, |event| event.sequence);
        ControlResponse::ok(TaskEventPage {
            next_sequence,
            has_more,
            events,
        })
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
        let components = self
            .health_probes
            .iter()
            .map(|(id, probe)| (id.clone(), probe()))
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
            components,
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

#[cfg(test)]
fn load_catalog(
    config: &ServiceConfig,
    builtin_registry: &BuiltinRegistry,
) -> ServiceRuntimeResult<PluginCatalog> {
    let preferences = load_deployment_state(config)?;
    load_catalog_with_state(config, builtin_registry, &preferences)
}

fn load_catalog_with_state(
    config: &ServiceConfig,
    builtin_registry: &BuiltinRegistry,
    preferences: &PluginDeploymentState,
) -> ServiceRuntimeResult<PluginCatalog> {
    let builtin = builtin_registry.load_all()?;
    let inventory = PluginInventory::scan(
        &config.plugins.dynamic_dirs,
        &config.plugins.disabled_dir,
        builtin,
    )?;
    resolve_catalog(config, inventory, preferences)
}

fn resolve_catalog(
    config: &ServiceConfig,
    inventory: PluginInventory,
    preferences: &PluginDeploymentState,
) -> ServiceRuntimeResult<PluginCatalog> {
    let mut selected = Vec::new();
    for selection in config
        .plugins
        .configured
        .iter()
        .filter(|selection| selection.enabled)
    {
        let plugin_id = selection.id.trim();
        let candidates = inventory
            .records
            .iter()
            .filter(|record| record.manifest.plugin_id == plugin_id)
            .collect::<Vec<_>>();
        let diagnostic = || {
            let details = inventory
                .diagnostics
                .iter()
                .filter(|item| item.plugin_id.as_deref() == Some(plugin_id))
                .map(|item| item.detail.as_str())
                .collect::<Vec<_>>()
                .join("; ");
            if details.is_empty() {
                "plugin is not linked or installed".into()
            } else {
                details
            }
        };
        if candidates.is_empty() {
            return Err(ServiceRuntimeError::PluginUnavailable {
                plugin_id: plugin_id.into(),
                detail: diagnostic(),
            });
        }
        let chosen = if let Some(preferred) = preferences.plugins.get(plugin_id) {
            candidates
                .iter()
                .copied()
                .find(|record| deployment_for(record) == *preferred)
                .ok_or_else(|| ServiceRuntimeError::PluginDeploymentUnavailable {
                    plugin_id: plugin_id.into(),
                    deployment: preferred.clone(),
                    detail: diagnostic(),
                })?
        } else if candidates.len() == 1 {
            candidates[0]
        } else {
            let builtin = candidates
                .iter()
                .copied()
                .find(|record| deployment_for(record) == PluginDeploymentKind::Builtin);
            let builtin_abi_only = candidates.iter().all(|record| {
                matches!(
                    deployment_for(record),
                    PluginDeploymentKind::Builtin | PluginDeploymentKind::Abi
                )
            });
            if builtin_abi_only {
                builtin.ok_or_else(|| ServiceRuntimeError::PluginDeploymentAmbiguous {
                    plugin_id: plugin_id.into(),
                })?
            } else {
                return Err(ServiceRuntimeError::PluginDeploymentAmbiguous {
                    plugin_id: plugin_id.into(),
                });
            }
        };
        if deployment_for(chosen) != PluginDeploymentKind::Builtin {
            if let Some(builtin) = candidates
                .iter()
                .copied()
                .find(|record| deployment_for(record) == PluginDeploymentKind::Builtin)
            {
                if builtin.manifest.business_surface() != chosen.manifest.business_surface() {
                    return Err(ServiceRuntimeError::PluginBusinessSurfaceMismatch {
                        plugin_id: plugin_id.into(),
                    });
                }
            }
        }
        selected.push(chosen.clone());
    }
    Ok(PluginCatalog::resolved(inventory, selected))
}

fn deployment_for(record: &PluginRecord) -> PluginDeploymentKind {
    PluginDeploymentKind::default_for_artifact(&record.manifest.artifact.artifact_type)
}

fn deployment_name(deployment: &PluginDeploymentKind) -> String {
    format!("{deployment:?}").to_ascii_lowercase()
}

fn deployment_state_path(config: &ServiceConfig) -> std::path::PathBuf {
    config.service.data_dir.join("plugin-deployments.json")
}

fn load_deployment_state(config: &ServiceConfig) -> ServiceRuntimeResult<PluginDeploymentState> {
    let path = deployment_state_path(config);
    if !path.is_file() {
        return Ok(PluginDeploymentState {
            version: deployment_state_version(),
            plugins: BTreeMap::new(),
        });
    }
    let bytes = fs::read(&path).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
        path: path.display().to_string(),
        detail: error.to_string(),
    })?;
    let state: PluginDeploymentState = serde_json::from_slice(&bytes).map_err(|error| {
        ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
    })?;
    if state.version != deployment_state_version() {
        return Err(ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: format!("unsupported state version {}", state.version),
        });
    }
    Ok(state)
}

fn save_deployment_state(
    config: &ServiceConfig,
    state: &PluginDeploymentState,
) -> ServiceRuntimeResult<()> {
    let path = deployment_state_path(config);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        })?;
    }
    let bytes = serde_json::to_vec_pretty(state).map_err(|error| {
        ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
    })?;
    let temporary = path.with_extension("json.pending");
    fs::write(&temporary, bytes).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
        path: temporary.display().to_string(),
        detail: error.to_string(),
    })?;
    if path.exists() {
        fs::remove_file(&path).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        })?;
    }
    fs::rename(&temporary, &path).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
        path: path.display().to_string(),
        detail: error.to_string(),
    })
}

fn builtin_registry() -> BuiltinRegistry {
    BuiltinRegistry::new()
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
    loaded_plugin_factories: &BTreeMap<String, LoadedPluginFactory>,
    runtime_client: Arc<DeferredRuntimeClient>,
) -> ServiceRuntimeResult<HostRuntime> {
    let (bootstrapper, profile) = runtime_bootstrapper(
        config,
        catalog,
        native_runner_factories,
        loaded_plugin_factories,
        runtime_client,
    )?;
    let host_config = HostRuntimeConfig {
        worker_threads: config.core.worker_threads,
        blocking_threads: config.core.blocking_threads,
        ..HostRuntimeConfig::default()
    };
    let lock = resolve_load_plan(
        &catalog
            .records
            .iter()
            .map(|record| record.manifest.clone())
            .collect::<Vec<_>>(),
        &profile,
    )?;
    let runtime = bootstrapper.into_host_runtime_with_config(profile, host_config)?;
    write_runtime_lock(config, &lock)?;
    Ok(runtime)
}

fn write_runtime_lock(
    config: &ServiceConfig,
    lock: &mutsuki_runtime_contracts::RuntimeLock,
) -> ServiceRuntimeResult<()> {
    let path = config.service.run_dir.join("runtime.lock.json");
    fs::create_dir_all(&config.service.run_dir).map_err(|error| {
        ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
    })?;
    let bytes = serde_json::to_vec_pretty(lock).map_err(|error| {
        ServiceRuntimeError::PluginDeploymentState {
            path: path.display().to_string(),
            detail: error.to_string(),
        }
    })?;
    fs::write(&path, bytes).map_err(|error| ServiceRuntimeError::PluginDeploymentState {
        path: path.display().to_string(),
        detail: error.to_string(),
    })
}

fn restore_runtime_lock(path: &std::path::Path, previous: Option<Vec<u8>>) -> std::io::Result<()> {
    match previous {
        Some(bytes) => fs::write(path, bytes),
        None if path.exists() => fs::remove_file(path),
        None => Ok(()),
    }
}

fn runtime_bootstrapper(
    config: &ServiceConfig,
    catalog: &PluginCatalog,
    native_runner_factories: &[NativeRunnerFactory],
    loaded_plugin_factories: &BTreeMap<String, LoadedPluginFactory>,
    runtime_client: Arc<DeferredRuntimeClient>,
) -> ServiceRuntimeResult<(RuntimeBootstrapper, RuntimeProfile)> {
    let mut bootstrapper = RuntimeBootstrapper::new();
    let mut enabled_plugins = Vec::new();
    let mut deployments = BTreeMap::new();

    for record in &catalog.records {
        if !is_bootable_record(record) {
            continue;
        }

        let deployment =
            PluginDeploymentKind::default_for_artifact(&record.manifest.artifact.artifact_type);
        deployments.insert(record.manifest.plugin_id.clone(), deployment.clone());
        enabled_plugins.push(record.manifest.plugin_id.clone());
        if matches!(
            record.manifest.artifact.artifact_type,
            mutsuki_runtime_contracts::ArtifactType::Abi
        ) {
            bootstrapper.register_loaded_plugin(abi_plugin::load_abi_plugin(
                record,
                config,
                runtime_client.clone(),
                configured_plugin_config(config, &record.manifest.plugin_id),
            )?);
        } else if let Some(factory) = loaded_plugin_factories.get(&record.manifest.plugin_id) {
            let plugin = factory().map_err(ServiceRuntimeError::NativeRunnerFactory)?;
            if plugin.manifest.business_surface() != record.manifest.business_surface() {
                return Err(ServiceRuntimeError::PluginBusinessSurfaceMismatch {
                    plugin_id: record.manifest.plugin_id.clone(),
                });
            }
            bootstrapper.register_loaded_plugin(plugin);
        } else {
            bootstrapper.register_manifest(record.manifest.clone());
        }

        if let Some(runtime) = &record.runtime {
            register_stdio_runners(config, &mut bootstrapper, record, runtime, deployment)?;
        }
    }

    for factory in native_runner_factories {
        let runner = factory().map_err(ServiceRuntimeError::NativeRunnerFactory)?;
        let plugin_id = &runner.descriptor().plugin_id;
        if deployments.get(plugin_id) == Some(&PluginDeploymentKind::Builtin) {
            bootstrapper.register_builtin_runner(runner);
        }
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

fn configured_plugin_config(config: &ServiceConfig, plugin_id: &str) -> Value {
    config
        .plugins
        .configured
        .iter()
        .find(|selection| selection.enabled && selection.id.trim() == plugin_id)
        .map(|selection| selection.config.clone())
        .unwrap_or(Value::Null)
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
        let mut extra_env = runtime.env.clone();
        extra_env.insert(
            "MUTSUKI_HOME".into(),
            config.service.home_dir.to_string_lossy().into_owned(),
        );
        extra_env.insert("MUTSUKI_RUNNER_ID".into(), descriptor.runner_id.clone());
        extra_env.insert("MUTSUKI_PLUGIN_ID".into(), descriptor.plugin_id.clone());
        let spec = ProcessRunnerSpec {
            command: runtime.command.clone().into(),
            args: runtime.args.clone(),
            cwd: runtime.cwd.clone(),
            env: filtered_environment(&config.runners.env_allowlist, extra_env),
        };
        let mut runner =
            SpawnedJsonlRunner::spawn(descriptor.clone(), &spec).map_err(|source| {
                ServiceRuntimeError::ExternalRunnerSpawn {
                    runner_id: descriptor.runner_id.clone(),
                    detail: source.to_string(),
                }
            })?;
        if let Some(stderr) = runner.take_stderr() {
            let runner_id = descriptor.runner_id.clone();
            std::thread::spawn(move || drain_blocking_stderr(runner_id, stderr));
        }
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
            evidence: BTreeMap::new(),
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
            evidence: BTreeMap::new(),
        },
        Some(TaskOutcome::Failed { task_id, error }) => {
            let evidence = error
                .evidence
                .into_iter()
                .filter_map(|(key, value)| {
                    serde_json::to_value(value).ok().map(|value| (key, value))
                })
                .collect();
            TaskOutcomeView {
                task_id,
                status: "failed".into(),
                output_ref: None,
                reason: Some(error.route),
                error_code: Some(error.code),
                evidence,
            }
        }
        Some(TaskOutcome::Cancelled { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "cancelled".into(),
            output_ref: None,
            reason,
            error_code: None,
            evidence: BTreeMap::new(),
        },
        Some(TaskOutcome::Expired { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "expired".into(),
            output_ref: None,
            reason,
            error_code: None,
            evidence: BTreeMap::new(),
        },
        Some(TaskOutcome::DeadLetter { task_id, reason }) => TaskOutcomeView {
            task_id,
            status: "dead_letter".into(),
            output_ref: None,
            reason,
            error_code: None,
            evidence: BTreeMap::new(),
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

fn runtime_stop_state(state: RuntimeStopState) -> &'static str {
    match state {
        RuntimeStopState::Running => "running",
        RuntimeStopState::Draining => "draining",
        RuntimeStopState::Aborted => "aborted",
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
        RunnerControlCapability, RunnerDescriptor, RunnerOrderingCapability,
        RunnerPayloadCapability, RunnerPurity, RunnerResourceCapability, Task, WorkBatch,
    };
    use mutsuki_runtime_sdk::map_work_batch_entries;
    use mutsuki_service_control::{PluginReloadResponse, TaskOutcomeView, TaskSnapshot};
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    const TEST_PLUGIN_ID: &str = "test.control.facade";

    fn test_builtin_registry() -> BuiltinRegistry {
        let mut registry = BuiltinRegistry::new();
        registry.register_manifest(minimal_manifest(TEST_PLUGIN_ID));
        registry
    }

    struct TestConfiguredFactory;

    struct SecondConfiguredFactory;

    impl ConfiguredPluginFactory for TestConfiguredFactory {
        fn plugin_id(&self) -> &str {
            "test.configured"
        }

        fn prepare(
            &self,
            config: &Value,
            builder: ServiceRuntimeBuilder,
        ) -> Result<ServiceRuntimeBuilder, String> {
            if config.get("mode").and_then(Value::as_str) != Some("enabled") {
                return Err("mode must be enabled".into());
            }
            Ok(builder.register_builtin_plugin(minimal_manifest(self.plugin_id())))
        }
    }

    impl ConfiguredPluginFactory for SecondConfiguredFactory {
        fn plugin_id(&self) -> &str {
            "test.configured.second"
        }

        fn prepare(
            &self,
            _config: &Value,
            builder: ServiceRuntimeBuilder,
        ) -> Result<ServiceRuntimeBuilder, String> {
            Ok(builder.register_builtin_plugin(minimal_manifest(self.plugin_id())))
        }
    }

    fn configured_selection(id: &str, config: Value) -> ConfiguredPluginSelection {
        ConfiguredPluginSelection {
            id: id.into(),
            enabled: true,
            config,
        }
    }

    #[test]
    fn configured_plugin_catalog_installs_before_boot_and_keeps_secret_references() {
        let mut config = ServiceConfig::default();
        config.plugins.configured = vec![configured_selection(
            "test.configured",
            json!({"mode": "enabled", "client_secret_key": "TEST_SECRET"}),
        )];
        let mut catalog = ConfiguredPluginCatalog::new();
        catalog.register(TestConfiguredFactory).unwrap();

        let builder = ServiceRuntimeBuilder::new(config)
            .with_configured_plugin_catalog(catalog)
            .install_configured_plugins()
            .unwrap();

        assert_eq!(
            builder.builtin_registry.load_all().unwrap().records.len(),
            1
        );
    }

    #[test]
    fn configured_plugin_catalog_rejects_duplicate_and_raw_credentials() {
        let cases = [
            (
                vec![
                    configured_selection("test.configured", json!({"mode": "enabled"})),
                    configured_selection("test.configured", json!({"mode": "enabled"})),
                ],
                "selected more than once",
            ),
            (
                vec![configured_selection(
                    "test.configured",
                    json!({"mode": "enabled", "client_secret": "raw"}),
                )],
                "contains raw credential field client_secret",
            ),
        ];
        for (configured, expected) in cases {
            let mut config = ServiceConfig::default();
            config.plugins.configured = configured;
            let mut catalog = ConfiguredPluginCatalog::new();
            catalog.register(TestConfiguredFactory).unwrap();
            let error = ServiceRuntimeBuilder::new(config)
                .with_configured_plugin_catalog(catalog)
                .install_configured_plugins()
                .err()
                .expect("configuration must fail");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn configured_plugin_catalog_merge_is_atomic_and_rejects_duplicates() {
        let mut first = ConfiguredPluginCatalog::new();
        first.register(TestConfiguredFactory).unwrap();
        let mut second = ConfiguredPluginCatalog::new();
        second.register(SecondConfiguredFactory).unwrap();
        first.merge(second).unwrap();
        assert!(first.factory("test.configured").is_some());
        assert!(first.factory("test.configured.second").is_some());

        let mut duplicate = ConfiguredPluginCatalog::new();
        duplicate.register(TestConfiguredFactory).unwrap();
        let error = first.merge(duplicate).unwrap_err();
        assert!(matches!(
            error,
            ServiceRuntimeError::DuplicateConfiguredPlugin(plugin_id)
                if plugin_id == "test.configured"
        ));
        assert!(first.factory("test.configured.second").is_some());
    }

    #[test]
    fn deployment_resolver_defaults_to_builtin_and_honors_host_preference() {
        let mut config = ServiceConfig::default();
        config.plugins.configured = vec![configured_selection("test.choice", Value::Null)];
        let builtin_manifest = minimal_manifest("test.choice");
        let mut abi_manifest = builtin_manifest.clone();
        abi_manifest.artifact.artifact_type = ArtifactType::Abi;
        abi_manifest.artifact.path = "test_choice.dll".into();
        abi_manifest.artifact.sha256 = format!("sha256:{}", "1".repeat(64));
        let inventory = PluginInventory {
            records: vec![
                PluginRecord {
                    manifest_path: "<builtin>".into(),
                    manifest: builtin_manifest,
                    runtime: None,
                    resolved_artifact: None,
                },
                PluginRecord {
                    manifest_path: "plugin.toml".into(),
                    manifest: abi_manifest,
                    runtime: None,
                    resolved_artifact: Some("test_choice.dll".into()),
                },
            ],
            diagnostics: Vec::new(),
        };
        let default = resolve_catalog(
            &config,
            inventory.clone(),
            &PluginDeploymentState::default(),
        )
        .unwrap();
        assert_eq!(
            deployment_for(&default.records[0]),
            PluginDeploymentKind::Builtin
        );

        let preferred = resolve_catalog(
            &config,
            inventory,
            &PluginDeploymentState {
                version: deployment_state_version(),
                plugins: [("test.choice".into(), PluginDeploymentKind::Abi)].into(),
            },
        )
        .unwrap();
        assert_eq!(
            deployment_for(&preferred.records[0]),
            PluginDeploymentKind::Abi
        );
    }

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

    #[tokio::test]
    async fn distributed_neutral_control_submits_observes_and_drains_core() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        let submit = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskSubmitBatch,
                params: serde_json::to_value(TaskSubmitBatchParam {
                    batch: TaskBatch::one(
                        "control-batch-1",
                        Task::new("submitted-task-1", "control.input", json!({ "value": 1 })),
                    ),
                })
                .expect("submit params"),
            })
            .await;
        assert!(submit.ok);
        let submitted: TaskSubmitBatchResponse =
            serde_json::from_value(submit.result.expect("submit result")).expect("handles");
        assert_eq!(submitted.handles.len(), 1);
        assert_eq!(submitted.handles[0].task_id, "submitted-task-1");

        let events = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskEventsAfter,
                params: json!({ "sequence": 0, "limit": 16 }),
            })
            .await;
        assert!(events.ok);
        let page: TaskEventPage =
            serde_json::from_value(events.result.expect("event result")).expect("event page");
        assert!(!page.events.is_empty());
        assert!(page.next_sequence > 0);
        assert!(page.events.iter().any(|event| {
            event.kind == mutsuki_runtime_contracts::RuntimeEventKind::Task
                && event.subject_id.as_deref() == Some("submitted-task-1")
        }));

        let invalid_events = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskEventsAfter,
                params: json!({ "sequence": 0, "limit": 0 }),
            })
            .await;
        assert!(!invalid_events.ok);
        assert_eq!(
            invalid_events.error.expect("invalid event error").code,
            "bad_request"
        );

        let drain = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::CoreBeginDrain,
                params: Value::Null,
            })
            .await;
        assert!(drain.ok);
        let drain: CoreDrainResponse =
            serde_json::from_value(drain.result.expect("drain result")).expect("drain state");
        assert_eq!(drain.state, "draining");

        let rejected = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::TaskSubmitBatch,
                params: serde_json::to_value(TaskSubmitBatchParam {
                    batch: TaskBatch::one(
                        "control-batch-2",
                        Task::new("submitted-task-2", "control.input", json!({})),
                    ),
                })
                .expect("submit params"),
            })
            .await;
        assert!(!rejected.ok);
        assert_eq!(rejected.error.expect("drain rejection").code, "failed");
    }

    #[test]
    fn task_outcome_preserves_structured_failure_evidence() {
        let mut error = mutsuki_runtime_contracts::RuntimeError::new(
            mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
            "plugin.test",
            "test.route",
        );
        error.evidence.insert(
            "message".into(),
            mutsuki_runtime_contracts::ScalarValue::String("redacted detail".into()),
        );

        let view = to_control_task_outcome(
            "failed-task",
            Some(TaskOutcome::Failed {
                task_id: "failed-task".into(),
                error,
            }),
        );

        assert_eq!(view.status, "failed");
        assert_eq!(view.reason.as_deref(), Some("test.route"));
        assert_eq!(view.evidence["message"], json!("redacted detail"));
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
    async fn deployment_management_persists_only_available_configured_choice() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        let unavailable = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginDeploymentSet,
                params: json!({ "plugin_id": TEST_PLUGIN_ID, "deployment": "abi" }),
            })
            .await;
        assert!(!unavailable.ok);
        assert!(!deployment_state_path(&inner.config).exists());

        let selected = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginDeploymentSet,
                params: json!({ "plugin_id": TEST_PLUGIN_ID, "deployment": "builtin" }),
            })
            .await;
        assert!(selected.ok);
        let state = load_deployment_state(&inner.config).unwrap();
        assert_eq!(
            state.plugins.get(TEST_PLUGIN_ID),
            Some(&PluginDeploymentKind::Builtin)
        );

        let cleared = inner
            .handle_request(ControlRequest {
                token: "token".into(),
                method: ControlMethod::PluginDeploymentClear,
                params: json!({ "plugin_id": TEST_PLUGIN_ID }),
            })
            .await;
        assert!(cleared.ok);
        assert!(
            load_deployment_state(&inner.config)
                .unwrap()
                .plugins
                .is_empty()
        );
    }

    #[tokio::test]
    async fn plugin_reload_keeps_unselected_invalid_artifact_as_diagnostic() {
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
        assert!(response.ok);

        let status = inner.core_status();
        let status: CoreStatus =
            serde_json::from_value(status.result.expect("status")).expect("core status");
        assert_eq!(status.registry_generation, Some(2));
        let plugins = inner.plugin_list();
        let plugins: PluginListResponse =
            serde_json::from_value(plugins.result.expect("plugins")).expect("plugin list");
        assert_eq!(plugins.plugins.len(), 1);
        assert_eq!(plugins.plugins[0].plugin_id, TEST_PLUGIN_ID);
        assert_eq!(plugins.diagnostics.len(), 1);
    }

    #[tokio::test]
    async fn plugin_list_reflects_catalog_after_successful_reload() {
        let dir = tempdir().expect("temp dir");
        let inner = test_started_runtime_inner("token", dir.path());
        let plugin_dir = dir.path().join("installed").join("dynamic");
        std::fs::create_dir_all(&plugin_dir).expect("plugin dir");
        let mut manifest = minimal_manifest("mutsuki.dynamic.test");
        manifest.artifact.artifact_type = ArtifactType::Process;
        manifest.artifact.path = "dynamic-test".into();
        manifest.artifact.sha256 = "sha256:dynamic-test".into();
        let plugin = mutsuki_service_plugin_loader::PluginToml {
            manifest,
            runtime: None,
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
        assert_eq!(reload.plugin_count, 1);

        let plugins = inner.plugin_list();
        let plugins: PluginListResponse =
            serde_json::from_value(plugins.result.expect("plugins")).expect("plugin list");
        assert!(
            plugins
                .plugins
                .iter()
                .any(|plugin| plugin.plugin_id == "mutsuki.dynamic.test")
        );
    }

    #[test]
    fn event_source_required_secret_is_validated_before_runtime_start() {
        let secret_key = format!("MISSING_EVENT_SOURCE_SECRET_{}", std::process::id());
        let sources: Vec<Box<dyn HostEventSource>> = vec![Box::new(FailingEventSource {
            descriptor: HostEventSourceDescriptor::new("secret-source", "test.source")
                .require_secret(secret_key.clone()),
        })];

        let error = validate_event_sources(&sources, &ServiceConfig::default())
            .expect_err("missing required Host secret must fail preflight");

        assert!(matches!(
            error,
            ServiceRuntimeError::EventSource(message)
                if message.contains("secret-source") && message.contains(&secret_key)
        ));
    }

    #[tokio::test]
    async fn fallible_native_runner_factory_fails_without_panicking() {
        let dir = tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("logs")).expect("logs");
        let mut config = ServiceConfig::default();
        config.ipc.enabled = false;
        config.observe.console = false;
        config.service.log_dir = dir.path().join("logs");
        config.plugins.dynamic_dirs.clear();

        let result = ServiceRuntimeBuilder::new(config)
            .register_fallible_builtin_runner(|| -> Result<Box<dyn Runner>, &'static str> {
                Err("client init rejected")
            })
            .start()
            .await;

        assert!(matches!(
            result,
            Err(ServiceRuntimeError::NativeRunnerFactory(message))
                if message == "client init rejected"
        ));
    }

    #[tokio::test]
    async fn product_health_probe_is_exposed_without_domain_coupling() {
        let dir = tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join("logs")).expect("logs");
        let mut config = ServiceConfig::default();
        config.ipc.enabled = false;
        config.observe.console = false;
        config.service.home_dir = dir.path().to_path_buf();
        config.service.data_dir = dir.path().join("data");
        config.service.log_dir = dir.path().join("logs");
        config.service.run_dir = dir.path().join("run");
        config.plugins.dynamic_dirs.clear();
        config.plugins.disabled_dir = dir.path().join("disabled");

        let runtime = ServiceRuntimeBuilder::new(config)
            .register_health_probe(
                "test.component",
                || serde_json::json!({"status": "ok", "ready": true}),
            )
            .start()
            .await
            .expect("runtime starts");
        let response = runtime.inner.health_check().await;
        let report: HealthReport = serde_json::from_value(response.result.unwrap()).unwrap();
        assert_eq!(report.components["test.component"]["status"], "ok");
        assert_eq!(report.components["test.component"]["ready"], true);
        runtime.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn product_builder_event_source_runs_real_three_stage_pipeline() {
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
        config.plugins.dynamic_dirs.clear();
        config.plugins.disabled_dir = dir.path().join("disabled");
        config.plugins.configured = [
            "test.stage.first",
            "test.stage.second",
            "test.stage.terminal",
        ]
        .into_iter()
        .map(|id| configured_selection(id, Value::Null))
        .collect();
        config.runners.graceful_shutdown_ms = 250;

        let terminal_count = Arc::new(AtomicUsize::new(0));
        let source_starts = Arc::new(AtomicUsize::new(0));
        let source_stops = Arc::new(AtomicUsize::new(0));
        let client_factory_count = Arc::new(AtomicUsize::new(0));
        let client_checks = Arc::new(AtomicUsize::new(0));
        let source = MockEventSource {
            descriptor: HostEventSourceDescriptor::new("mock-source", "test.source"),
            starts: source_starts.clone(),
            stops: source_stops.clone(),
        };
        let first_descriptor =
            chain_descriptor("test.stage.first", "test.stage.first.runner", "test.input");
        let second_descriptor = chain_descriptor(
            "test.stage.second",
            "test.stage.second.runner",
            "test.intermediate",
        );
        let terminal_descriptor = chain_descriptor(
            "test.stage.terminal",
            "test.stage.terminal.runner",
            "test.output",
        );

        let first_factory_descriptor = first_descriptor.clone();
        let second_factory_descriptor = second_descriptor.clone();
        let second_factory_count = client_factory_count.clone();
        let second_client_checks = client_checks.clone();
        let terminal_factory_descriptor = terminal_descriptor.clone();
        let terminal_factory_count = terminal_count.clone();
        let runtime = ServiceRuntimeBuilder::new(config)
            .register_builtin_plugin(runner_manifest("test.stage.first", first_descriptor))
            .register_builtin_plugin(runner_manifest("test.stage.second", second_descriptor))
            .register_builtin_plugin(runner_manifest("test.stage.terminal", terminal_descriptor))
            .register_builtin_runner(move || {
                Box::new(ChainRunner::next(
                    first_factory_descriptor.clone(),
                    "test.intermediate",
                ))
            })
            .register_runtime_client_runner(move |client| {
                second_factory_count.fetch_add(1, Ordering::SeqCst);
                Box::new(ChainRunner::next_with_client(
                    second_factory_descriptor.clone(),
                    "test.output",
                    client,
                    second_client_checks.clone(),
                ))
            })
            .register_builtin_runner(move || {
                Box::new(ChainRunner::terminal(
                    terminal_factory_descriptor.clone(),
                    terminal_factory_count.clone(),
                ))
            })
            .register_event_source(Box::new(source))
            .start()
            .await
            .expect("real service runtime starts");

        wait_for_count(&terminal_count, 1).await;
        assert_eq!(client_factory_count.load(Ordering::SeqCst), 1);
        assert_eq!(client_checks.load(Ordering::SeqCst), 1);
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
        assert_eq!(health.event_source_details[0].source_id, "mock-source");

        let reload = runtime.inner.plugin_reload().await;
        assert!(reload.ok);
        let reload: PluginReloadResponse =
            serde_json::from_value(reload.result.expect("reload")).expect("reload response");
        assert_eq!(reload.event_sources, "kept");
        assert_eq!(source_starts.load(Ordering::SeqCst), 1);
        assert_eq!(client_factory_count.load(Ordering::SeqCst), 2);

        let restart = runtime
            .inner
            .handle_request(ControlRequest {
                token: "test-token".into(),
                method: ControlMethod::EventSourceRestart,
                params: json!({ "id": "mock-source" }),
            })
            .await;
        assert!(restart.ok);
        wait_for_count(&terminal_count, 2).await;
        assert_eq!(client_checks.load(Ordering::SeqCst), 2);
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
        config.service.home_dir = dir.path().to_path_buf();
        config.service.data_dir = dir.path().join("data");
        config.service.log_dir = dir.path().join("logs");
        config.service.run_dir = dir.path().join("run");
        config.plugins.dynamic_dirs.clear();
        config.plugins.disabled_dir = dir.path().join("disabled");
        config.runners.graceful_shutdown_ms = 50;
        let runtime = ServiceRuntimeBuilder::new(config)
            .register_event_source(Box::new(FailingEventSource {
                descriptor: HostEventSourceDescriptor::new("failed-source", "test.source"),
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
        assert!(health.recent_errors[0].contains("failed-source"));
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

    struct MockEventSource {
        descriptor: HostEventSourceDescriptor,
        starts: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
    }

    struct FailingEventSource {
        descriptor: HostEventSourceDescriptor,
    }

    impl HostEventSource for FailingEventSource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, _ctx: HostEventSourceContext) -> HostEventSourceFuture {
            Box::pin(async { Err(std::io::Error::other("upstream disconnected").into()) })
        }

        fn shutdown(&mut self) -> HostEventSourceFuture {
            Box::pin(async { Ok(()) })
        }

        fn health(&self) -> HostEventSourceHealth {
            HostEventSourceHealth::Unhealthy("upstream disconnected".into())
        }
    }

    impl HostEventSource for MockEventSource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, ctx: HostEventSourceContext) -> HostEventSourceFuture {
            let sequence = self.starts.fetch_add(1, Ordering::SeqCst) + 1;
            Box::pin(async move {
                let mut task = Task::new(
                    format!("mock-source-{sequence}"),
                    "test.input",
                    json!({ "value": "pipeline input" }),
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
        runtime_client: Option<RuntimeClientRef>,
        client_checks: Option<Arc<AtomicUsize>>,
    }

    impl ChainRunner {
        fn next(descriptor: RunnerDescriptor, next_protocol: &'static str) -> Self {
            Self {
                descriptor,
                next_protocol: Some(next_protocol),
                terminal_count: None,
                runtime_client: None,
                client_checks: None,
            }
        }

        fn next_with_client(
            descriptor: RunnerDescriptor,
            next_protocol: &'static str,
            runtime_client: RuntimeClientRef,
            client_checks: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                descriptor,
                next_protocol: Some(next_protocol),
                terminal_count: None,
                runtime_client: Some(runtime_client),
                client_checks: Some(client_checks),
            }
        }

        fn terminal(descriptor: RunnerDescriptor, count: Arc<AtomicUsize>) -> Self {
            Self {
                descriptor,
                next_protocol: None,
                terminal_count: Some(count),
                runtime_client: None,
                client_checks: None,
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
            let runtime_client = self.runtime_client.clone();
            let client_checks = self.client_checks.clone();
            map_work_batch_entries(&batch, |task| {
                if let Some(client) = &runtime_client {
                    let handle = TaskHandle {
                        task_id: task.task_id.clone(),
                        protocol_id: task.protocol_id.clone(),
                        target_binding_id: task.target_binding_id.clone(),
                        cancel_policy: CancelPolicy::Cascade,
                        trace_id: task.trace_id.clone(),
                        correlation_id: task.correlation_id.clone(),
                    };
                    let _ = client.task_outcome(&handle).map_err(|error| {
                        mutsuki_runtime_contracts::RuntimeError::new(
                            mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
                            "test.stage.second",
                            format!("runtime_client.outcome:{error}"),
                        )
                    })?;
                    if let Some(checks) = &client_checks {
                        checks.fetch_add(1, Ordering::SeqCst);
                    }
                }
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

    fn test_started_runtime_inner(token: &str, root: &Path) -> ServiceRuntimeInner {
        let mut config = ServiceConfig::default();
        config.ipc.token = Some(token.into());
        config.service.home_dir = root.to_path_buf();
        config.service.data_dir = root.join("data");
        config.service.log_dir = root.join("logs");
        config.service.run_dir = root.join("run");
        config.plugins.configured = vec![configured_selection(TEST_PLUGIN_ID, Value::Null)];
        config.plugins.dynamic_dirs = vec![root.join("installed")];
        config.plugins.disabled_dir = root.join("disabled");
        let registry = test_builtin_registry();
        let catalog = load_catalog(&config, &registry).expect("catalog");
        let runtime_client = Arc::new(DeferredRuntimeClient::default());
        let host_runtime = boot_core(
            &config,
            &catalog,
            &[],
            &BTreeMap::new(),
            runtime_client.clone(),
        )
        .expect("core");
        ServiceRuntimeInner {
            config,
            started_at: Instant::now(),
            catalog: Mutex::new(catalog),
            host_runtime: Mutex::new(Some(host_runtime)),
            supervisor: RunnerSupervisor::new(),
            event_sources: EventSourceSupervisor::default(),
            builtin_registry: registry,
            native_runner_factories: Vec::new(),
            loaded_plugin_factories: BTreeMap::new(),
            health_probes: BTreeMap::new(),
            runtime_client,
            deployment_state: Mutex::new(PluginDeploymentState {
                version: deployment_state_version(),
                plugins: BTreeMap::new(),
            }),
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
