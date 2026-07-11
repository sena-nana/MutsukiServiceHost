use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use mutsuki_runtime_contracts::{TaskBatch, TaskHandle, TaskOutcome};
use mutsuki_runtime_core::RuntimeResult;
use mutsuki_runtime_sdk::TaskSubmitter;
use mutsuki_service_config::ServiceConfig;
use mutsuki_service_control::EventSourceStatus;
use tokio::sync::{mpsc, watch};

pub type HostEventSourceError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type HostEventSourceFuture =
    Pin<Box<dyn Future<Output = Result<(), HostEventSourceError>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostEventSourceDescriptor {
    pub source_id: String,
    pub plugin_id: String,
    pub instance_id: String,
    /// Secret keys that must resolve through the Host secret backend before
    /// any runtime component is started. Values are never stored here.
    pub required_secrets: Vec<String>,
}

impl HostEventSourceDescriptor {
    pub fn new(source_id: impl Into<String>, plugin_id: impl Into<String>) -> Self {
        let source_id = source_id.into();
        Self {
            instance_id: source_id.clone(),
            source_id,
            plugin_id: plugin_id.into(),
            required_secrets: Vec::new(),
        }
    }

    pub fn with_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.instance_id = instance_id.into();
        self
    }

    pub fn require_secret(mut self, key: impl Into<String>) -> Self {
        self.required_secrets.push(key.into());
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostEventSourceHealth {
    Healthy,
    Degraded(String),
    Unhealthy(String),
}

impl HostEventSourceHealth {
    fn label(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded(_) => "degraded",
            Self::Unhealthy(_) => "unhealthy",
        }
    }
}

#[derive(Clone)]
pub struct HostEventSourceConfig {
    instance_id: String,
    profile: String,
    home_dir: String,
    data_dir: String,
    secret_env_prefix: String,
}

impl HostEventSourceConfig {
    pub(crate) fn from_service(config: &ServiceConfig) -> Self {
        Self {
            instance_id: config.service.instance_id.clone(),
            profile: config.service.profile.clone(),
            home_dir: config.service.home_dir.to_string_lossy().into_owned(),
            data_dir: config.service.data_dir.to_string_lossy().into_owned(),
            secret_env_prefix: config.security.secret_env_prefix.clone(),
        }
    }

    pub fn get(&self, scope: &str, key: &str) -> Option<&str> {
        match (scope, key) {
            ("service", "instance_id") => Some(&self.instance_id),
            ("service", "profile") => Some(&self.profile),
            ("service", "home_dir") => Some(&self.home_dir),
            ("service", "data_dir") => Some(&self.data_dir),
            _ => None,
        }
    }

    pub fn secret(&self, key: &str) -> Option<String> {
        let key = key
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect::<String>();
        std::env::var(format!("{}{key}", self.secret_env_prefix)).ok()
    }

    pub(crate) fn contains_secret(&self, key: &str) -> bool {
        self.secret(key).is_some_and(|value| !value.is_empty())
    }
}

#[derive(Clone)]
pub struct HostEventSourceLogger {
    source_id: String,
    plugin_id: String,
}

impl HostEventSourceLogger {
    pub fn log(&self, level: &str, message: &str, correlation_id: Option<&str>) {
        tracing::event!(
            tracing::Level::INFO,
            source_id = %self.source_id,
            plugin_id = %self.plugin_id,
            correlation_id = correlation_id.unwrap_or(""),
            event_level = level,
            message
        );
    }
}

#[derive(Clone)]
pub struct HostShutdownToken {
    rx: watch::Receiver<bool>,
}

impl HostShutdownToken {
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn cancelled(&mut self) {
        if !*self.rx.borrow() {
            let _ = self.rx.changed().await;
        }
    }
}

#[derive(Clone)]
pub struct HostEventSourceContext {
    pub task_submitter: Arc<dyn TaskSubmitter>,
    pub shutdown: HostShutdownToken,
    pub config: HostEventSourceConfig,
    pub events: HostEventSourceLogger,
    pub source_instance_id: String,
}

pub trait HostEventSource: Send + 'static {
    fn descriptor(&self) -> &HostEventSourceDescriptor;
    fn start(&mut self, ctx: HostEventSourceContext) -> HostEventSourceFuture;
    fn shutdown(&mut self) -> HostEventSourceFuture;
    fn health(&self) -> HostEventSourceHealth;
}

#[derive(Clone, Default)]
pub(crate) struct EventSourceSupervisor {
    sources: Arc<Mutex<BTreeMap<String, ManagedSource>>>,
}

struct ManagedSource {
    status: SourceStatus,
    commands: mpsc::Sender<SourceCommand>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
struct SourceStatus(Arc<Mutex<EventSourceStatus>>);

impl SourceStatus {
    fn new(descriptor: &HostEventSourceDescriptor) -> Self {
        Self(Arc::new(Mutex::new(EventSourceStatus {
            source_id: descriptor.source_id.clone(),
            plugin_id: descriptor.plugin_id.clone(),
            instance_id: descriptor.instance_id.clone(),
            state: "starting".into(),
            health: "unknown".into(),
            last_error: None,
            reconnects: 0,
            last_event_unix_ms: None,
        })))
    }

    fn snapshot(&self) -> EventSourceStatus {
        self.0.lock().expect("event source status mutex").clone()
    }

    fn set(&self, state: &str) {
        self.0.lock().expect("event source status mutex").state = state.into();
    }

    fn fail(&self, error: String) {
        let mut status = self.0.lock().expect("event source status mutex");
        status.state = "failed".into();
        status.last_error = Some(error);
    }

    fn update_health(&self, health: HostEventSourceHealth) {
        let mut status = self.0.lock().expect("event source status mutex");
        status.health = health.label().into();
        if let HostEventSourceHealth::Degraded(error) | HostEventSourceHealth::Unhealthy(error) =
            health
        {
            status.last_error = Some(error);
        }
    }

    fn reconnect(&self) {
        let mut status = self.0.lock().expect("event source status mutex");
        status.reconnects = status.reconnects.saturating_add(1);
        status.state = "restarting".into();
    }

    fn submitted(&self, correlation_ids: &[Option<String>]) {
        let mut status = self.0.lock().expect("event source status mutex");
        status.last_event_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis());
        for correlation_id in correlation_ids {
            tracing::info!(
                source_id = %status.source_id,
                plugin_id = %status.plugin_id,
                correlation_id = correlation_id.as_deref().unwrap_or(""),
                "event source submitted task"
            );
        }
    }
}

enum SourceCommand {
    Restart,
    Shutdown,
}

impl EventSourceSupervisor {
    pub(crate) fn start(
        &self,
        source: Box<dyn HostEventSource>,
        task_submitter: Arc<dyn TaskSubmitter>,
        config: &ServiceConfig,
        graceful: Duration,
    ) {
        let descriptor = source.descriptor().clone();
        let mut sources = self.sources.lock().expect("event source supervisor mutex");
        let status = SourceStatus::new(&descriptor);
        let (tx, rx) = mpsc::channel(4);
        let source_config = HostEventSourceConfig::from_service(config);
        let logger = HostEventSourceLogger {
            source_id: descriptor.source_id.clone(),
            plugin_id: descriptor.plugin_id.clone(),
        };
        let task = tokio::spawn(run_source_actor(
            source,
            task_submitter,
            source_config,
            logger,
            status.clone(),
            rx,
            graceful,
        ));
        sources.insert(
            descriptor.source_id,
            ManagedSource {
                status,
                commands: tx,
                task,
            },
        );
    }

    pub(crate) fn list(&self) -> Vec<EventSourceStatus> {
        self.sources
            .lock()
            .expect("event source supervisor mutex")
            .values()
            .map(|source| source.status.snapshot())
            .collect()
    }

    pub(crate) async fn restart(&self, source_id: &str) -> Result<(), String> {
        let sender = self
            .sources
            .lock()
            .expect("event source supervisor mutex")
            .get(source_id)
            .map(|source| source.commands.clone())
            .ok_or_else(|| format!("unknown event source {source_id}"))?;
        sender
            .send(SourceCommand::Restart)
            .await
            .map_err(|_| format!("event source {source_id} lifecycle task has stopped"))
    }

    pub(crate) async fn shutdown(&self, graceful: Duration) {
        let managed = self.take_sources();
        for source in &managed {
            let _ = source.commands.send(SourceCommand::Shutdown).await;
        }
        for source in managed {
            let mut task = source.task;
            if tokio::time::timeout(graceful + Duration::from_millis(100), &mut task)
                .await
                .is_err()
            {
                task.abort();
            }
        }
    }

    pub(crate) fn abort(&self) {
        for source in self.take_sources() {
            let _ = source.commands.try_send(SourceCommand::Shutdown);
            source.task.abort();
        }
    }

    fn take_sources(&self) -> Vec<ManagedSource> {
        let mut sources = self.sources.lock().expect("event source supervisor mutex");
        std::mem::take(&mut *sources).into_values().collect()
    }
}

async fn run_source_actor(
    mut source: Box<dyn HostEventSource>,
    task_submitter: Arc<dyn TaskSubmitter>,
    config: HostEventSourceConfig,
    events: HostEventSourceLogger,
    status: SourceStatus,
    mut commands: mpsc::Receiver<SourceCommand>,
    graceful: Duration,
) {
    loop {
        status.set("starting");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let tracked_submitter: Arc<dyn TaskSubmitter> = Arc::new(SourceTaskSubmitter {
            inner: task_submitter.clone(),
            status: status.clone(),
        });
        let ctx = HostEventSourceContext {
            task_submitter: tracked_submitter,
            shutdown: HostShutdownToken { rx: shutdown_rx },
            config: config.clone(),
            events: events.clone(),
            source_instance_id: status.snapshot().instance_id,
        };
        let start = catch_unwind(AssertUnwindSafe(|| source.start(ctx)));
        let mut running = match start {
            Ok(future) => future,
            Err(payload) => {
                status.fail(panic_message(payload));
                if !wait_command(&mut commands, &mut source, &status, &shutdown_tx, graceful).await
                {
                    return;
                }
                continue;
            }
        };
        status.update_health(safe_health(source.as_ref()));
        status.set("running");
        let mut health_tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                outcome = AssertUnwindSafe(&mut running).catch_unwind() => {
                    let message = match outcome {
                        Ok(Ok(())) => "event source exited unexpectedly".to_string(),
                        Ok(Err(error)) => error.to_string(),
                        Err(payload) => panic_message(payload),
                    };
                    status.fail(message);
                    break;
                }
                command = commands.recv() => {
                    if handle_command(command, &mut source, &status, &shutdown_tx, graceful).await {
                        break;
                    } else {
                        return;
                    }
                }
                _ = health_tick.tick() => status.update_health(safe_health(source.as_ref())),
            }
        }
        if status.snapshot().state == "failed"
            && !wait_command(&mut commands, &mut source, &status, &shutdown_tx, graceful).await
        {
            return;
        }
    }
}

async fn wait_command(
    commands: &mut mpsc::Receiver<SourceCommand>,
    source: &mut Box<dyn HostEventSource>,
    status: &SourceStatus,
    shutdown: &watch::Sender<bool>,
    graceful: Duration,
) -> bool {
    handle_command(commands.recv().await, source, status, shutdown, graceful).await
}

async fn handle_command(
    command: Option<SourceCommand>,
    source: &mut Box<dyn HostEventSource>,
    status: &SourceStatus,
    shutdown: &watch::Sender<bool>,
    graceful: Duration,
) -> bool {
    match command {
        Some(SourceCommand::Restart) => {
            status.reconnect();
            let _ = shutdown.send(true);
            stop_source(source, status, graceful, false).await;
            true
        }
        Some(SourceCommand::Shutdown) | None => {
            status.set("stopping");
            let _ = shutdown.send(true);
            stop_source(source, status, graceful, true).await;
            false
        }
    }
}

async fn stop_source(
    source: &mut Box<dyn HostEventSource>,
    status: &SourceStatus,
    graceful: Duration,
    terminal: bool,
) {
    let future = match catch_unwind(AssertUnwindSafe(|| source.shutdown())) {
        Ok(future) => future,
        Err(payload) => {
            status.fail(panic_message(payload));
            return;
        }
    };
    match tokio::time::timeout(graceful, AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(Ok(()))) => {
            if terminal {
                status.set("stopped");
            }
        }
        Ok(Ok(Err(error))) => {
            status.fail(error.to_string());
        }
        Ok(Err(payload)) => {
            status.fail(panic_message(payload));
        }
        Err(_) => {
            status.fail(format!(
                "shutdown timed out after {} ms",
                graceful.as_millis()
            ));
        }
    }
}

fn safe_health(source: &dyn HostEventSource) -> HostEventSourceHealth {
    catch_unwind(AssertUnwindSafe(|| source.health()))
        .unwrap_or_else(|payload| HostEventSourceHealth::Unhealthy(panic_message(payload)))
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("event source panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("event source panicked: {message}")
    } else {
        "event source panicked with a non-string payload".into()
    }
}

struct SourceTaskSubmitter {
    inner: Arc<dyn TaskSubmitter>,
    status: SourceStatus,
}

impl TaskSubmitter for SourceTaskSubmitter {
    fn submit_batch(&self, batch: TaskBatch) -> RuntimeResult<Vec<TaskHandle>> {
        let correlation_ids = batch
            .tasks
            .iter()
            .map(|task| task.correlation_id.clone())
            .collect::<Vec<_>>();
        let result = self.inner.submit_batch(batch);
        if result.is_ok() {
            self.status.submitted(&correlation_ids);
        }
        result
    }

    fn cancel_task(&self, handle: &TaskHandle) -> RuntimeResult<()> {
        self.inner.cancel_task(handle)
    }

    fn task_outcome(&self, handle: &TaskHandle) -> RuntimeResult<Option<TaskOutcome>> {
        self.inner.task_outcome(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn source_panic_is_isolated_and_explicit_restart_recovers() {
        let starts = Arc::new(AtomicUsize::new(0));
        let supervisor = EventSourceSupervisor::default();
        supervisor.start(
            Box::new(PanicOnceSource {
                descriptor: HostEventSourceDescriptor::new("panic-once", "test.plugin"),
                starts: starts.clone(),
            }),
            Arc::new(NoopSubmitter),
            &test_config(),
            Duration::from_millis(50),
        );

        wait_for_state(&supervisor, "failed").await;
        assert!(
            supervisor.list()[0]
                .last_error
                .as_deref()
                .expect("panic error")
                .contains("panicked")
        );
        supervisor.restart("panic-once").await.expect("restart");
        wait_for_state(&supervisor, "running").await;
        assert_eq!(supervisor.list()[0].reconnects, 1);
        assert_eq!(starts.load(Ordering::SeqCst), 2);

        supervisor.shutdown(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn shutdown_timeout_becomes_structured_failure() {
        let supervisor = EventSourceSupervisor::default();
        supervisor.start(
            Box::new(HangingShutdownSource {
                descriptor: HostEventSourceDescriptor::new("hang", "test.plugin"),
            }),
            Arc::new(NoopSubmitter),
            &test_config(),
            Duration::from_millis(20),
        );
        wait_for_state(&supervisor, "running").await;
        let sender = supervisor
            .sources
            .lock()
            .expect("sources")
            .get("hang")
            .expect("source")
            .commands
            .clone();
        sender
            .send(SourceCommand::Shutdown)
            .await
            .expect("shutdown");
        wait_for_state(&supervisor, "failed").await;
        assert!(
            supervisor.list()[0]
                .last_error
                .as_deref()
                .expect("timeout error")
                .contains("timed out")
        );
        supervisor.shutdown(Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn explicit_restart_exposes_restarting_until_graceful_stop_finishes() {
        let supervisor = EventSourceSupervisor::default();
        supervisor.start(
            Box::new(HangingShutdownSource {
                descriptor: HostEventSourceDescriptor::new("restart-visible", "test.plugin"),
            }),
            Arc::new(NoopSubmitter),
            &test_config(),
            Duration::from_millis(100),
        );
        wait_for_state(&supervisor, "running").await;

        supervisor
            .restart("restart-visible")
            .await
            .expect("restart command");
        wait_for_state(&supervisor, "restarting").await;
        assert_eq!(supervisor.list()[0].state, "restarting");
        assert_eq!(supervisor.list()[0].reconnects, 1);

        wait_for_state(&supervisor, "failed").await;
        supervisor.shutdown(Duration::from_millis(100)).await;
    }

    async fn wait_for_state(supervisor: &EventSourceSupervisor, expected: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if supervisor
                    .list()
                    .first()
                    .is_some_and(|source| source.state == expected)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("source reached expected state");
    }

    fn test_config() -> ServiceConfig {
        let mut config = ServiceConfig::default();
        config.ipc.token = Some("test".into());
        config
    }

    struct NoopSubmitter;

    impl TaskSubmitter for NoopSubmitter {
        fn submit_batch(&self, _batch: TaskBatch) -> RuntimeResult<Vec<TaskHandle>> {
            Ok(Vec::new())
        }

        fn cancel_task(&self, _handle: &TaskHandle) -> RuntimeResult<()> {
            Ok(())
        }

        fn task_outcome(&self, _handle: &TaskHandle) -> RuntimeResult<Option<TaskOutcome>> {
            Ok(None)
        }
    }

    struct PanicOnceSource {
        descriptor: HostEventSourceDescriptor,
        starts: Arc<AtomicUsize>,
    }

    impl HostEventSource for PanicOnceSource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, mut ctx: HostEventSourceContext) -> HostEventSourceFuture {
            let start = self.starts.fetch_add(1, Ordering::SeqCst);
            if start == 0 {
                Box::pin(async {
                    panic!("event source panic");
                    #[allow(unreachable_code)]
                    Ok(())
                })
            } else {
                Box::pin(async move {
                    ctx.shutdown.cancelled().await;
                    Ok(())
                })
            }
        }

        fn shutdown(&mut self) -> HostEventSourceFuture {
            Box::pin(async { Ok(()) })
        }

        fn health(&self) -> HostEventSourceHealth {
            HostEventSourceHealth::Healthy
        }
    }

    struct HangingShutdownSource {
        descriptor: HostEventSourceDescriptor,
    }

    impl HostEventSource for HangingShutdownSource {
        fn descriptor(&self) -> &HostEventSourceDescriptor {
            &self.descriptor
        }

        fn start(&mut self, mut ctx: HostEventSourceContext) -> HostEventSourceFuture {
            Box::pin(async move {
                ctx.shutdown.cancelled().await;
                Ok(())
            })
        }

        fn shutdown(&mut self) -> HostEventSourceFuture {
            Box::pin(std::future::pending())
        }

        fn health(&self) -> HostEventSourceHealth {
            HostEventSourceHealth::Healthy
        }
    }
}
