use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use mutsuki_service_config::filtered_environment;
use mutsuki_service_plugin_loader::ExternalRuntimeSpec;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

#[derive(Debug, thiserror::Error)]
pub enum RunnerSupervisorError {
    #[error("runner {0} is already running")]
    AlreadyRunning(String),
    #[error("runner {0} is not known")]
    UnknownRunner(String),
    #[error("failed to spawn runner {runner_id}: {source}")]
    Spawn {
        runner_id: String,
        source: std::io::Error,
    },
    #[error("failed to stop runner {runner_id}: {source}")]
    Stop {
        runner_id: String,
        source: std::io::Error,
    },
}

pub type RunnerSupervisorResult<T> = Result<T, RunnerSupervisorError>;

#[derive(Clone, Debug)]
pub struct ManagedRunnerSpec {
    pub runner_id: String,
    pub plugin_id: String,
    pub runtime: ExternalRuntimeSpec,
    pub env_allowlist: Vec<String>,
    pub service_home: PathBuf,
    pub session_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnerSnapshot {
    pub runner_id: String,
    pub plugin_id: String,
    pub state: RunnerProcessState,
    pub pid: Option<u32>,
    pub restarts: u32,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerProcessState {
    Running,
    Exited(i32),
    Failed,
    Stopped,
}

#[derive(Clone, Default)]
pub struct RunnerSupervisor {
    inner: Arc<Mutex<SupervisorState>>,
}

#[derive(Default)]
struct SupervisorState {
    runners: BTreeMap<String, ManagedRunner>,
}

struct ManagedRunner {
    spec: ManagedRunnerSpec,
    child: Option<Child>,
    state: RunnerProcessState,
    restarts: u32,
    last_error: Option<String>,
}

impl RunnerSupervisor {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn start(&self, spec: ManagedRunnerSpec) -> RunnerSupervisorResult<()> {
        let mut state = self.inner.lock().await;
        if matches!(
            state
                .runners
                .get(&spec.runner_id)
                .map(|runner| &runner.state),
            Some(RunnerProcessState::Running)
        ) {
            return Err(RunnerSupervisorError::AlreadyRunning(spec.runner_id));
        }
        let runner_id = spec.runner_id.clone();
        let child = spawn_child(&spec)?;
        state.runners.insert(
            runner_id,
            ManagedRunner {
                spec,
                child: Some(child),
                state: RunnerProcessState::Running,
                restarts: 0,
                last_error: None,
            },
        );
        Ok(())
    }

    pub async fn list(&self) -> Vec<RunnerSnapshot> {
        let mut state = self.inner.lock().await;
        for runner in state.runners.values_mut() {
            refresh_runner(runner);
        }
        state
            .runners
            .values()
            .map(snapshot_runner)
            .collect::<Vec<_>>()
    }

    pub async fn restart(&self, runner_id: &str) -> RunnerSupervisorResult<()> {
        let mut state = self.inner.lock().await;
        let Some(runner) = state.runners.get_mut(runner_id) else {
            return Err(RunnerSupervisorError::UnknownRunner(runner_id.into()));
        };
        stop_child(runner, Duration::from_secs(5)).await?;
        let child = spawn_child(&runner.spec)?;
        runner.child = Some(child);
        runner.state = RunnerProcessState::Running;
        runner.restarts += 1;
        runner.last_error = None;
        Ok(())
    }

    pub async fn stop(&self, runner_id: &str) -> RunnerSupervisorResult<()> {
        let mut state = self.inner.lock().await;
        let Some(runner) = state.runners.get_mut(runner_id) else {
            return Err(RunnerSupervisorError::UnknownRunner(runner_id.into()));
        };
        stop_child(runner, Duration::from_secs(5)).await?;
        runner.state = RunnerProcessState::Stopped;
        Ok(())
    }

    pub async fn shutdown(&self, graceful: Duration) {
        let mut state = self.inner.lock().await;
        for runner in state.runners.values_mut() {
            let _ = stop_child(runner, graceful).await;
        }
    }
}

fn spawn_child(spec: &ManagedRunnerSpec) -> RunnerSupervisorResult<Child> {
    let mut extra_env = spec.runtime.env.clone();
    extra_env.insert(
        "MUTSUKI_HOME".into(),
        spec.service_home.to_string_lossy().into_owned(),
    );
    extra_env.insert(
        "MUTSUKI_RUNNER_SESSION_TOKEN".into(),
        spec.session_token.clone(),
    );
    extra_env.insert("MUTSUKI_RUNNER_ID".into(), spec.runner_id.clone());
    extra_env.insert("MUTSUKI_PLUGIN_ID".into(), spec.plugin_id.clone());
    let envs = filtered_environment(&spec.env_allowlist, extra_env);

    let mut command = Command::new(&spec.runtime.command);
    command
        .args(&spec.runtime.args)
        .env_clear()
        .envs(envs)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(cwd) = &spec.runtime.cwd {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .map_err(|source| RunnerSupervisorError::Spawn {
            runner_id: spec.runner_id.clone(),
            source,
        })?;
    if let Some(stdout) = child.stdout.take() {
        let runner_id = spec.runner_id.clone();
        tokio::spawn(drain_stream(runner_id, "stdout", stdout));
    }
    if let Some(stderr) = child.stderr.take() {
        let runner_id = spec.runner_id.clone();
        tokio::spawn(drain_stream(runner_id, "stderr", stderr));
    }
    Ok(child)
}

async fn drain_stream<R>(runner_id: String, stream: &'static str, reader: R)
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(reader).lines();
    loop {
        match lines.next_line().await {
            Ok(Some(line)) => tracing::info!(runner_id, stream, line),
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(runner_id, stream, error = %error, "runner stream read failed");
                break;
            }
        }
    }
}

fn refresh_runner(runner: &mut ManagedRunner) {
    let Some(child) = runner.child.as_mut() else {
        return;
    };
    match child.try_wait() {
        Ok(Some(status)) => {
            runner.state = RunnerProcessState::Exited(status.code().unwrap_or(-1));
            runner.child = None;
        }
        Ok(None) => runner.state = RunnerProcessState::Running,
        Err(error) => {
            runner.state = RunnerProcessState::Failed;
            runner.last_error = Some(error.to_string());
        }
    }
}

async fn stop_child(runner: &mut ManagedRunner, graceful: Duration) -> RunnerSupervisorResult<()> {
    let Some(mut child) = runner.child.take() else {
        runner.state = RunnerProcessState::Stopped;
        return Ok(());
    };
    if let Ok(Some(_)) = child.try_wait() {
        runner.state = RunnerProcessState::Stopped;
        return Ok(());
    }
    if timeout(graceful, child.wait()).await.is_err() {
        child
            .kill()
            .await
            .map_err(|source| RunnerSupervisorError::Stop {
                runner_id: runner.spec.runner_id.clone(),
                source,
            })?;
    }
    runner.state = RunnerProcessState::Stopped;
    Ok(())
}

fn snapshot_runner(runner: &ManagedRunner) -> RunnerSnapshot {
    RunnerSnapshot {
        runner_id: runner.spec.runner_id.clone(),
        plugin_id: runner.spec.plugin_id.clone(),
        state: runner.state.clone(),
        pid: runner.child.as_ref().and_then(Child::id),
        restarts: runner.restarts,
        last_error: runner.last_error.clone(),
    }
}
