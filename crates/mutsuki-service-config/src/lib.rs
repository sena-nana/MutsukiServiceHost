use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    ParseFile {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("failed to create directory {path}: {source}")]
    CreateDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("configured directory is not readable and writable {path}: {source}")]
    DirectoryAccess {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("control token is required; set MUTSUKI_CONTROL_TOKEN or configure [ipc].token")]
    MissingControlToken,
}

pub type ConfigResult<T> = Result<T, ConfigError>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub service: ServiceSection,
    #[serde(default)]
    pub core: CoreSection,
    #[serde(default)]
    pub ipc: IpcSection,
    #[serde(default)]
    pub plugins: PluginsSection,
    #[serde(default)]
    pub runners: RunnersSection,
    #[serde(default)]
    pub observe: ObserveSection,
    #[serde(default)]
    pub security: SecuritySection,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceSection {
    pub profile: String,
    pub instance_id: String,
    pub home_dir: PathBuf,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub plugin_dir: PathBuf,
    pub run_dir: PathBuf,
}

impl Default for ServiceSection {
    fn default() -> Self {
        let home = default_home_dir();
        Self {
            profile: "default".into(),
            instance_id: "default".into(),
            home_dir: home,
            data_dir: PathBuf::from("data"),
            log_dir: PathBuf::from("logs"),
            plugin_dir: PathBuf::from("plugins"),
            run_dir: PathBuf::from("run"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoreSection {
    pub max_tasks: usize,
    pub shutdown_timeout_ms: u64,
    pub worker_threads: usize,
    pub blocking_threads: usize,
}

impl Default for CoreSection {
    fn default() -> Self {
        Self {
            max_tasks: 4096,
            shutdown_timeout_ms: 30_000,
            worker_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(2),
            blocking_threads: 2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IpcSection {
    pub enabled: bool,
    pub transport: IpcTransport,
    pub name: String,
    pub token: Option<String>,
    pub tcp_debug_addr: Option<String>,
}

impl Default for IpcSection {
    fn default() -> Self {
        Self {
            enabled: true,
            transport: default_transport(),
            name: "mutsuki-service-default".into(),
            token: None,
            tcp_debug_addr: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum IpcTransport {
    NamedPipe,
    UnixSocket,
    TcpDebug,
}

impl Default for IpcTransport {
    fn default() -> Self {
        default_transport()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginsSection {
    pub builtin: Vec<String>,
    pub dynamic_dirs: Vec<PathBuf>,
    pub disabled_dir: PathBuf,
}

impl Default for PluginsSection {
    fn default() -> Self {
        let home = default_home_dir();
        Self {
            builtin: Vec::new(),
            dynamic_dirs: vec![home.join("plugins").join("installed")],
            disabled_dir: home.join("plugins").join("disabled"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunnersSection {
    pub restart: bool,
    pub max_restart_per_minute: u32,
    pub graceful_shutdown_ms: u64,
    pub env_allowlist: Vec<String>,
}

impl Default for RunnersSection {
    fn default() -> Self {
        Self {
            restart: true,
            max_restart_per_minute: 5,
            graceful_shutdown_ms: 5_000,
            env_allowlist: default_env_allowlist(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObserveSection {
    pub console: bool,
    pub json: bool,
    pub log_file: String,
    pub panic_file: String,
}

impl Default for ObserveSection {
    fn default() -> Self {
        Self {
            console: true,
            json: false,
            log_file: "service.log".into(),
            panic_file: "panic.log".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecuritySection {
    pub control_token_env: String,
    pub secret_env_prefix: String,
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            control_token_env: "MUTSUKI_CONTROL_TOKEN".into(),
            secret_env_prefix: "MUTSUKI_SECRET_".into(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConfigOverrides {
    pub profile: Option<String>,
    pub config_file: Option<PathBuf>,
    pub home_dir: Option<PathBuf>,
    pub control_token: Option<String>,
}

impl ServiceConfig {
    pub fn load(overrides: ConfigOverrides) -> ConfigResult<Self> {
        let mut config = Self::default();
        if let Some(home) = overrides.home_dir {
            config.service.home_dir = home;
        } else if let Ok(home) = env::var("MUTSUKI_HOME") {
            config.service.home_dir = PathBuf::from(home);
        }
        config.resolve_relative_dirs();

        let local_file = overrides
            .config_file
            .clone()
            .unwrap_or_else(|| config.service.home_dir.join("config").join("service.toml"));

        let local_profile = read_optional_config(&local_file)?
            .map(|file_config| file_config.service.profile)
            .filter(|profile| !profile.is_empty());
        if let Some(profile) = overrides
            .profile
            .clone()
            .or_else(|| env::var("MUTSUKI_PROFILE").ok())
            .or(local_profile)
        {
            config.service.profile = profile;
        }

        let profile_file = config
            .service
            .home_dir
            .join("config")
            .join("profiles")
            .join(format!("{}.toml", config.service.profile));
        if let Some(profile_config) = read_optional_config(&profile_file)? {
            config.merge(profile_config);
        }
        if let Some(local_config) = read_optional_config(&local_file)? {
            config.merge(local_config);
        }
        config.apply_env();
        if let Some(profile) = overrides.profile {
            config.service.profile = profile;
        }
        if let Some(token) = overrides.control_token {
            config.ipc.token = Some(token);
        }
        config.resolve_relative_dirs();
        config.ensure_dirs()?;
        config.ensure_control_token()?;
        Ok(config)
    }

    pub fn control_token(&self) -> &str {
        self.ipc
            .token
            .as_deref()
            .expect("config validated control token")
    }

    pub fn ipc_endpoint(&self) -> String {
        match self.ipc.transport {
            IpcTransport::NamedPipe => self.ipc.name.clone(),
            IpcTransport::UnixSocket => self
                .service
                .run_dir
                .join(format!("{}.sock", self.ipc.name))
                .to_string_lossy()
                .into_owned(),
            IpcTransport::TcpDebug => self
                .ipc
                .tcp_debug_addr
                .clone()
                .unwrap_or_else(|| "127.0.0.1:7687".into()),
        }
    }

    fn merge(&mut self, other: ServiceConfig) {
        self.service = other.service;
        self.core = other.core;
        self.ipc = other.ipc;
        self.plugins = other.plugins;
        self.runners = other.runners;
        self.observe = other.observe;
        self.security = other.security;
    }

    fn apply_env(&mut self) {
        if let Ok(instance) = env::var("MUTSUKI_INSTANCE_ID") {
            self.service.instance_id = instance;
        }
        if let Ok(token) = env::var(&self.security.control_token_env) {
            self.ipc.token = Some(token);
        }
        if let Ok(transport) = env::var("MUTSUKI_IPC_TRANSPORT") {
            self.ipc.transport = match transport.as_str() {
                "named-pipe" => IpcTransport::NamedPipe,
                "unix-socket" => IpcTransport::UnixSocket,
                "tcp-debug" => IpcTransport::TcpDebug,
                _ => self.ipc.transport.clone(),
            };
        }
    }

    fn resolve_relative_dirs(&mut self) {
        let home = self.service.home_dir.clone();
        self.service.data_dir = absolutize(&home, &self.service.data_dir);
        self.service.log_dir = absolutize(&home, &self.service.log_dir);
        self.service.plugin_dir = absolutize(&home, &self.service.plugin_dir);
        self.service.run_dir = absolutize(&home, &self.service.run_dir);
        self.plugins.dynamic_dirs = self
            .plugins
            .dynamic_dirs
            .iter()
            .map(|path| absolutize(&home, path))
            .collect();
        self.plugins.disabled_dir = absolutize(&home, &self.plugins.disabled_dir);
    }

    fn ensure_dirs(&self) -> ConfigResult<()> {
        for path in [
            &self.service.home_dir,
            &self.service.data_dir,
            &self.service.log_dir,
            &self.service.plugin_dir,
            &self.service.run_dir,
        ] {
            fs::create_dir_all(path).map_err(|source| ConfigError::CreateDir {
                path: path.clone(),
                source,
            })?;
            verify_directory_access(path)?;
        }
        Ok(())
    }

    fn ensure_control_token(&mut self) -> ConfigResult<()> {
        if self.ipc.token.is_some() {
            return Ok(());
        }
        if env::var("MUTSUKI_TEST_ALLOW_EMPTY_TOKEN").as_deref() == Ok("1") {
            self.ipc.token = Some(String::new());
            return Ok(());
        }
        let token_path = self.service.run_dir.join("control.token");
        if let Ok(token) = fs::read_to_string(&token_path) {
            let token = token.trim().to_string();
            if !token.is_empty() {
                self.ipc.token = Some(token);
                return Ok(());
            }
        }
        let generated = generate_local_token();
        fs::write(&token_path, &generated).map_err(|source| ConfigError::CreateDir {
            path: token_path,
            source,
        })?;
        self.ipc.token = Some(generated);
        Ok(())
    }
}

fn verify_directory_access(path: &Path) -> ConfigResult<()> {
    let access_error = |source| ConfigError::DirectoryAccess {
        path: path.to_path_buf(),
        source,
    };
    fs::read_dir(path).map_err(&access_error)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let probe = path.join(format!(
        ".mutsuki-access-probe-{}-{nonce:x}",
        std::process::id()
    ));
    fs::write(&probe, []).map_err(&access_error)?;
    fs::remove_file(probe).map_err(access_error)
}

fn default_home_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".mutsuki")
}

fn read_optional_config(path: &Path) -> ConfigResult<Option<ServiceConfig>> {
    if !path.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
        path: path.to_path_buf(),
        source,
    })?;
    let config = toml::from_str(&content).map_err(|source| ConfigError::ParseFile {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(Some(config))
}

fn absolutize(home: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        home.join(path)
    }
}

fn default_env_allowlist() -> Vec<String> {
    let mut vars = vec!["PATH".to_string()];
    #[cfg(windows)]
    {
        vars.extend(["SystemRoot".into(), "WINDIR".into(), "COMSPEC".into()]);
    }
    vars
}

fn default_transport() -> IpcTransport {
    #[cfg(windows)]
    {
        IpcTransport::NamedPipe
    }
    #[cfg(not(windows))]
    {
        IpcTransport::UnixSocket
    }
}

fn generate_local_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("local-{nanos:x}-{}", std::process::id())
}

pub fn filtered_environment(
    allowlist: &[String],
    extra: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut envs = BTreeMap::new();
    for key in allowlist {
        if let Ok(value) = env::var(key) {
            envs.insert(key.clone(), value);
        }
    }
    envs.extend(extra);
    envs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_access_probe_is_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();

        verify_directory_access(dir.path()).unwrap();

        assert!(fs::read_dir(dir.path()).unwrap().next().is_none());
    }
}
