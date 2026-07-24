use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configured service file does not exist: {path}")]
    MissingConfigFile { path: PathBuf },
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
    #[error("failed to read secret file {path}: {source}")]
    ReadSecretFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse secret file {path}: {source}")]
    ParseSecretFile {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid secret file {path}: {detail}")]
    InvalidSecretFile { path: PathBuf, detail: String },
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
    #[error("Host secret rotation requires a configured secret_file")]
    SecretRotationUnavailable,
    #[error("Host secret {key} is controlled by environment variable {variable}")]
    SecretEnvironmentOverride { key: String, variable: String },
    #[error("Host secret {key} must not be empty")]
    InvalidSecretValue { key: String },
    #[error("configured plugin management requires a loaded product config file")]
    ConfigMutationUnavailable,
    #[error("configured plugin {plugin_id} was not found exactly once in {path}")]
    ConfiguredPluginNotFound { plugin_id: String, path: PathBuf },
    #[error("configured plugin {plugin_id} config cannot be represented as TOML: {detail}")]
    InvalidConfiguredPluginValue { plugin_id: String, detail: String },
    #[error("product surface field `{field}` is read-only")]
    ProductSurfaceReadOnly { field: String },
    #[error("unknown product surface field `{field}`")]
    UnknownProductSurfaceField { field: String },
    #[error("product surface value invalid for `{field}`: {detail}")]
    InvalidProductSurfaceValue { field: String, detail: String },
    #[error("failed to persist managed config file {path}: {source}")]
    WriteManagedFile {
        path: PathBuf,
        source: std::io::Error,
    },
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
    #[serde(skip)]
    secret_store: SecretStore,
    #[serde(skip)]
    configured_plugin_store: Option<ConfiguredPluginStore>,
}

#[derive(Default)]
struct SecretStoreInner {
    entries: RwLock<BTreeMap<String, String>>,
    path: Option<PathBuf>,
    write_lock: Mutex<()>,
}

#[derive(Clone, Default)]
pub struct SecretStore(Arc<SecretStoreInner>);

impl std::fmt::Debug for SecretStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretStore")
            .field(
                "entries",
                &self.0.entries.read().expect("secret store read lock").len(),
            )
            .finish_non_exhaustive()
    }
}

impl SecretStore {
    pub fn resolve(&self, key: &str) -> Option<String> {
        self.0
            .entries
            .read()
            .expect("secret store read lock")
            .get(&normalize_secret_key(key))
            .cloned()
    }

    fn rotate(&self, key: &str, value: String) -> ConfigResult<()> {
        let key = normalize_secret_key(key);
        if key.is_empty() || value.trim().is_empty() {
            return Err(ConfigError::InvalidSecretValue { key });
        }
        let path = self
            .0
            .path
            .clone()
            .ok_or(ConfigError::SecretRotationUnavailable)?;
        let _write = self.0.write_lock.lock().expect("secret store write lock");
        let content = fs::read_to_string(&path).map_err(|source| ConfigError::ReadSecretFile {
            path: path.clone(),
            source,
        })?;
        let mut file: SecretFile =
            toml::from_str(&content).map_err(|source| ConfigError::ParseSecretFile {
                path: path.clone(),
                source,
            })?;
        file.secrets
            .retain(|candidate, _| normalize_secret_key(candidate) != key);
        file.secrets.insert(key.clone(), value);
        let content =
            toml::to_string_pretty(&file).map_err(|source| ConfigError::InvalidSecretFile {
                path: path.clone(),
                detail: source.to_string(),
            })?;
        atomic_write(&path, content.as_bytes(), true)?;
        let entries = validate_secret_entries(&path, file.secrets)?;
        *self.0.entries.write().expect("secret store write lock") = entries;
        Ok(())
    }
}

#[derive(Clone)]
pub struct HostSecretStore {
    store: SecretStore,
    env_prefix: String,
}

impl std::fmt::Debug for HostSecretStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostSecretStore")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl HostSecretStore {
    pub fn rotation_available(&self) -> bool {
        self.store.0.path.is_some()
    }

    pub fn resolve(&self, key: &str) -> Option<String> {
        let key = normalize_secret_key(key);
        env::var(format!("{}{key}", self.env_prefix))
            .ok()
            .or_else(|| self.store.resolve(&key))
    }

    /// Atomically persists a rotated secret in the configured Host secret file.
    /// Environment-backed secrets are intentionally immutable at runtime.
    pub fn rotate(&self, key: &str, value: String) -> ConfigResult<()> {
        let key = normalize_secret_key(key);
        let variable = format!("{}{key}", self.env_prefix);
        if env::var_os(&variable).is_some() {
            return Err(ConfigError::SecretEnvironmentOverride { key, variable });
        }
        self.store.rotate(&key, value)
    }
}

#[derive(Clone, Debug)]
pub struct ConfiguredPluginStore {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl ConfiguredPluginStore {
    /// Open a managed product config file for atomic plugin / product-surface patches.
    pub fn open(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Atomically replaces one owner plugin's opaque config in the product config file.
    pub fn replace_config(&self, plugin_id: &str, config: serde_json::Value) -> ConfigResult<()> {
        let _write = self
            .write_lock
            .lock()
            .expect("configured plugin write lock");
        let mut document = self.read_document()?;
        let configured = document
            .get_mut("plugins")
            .and_then(toml::Value::as_table_mut)
            .and_then(|plugins| plugins.get_mut("configured"))
            .and_then(toml::Value::as_array_mut)
            .ok_or_else(|| ConfigError::ConfiguredPluginNotFound {
                plugin_id: plugin_id.into(),
                path: self.path.clone(),
            })?;
        let matches = configured
            .iter()
            .enumerate()
            .filter(|(_, selection)| {
                selection
                    .get("id")
                    .and_then(toml::Value::as_str)
                    .is_some_and(|id| id.trim() == plugin_id.trim())
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let [index] = matches.as_slice() else {
            return Err(ConfigError::ConfiguredPluginNotFound {
                plugin_id: plugin_id.into(),
                path: self.path.clone(),
            });
        };
        let value =
            json_to_toml(config).map_err(|detail| ConfigError::InvalidConfiguredPluginValue {
                plugin_id: plugin_id.into(),
                detail,
            })?;
        configured[*index]
            .as_table_mut()
            .expect("configured plugin selection is a TOML table")
            .insert("config".into(), value);
        let content = toml::to_string_pretty(&document).map_err(|source| {
            ConfigError::InvalidConfiguredPluginValue {
                plugin_id: plugin_id.into(),
                detail: source.to_string(),
            }
        })?;
        atomic_write(&self.path, content.as_bytes(), false)
    }

    /// Atomically patches known product surface keys under `service` / `web.console`.
    ///
    /// Accepted writable keys: `profile`, `console_enabled`, `console_listen`, `include_config`.
    /// Read-only keys (`instance_id`, `distribution_mode`, `auth_token_key`) are rejected.
    pub fn patch_product_surface(
        &self,
        fields: BTreeMap<String, serde_json::Value>,
    ) -> ConfigResult<()> {
        let _write = self
            .write_lock
            .lock()
            .expect("configured plugin write lock");
        let mut document = self.read_document()?;
        for (field, value) in fields {
            apply_product_surface_field(&mut document, &field, value)?;
        }
        let content =
            toml::to_string_pretty(&document).map_err(|source| ConfigError::WriteManagedFile {
                path: self.path.clone(),
                source: std::io::Error::other(source.to_string()),
            })?;
        atomic_write(&self.path, content.as_bytes(), false)
    }

    fn read_document(&self) -> ConfigResult<toml::Value> {
        let content = fs::read_to_string(&self.path).map_err(|source| ConfigError::ReadFile {
            path: self.path.clone(),
            source,
        })?;
        toml::from_str(&content).map_err(|source| ConfigError::ParseFile {
            path: self.path.clone(),
            source,
        })
    }
}

fn apply_product_surface_field(
    document: &mut toml::Value,
    field: &str,
    value: serde_json::Value,
) -> ConfigResult<()> {
    match field {
        "instance_id" | "distribution_mode" | "auth_token_key" => {
            Err(ConfigError::ProductSurfaceReadOnly {
                field: field.into(),
            })
        }
        "profile" => {
            let text = value
                .as_str()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: field.into(),
                    detail: "expected string".into(),
                })?;
            ensure_table(document, "service")?
                .insert("profile".into(), toml::Value::String(text.into()));
            Ok(())
        }
        "console_enabled" => {
            let flag = value
                .as_bool()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: field.into(),
                    detail: "expected bool".into(),
                })?;
            ensure_nested_table(document, &["web", "console"])?
                .insert("enabled".into(), toml::Value::Boolean(flag));
            Ok(())
        }
        "console_listen" => {
            let text = value
                .as_str()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: field.into(),
                    detail: "expected string".into(),
                })?;
            ensure_nested_table(document, &["web", "console"])?
                .insert("listen".into(), toml::Value::String(text.into()));
            Ok(())
        }
        "include_config" => {
            let flag = value
                .as_bool()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: field.into(),
                    detail: "expected bool".into(),
                })?;
            ensure_nested_table(document, &["web", "console"])?
                .insert("include_config".into(), toml::Value::Boolean(flag));
            Ok(())
        }
        other => Err(ConfigError::UnknownProductSurfaceField {
            field: other.into(),
        }),
    }
}

fn ensure_table<'a>(
    document: &'a mut toml::Value,
    key: &str,
) -> ConfigResult<&'a mut toml::map::Map<String, toml::Value>> {
    if !document
        .as_table()
        .is_some_and(|table| table.contains_key(key))
    {
        document
            .as_table_mut()
            .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                field: key.into(),
                detail: "document root must be a table".into(),
            })?
            .insert(key.into(), toml::Value::Table(toml::map::Map::new()));
    }
    document
        .get_mut(key)
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
            field: key.into(),
            detail: format!("`{key}` must be a table"),
        })
}

fn ensure_nested_table<'a>(
    document: &'a mut toml::Value,
    path: &[&str],
) -> ConfigResult<&'a mut toml::map::Map<String, toml::Value>> {
    let mut current = document;
    for (index, key) in path.iter().enumerate() {
        let is_last = index + 1 == path.len();
        if current
            .as_table()
            .is_none_or(|table| !table.contains_key(*key))
        {
            current
                .as_table_mut()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: key.to_string(),
                    detail: "document path must be tables".into(),
                })?
                .insert((*key).into(), toml::Value::Table(toml::map::Map::new()));
        }
        current = current
            .get_mut(*key)
            .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                field: key.to_string(),
                detail: "missing nested table".into(),
            })?;
        if is_last {
            return current
                .as_table_mut()
                .ok_or_else(|| ConfigError::InvalidProductSurfaceValue {
                    field: key.to_string(),
                    detail: "nested path must end at a table".into(),
                });
        }
        if !current.is_table() {
            return Err(ConfigError::InvalidProductSurfaceValue {
                field: key.to_string(),
                detail: "nested path must be tables".into(),
            });
        }
    }
    Err(ConfigError::InvalidProductSurfaceValue {
        field: path.join("."),
        detail: "empty product surface path".into(),
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
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
#[serde(default)]
pub struct CoreSection {
    pub max_tasks: usize,
    pub shutdown_timeout_ms: u64,
    pub worker_profile: WorkerProfile,
    pub worker_threads: Option<usize>,
    pub blocking_threads: Option<usize>,
    pub pool_queue_limit: Option<usize>,
    pub pool_max_inflight_bytes: Option<usize>,
    pub max_isolated_workers: Option<usize>,
    pub runner_wall_clock_timeout_ms: Option<u64>,
    pub cancel_grace_period_ms: Option<u64>,
    pub worker_health_timeout_ms: Option<u64>,
}

impl Default for CoreSection {
    fn default() -> Self {
        Self {
            max_tasks: 4096,
            shutdown_timeout_ms: 30_000,
            worker_profile: WorkerProfile::Desktop,
            worker_threads: None,
            blocking_threads: None,
            pool_queue_limit: None,
            pool_max_inflight_bytes: None,
            max_isolated_workers: None,
            runner_wall_clock_timeout_ms: None,
            cancel_grace_period_ms: Some(30_000),
            worker_health_timeout_ms: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum WorkerProfile {
    LowResource,
    #[default]
    Desktop,
    Server,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerPoolSettings {
    pub compute_threads: usize,
    pub blocking_threads: usize,
    pub queue_capacity: usize,
    pub max_inflight_bytes: usize,
    pub max_isolated_workers: usize,
}

impl CoreSection {
    pub fn worker_pool_settings(&self) -> WorkerPoolSettings {
        let parallelism = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(2)
            .max(1);
        let profile = match self.worker_profile {
            WorkerProfile::LowResource => WorkerPoolSettings {
                compute_threads: 1,
                blocking_threads: 1,
                queue_capacity: 64,
                max_inflight_bytes: 8 * 1024 * 1024,
                max_isolated_workers: 1,
            },
            WorkerProfile::Desktop => WorkerPoolSettings {
                compute_threads: parallelism,
                blocking_threads: 2,
                queue_capacity: 256,
                max_inflight_bytes: 64 * 1024 * 1024,
                max_isolated_workers: 1,
            },
            WorkerProfile::Server => {
                let blocking_threads = (parallelism / 4).clamp(2, 8);
                WorkerPoolSettings {
                    compute_threads: parallelism,
                    blocking_threads,
                    queue_capacity: 1024,
                    max_inflight_bytes: 256 * 1024 * 1024,
                    max_isolated_workers: blocking_threads,
                }
            }
        };
        WorkerPoolSettings {
            compute_threads: self.worker_threads.unwrap_or(profile.compute_threads),
            blocking_threads: self.blocking_threads.unwrap_or(profile.blocking_threads),
            queue_capacity: self.pool_queue_limit.unwrap_or(profile.queue_capacity),
            max_inflight_bytes: self
                .pool_max_inflight_bytes
                .unwrap_or(profile.max_inflight_bytes),
            max_isolated_workers: self
                .max_isolated_workers
                .unwrap_or(profile.max_isolated_workers),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct IpcSection {
    pub enabled: bool,
    pub transport: IpcTransport,
    pub codec: IpcCodec,
    pub name: String,
    pub token: Option<String>,
    pub tcp_debug_addr: Option<String>,
    pub max_frame_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_jsonl_line_bytes: usize,
    pub max_in_flight: usize,
    pub idle_timeout_ms: u64,
    pub request_timeout_ms: u64,
}

impl Default for IpcSection {
    fn default() -> Self {
        Self {
            enabled: true,
            transport: default_transport(),
            codec: IpcCodec::Binary,
            name: "mutsuki-service-default".into(),
            token: None,
            tcp_debug_addr: None,
            max_frame_bytes: 1024 * 1024,
            max_payload_bytes: 512 * 1024,
            max_jsonl_line_bytes: 256 * 1024,
            max_in_flight: 64,
            idle_timeout_ms: 60_000,
            request_timeout_ms: 30_000,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum IpcCodec {
    #[default]
    Binary,
    Jsonl,
}

impl Default for IpcTransport {
    fn default() -> Self {
        default_transport()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PluginsSection {
    #[serde(default)]
    pub configured: Vec<ConfiguredPluginSelection>,
    pub dynamic_dirs: Vec<PathBuf>,
    pub disabled_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredPluginSelection {
    pub id: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    #[serde(default)]
    pub config: serde_json::Value,
}

fn enabled_by_default() -> bool {
    true
}

impl Default for PluginsSection {
    fn default() -> Self {
        let home = default_home_dir();
        Self {
            configured: Vec::new(),
            dynamic_dirs: vec![home.join("plugins").join("installed")],
            disabled_dir: home.join("plugins").join("disabled"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
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
#[serde(default)]
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
#[serde(default)]
pub struct SecuritySection {
    pub control_token_env: String,
    pub secret_env_prefix: String,
    pub secret_file: Option<PathBuf>,
}

impl Default for SecuritySection {
    fn default() -> Self {
        Self {
            control_token_env: "MUTSUKI_CONTROL_TOKEN".into(),
            secret_env_prefix: "MUTSUKI_SECRET_".into(),
            secret_file: None,
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
    /// Resolves a named secret through the Host environment boundary.
    ///
    /// The returned value must remain inside product assembly and effectful
    /// adapters; task payloads and ordinary configuration should only store
    /// the key passed to this method.
    pub fn secret(&self, key: &str) -> Option<String> {
        self.host_secret_store().resolve(key)
    }

    /// Snapshot used only by Host boundaries that must resolve secret keys
    /// after service configuration has been loaded.
    pub fn secret_store(&self) -> SecretStore {
        self.secret_store.clone()
    }

    /// Host-owned secret boundary for runtime credential resolution and rotation.
    pub fn host_secret_store(&self) -> HostSecretStore {
        HostSecretStore {
            store: self.secret_store.clone(),
            env_prefix: self.security.secret_env_prefix.clone(),
        }
    }

    /// Host-owned product config persistence boundary, available only for a loaded file.
    pub fn configured_plugin_store(&self) -> Option<ConfiguredPluginStore> {
        self.configured_plugin_store.clone()
    }

    pub fn load(overrides: ConfigOverrides) -> ConfigResult<Self> {
        let mut config = Self::default();
        if let Some(home) = overrides.home_dir {
            config.service.home_dir = home;
        } else if let Ok(home) = env::var("MUTSUKI_HOME") {
            config.service.home_dir = PathBuf::from(home);
        }
        config.resolve_relative_dirs();

        let explicit_config_file = overrides.config_file.is_some();
        let local_file = overrides
            .config_file
            .clone()
            .unwrap_or_else(|| config.service.home_dir.join("config").join("service.toml"));
        if explicit_config_file && !local_file.is_file() {
            return Err(ConfigError::MissingConfigFile { path: local_file });
        }

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
        config.load_secret_file(&local_file)?;
        if local_file.is_file() {
            config.configured_plugin_store = Some(ConfiguredPluginStore::open(local_file));
        }
        config.resolve_relative_dirs();
        config.ensure_dirs()?;
        config.ensure_control_token()?;
        Ok(config)
    }

    fn load_secret_file(&mut self, local_file: &Path) -> ConfigResult<()> {
        let Some(configured_path) = self.security.secret_file.clone() else {
            self.secret_store = SecretStore::default();
            return Ok(());
        };
        let path = if configured_path.is_absolute() {
            configured_path
        } else {
            local_file
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(configured_path)
        };
        let content = fs::read_to_string(&path).map_err(|source| ConfigError::ReadSecretFile {
            path: path.clone(),
            source,
        })?;
        let file: SecretFile =
            toml::from_str(&content).map_err(|source| ConfigError::ParseSecretFile {
                path: path.clone(),
                source,
            })?;
        let secrets = validate_secret_entries(&path, file.secrets)?;
        self.security.secret_file = Some(path);
        self.secret_store = SecretStore(Arc::new(SecretStoreInner {
            entries: RwLock::new(secrets),
            path: self.security.secret_file.clone(),
            write_lock: Mutex::new(()),
        }));
        Ok(())
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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretFile {
    secrets: BTreeMap<String, String>,
}

fn validate_secret_entries(
    path: &Path,
    entries: BTreeMap<String, String>,
) -> ConfigResult<BTreeMap<String, String>> {
    let mut secrets = BTreeMap::new();
    for (raw_key, value) in entries {
        let key = normalize_secret_key(&raw_key);
        if key.is_empty() {
            return Err(ConfigError::InvalidSecretFile {
                path: path.to_path_buf(),
                detail: "secret key must not be empty".into(),
            });
        }
        if value.trim().is_empty() {
            return Err(ConfigError::InvalidSecretFile {
                path: path.to_path_buf(),
                detail: format!("secret {raw_key} must not be empty"),
            });
        }
        if secrets.insert(key.clone(), value).is_some() {
            return Err(ConfigError::InvalidSecretFile {
                path: path.to_path_buf(),
                detail: format!("duplicate normalized secret key {key}"),
            });
        }
    }
    Ok(secrets)
}

fn json_to_toml(value: serde_json::Value) -> Result<toml::Value, String> {
    match value {
        serde_json::Value::Null => Err("null values are not representable in TOML".into()),
        serde_json::Value::Bool(value) => Ok(toml::Value::Boolean(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(toml::Value::Integer(value))
            } else if let Some(value) = value.as_u64() {
                i64::try_from(value)
                    .map(toml::Value::Integer)
                    .map_err(|_| "unsigned integer exceeds TOML range".into())
            } else {
                value
                    .as_f64()
                    .map(toml::Value::Float)
                    .ok_or_else(|| "invalid JSON number".into())
            }
        }
        serde_json::Value::String(value) => Ok(toml::Value::String(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(json_to_toml)
            .collect::<Result<Vec<_>, _>>()
            .map(toml::Value::Array),
        serde_json::Value::Object(values) => values
            .into_iter()
            .map(|(key, value)| json_to_toml(value).map(|value| (key, value)))
            .collect::<Result<toml::map::Map<_, _>, _>>()
            .map(toml::Value::Table),
    }
}

fn atomic_write(path: &Path, bytes: &[u8], secret: bool) -> ConfigResult<()> {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let temp = path.with_extension(format!("mutsuki-{nonce:x}.tmp"));
    fs::write(&temp, bytes).map_err(|source| ConfigError::WriteManagedFile {
        path: temp.clone(),
        source,
    })?;
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o600)).map_err(|source| {
            ConfigError::WriteManagedFile {
                path: temp.clone(),
                source,
            }
        })?;
    }
    #[cfg(not(unix))]
    let _ = secret;
    fs::rename(&temp, path).map_err(|source| ConfigError::WriteManagedFile {
        path: path.to_path_buf(),
        source,
    })
}

fn normalize_secret_key(key: &str) -> String {
    key.trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
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
    #[cfg(windows)]
    let mut vars = vec!["PATH".to_string()];
    #[cfg(not(windows))]
    let vars = vec!["PATH".to_string()];
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
    use std::sync::Mutex;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn worker_profiles_keep_default_threads_close_to_compute_plus_bounded_blocking() {
        let mut core = CoreSection {
            worker_profile: WorkerProfile::LowResource,
            ..CoreSection::default()
        };
        let low = core.worker_pool_settings();
        assert_eq!((low.compute_threads, low.blocking_threads), (1, 1));

        core.worker_profile = WorkerProfile::Desktop;
        let desktop = core.worker_pool_settings();
        assert_eq!(desktop.blocking_threads, 2);
        assert!(desktop.compute_threads >= 1);

        core.worker_profile = WorkerProfile::Server;
        let server = core.worker_pool_settings();
        assert_eq!(server.compute_threads, desktop.compute_threads);
        assert!((2..=8).contains(&server.blocking_threads));
        assert!(server.queue_capacity > desktop.queue_capacity);
    }

    #[test]
    fn explicit_worker_overrides_are_applied_on_top_of_profile() {
        let core = CoreSection {
            worker_profile: WorkerProfile::LowResource,
            worker_threads: Some(3),
            blocking_threads: Some(4),
            pool_queue_limit: Some(5),
            pool_max_inflight_bytes: Some(6),
            max_isolated_workers: Some(2),
            runner_wall_clock_timeout_ms: None,
            cancel_grace_period_ms: None,
            worker_health_timeout_ms: None,
            ..CoreSection::default()
        };

        assert_eq!(
            core.worker_pool_settings(),
            WorkerPoolSettings {
                compute_threads: 3,
                blocking_threads: 4,
                queue_capacity: 5,
                max_inflight_bytes: 6,
                max_isolated_workers: 2,
            }
        );
    }

    #[test]
    fn directory_access_probe_is_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();

        verify_directory_access(dir.path()).unwrap();

        assert!(fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn secret_file_loads_relative_to_config_and_environment_overrides_it() {
        let _env = ENV_LOCK.lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        let config_path = write_product_config(root.path(), "local.secret.toml");
        fs::write(
            root.path().join("local.secret.toml"),
            "[secrets]\nQQBOT_CLIENT_SECRET = \"FILE_SECRET\"\n",
        )
        .unwrap();

        let config = ServiceConfig::load(ConfigOverrides {
            config_file: Some(config_path),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            config.secret("qqbot-client-secret").as_deref(),
            Some("FILE_SECRET")
        );
        assert!(!format!("{config:?}").contains("FILE_SECRET"));
        assert!(!toml::to_string(&config).unwrap().contains("FILE_SECRET"));

        unsafe { env::set_var("MUTSUKI_SECRET_QQBOT_CLIENT_SECRET", "ENV_SECRET") };
        assert_eq!(
            config.secret("QQBOT_CLIENT_SECRET").as_deref(),
            Some("ENV_SECRET")
        );
        unsafe { env::remove_var("MUTSUKI_SECRET_QQBOT_CLIENT_SECRET") };
    }

    #[test]
    fn host_secret_rotation_is_atomic_shared_and_environment_safe() {
        let _env = ENV_LOCK.lock().unwrap();
        let root = tempfile::tempdir().unwrap();
        let config_path = write_product_config(root.path(), "local.secret.toml");
        let secret_path = root.path().join("local.secret.toml");
        fs::write(&secret_path, "[secrets]\nBILIBILI_COOKIE = \"OLD\"\n").unwrap();
        let config = ServiceConfig::load(ConfigOverrides {
            config_file: Some(config_path),
            ..Default::default()
        })
        .unwrap();
        let first = config.host_secret_store();
        let second = config.host_secret_store();

        first
            .rotate("bilibili-cookie", "SESSDATA=ROTATED".into())
            .unwrap();
        assert_eq!(
            second.resolve("BILIBILI_COOKIE").as_deref(),
            Some("SESSDATA=ROTATED")
        );
        assert!(
            fs::read_to_string(&secret_path)
                .unwrap()
                .contains("SESSDATA=ROTATED")
        );
        assert!(!format!("{first:?}").contains("SESSDATA"));

        unsafe { env::set_var("MUTSUKI_SECRET_BILIBILI_COOKIE", "ENV") };
        assert!(matches!(
            first.rotate("BILIBILI_COOKIE", "REJECTED".into()),
            Err(ConfigError::SecretEnvironmentOverride { .. })
        ));
        unsafe { env::remove_var("MUTSUKI_SECRET_BILIBILI_COOKIE") };
    }

    #[test]
    fn configured_plugin_store_replaces_only_owner_config() {
        let root = tempfile::tempdir().unwrap();
        let config_path = root.path().join("local.toml");
        let content = format!(
            "[service]\nhome_dir = \"{}\"\n\n[ipc]\nenabled = false\n\n[plugins]\ndynamic_dirs = []\n\n[[plugins.configured]]\nid = \"mutsuki.bot.bilibili\"\n\n[plugins.configured.config]\ncookie_secret_key = \"BILIBILI_COOKIE\"\nsubscriptions = []\n\n[[plugins.configured]]\nid = \"other.plugin\"\n\n[plugins.configured.config]\nmode = \"kept\"\n",
            root.path()
                .join("home")
                .to_string_lossy()
                .replace('\\', "/")
        );
        fs::write(&config_path, content).unwrap();
        let config = ServiceConfig::load(ConfigOverrides {
            config_file: Some(config_path.clone()),
            ..Default::default()
        })
        .unwrap();
        config
            .configured_plugin_store()
            .unwrap()
            .replace_config(
                "mutsuki.bot.bilibili",
                serde_json::json!({
                    "cookie_secret_key": "BILIBILI_COOKIE",
                    "subscriptions": [{"id": "alice", "paused": true}]
                }),
            )
            .unwrap();

        let persisted: toml::Value =
            toml::from_str(&fs::read_to_string(config_path).unwrap()).unwrap();
        let configured = persisted["plugins"]["configured"].as_array().unwrap();
        assert_eq!(
            configured[0]["config"]["subscriptions"][0]["id"].as_str(),
            Some("alice")
        );
        assert_eq!(configured[1]["config"]["mode"].as_str(), Some("kept"));
    }

    #[test]
    fn product_toml_store_patches_console_surface_atomically() {
        let root = tempfile::tempdir().unwrap();
        let config_path = root.path().join("local.toml");
        fs::write(
            &config_path,
            format!(
                "[service]\nhome_dir = \"{}\"\nprofile = \"bot\"\ninstance_id = \"demo\"\n\n[ipc]\nenabled = false\n\n[web.console]\nenabled = false\nlisten = \"127.0.0.1:1\"\ninclude_config = false\n",
                root.path()
                    .join("home")
                    .to_string_lossy()
                    .replace('\\', "/")
            ),
        )
        .unwrap();
        let store = ConfiguredPluginStore::open(&config_path);
        store
            .patch_product_surface(BTreeMap::from([
                ("profile".into(), serde_json::json!("prod")),
                ("console_enabled".into(), serde_json::json!(true)),
                ("console_listen".into(), serde_json::json!("127.0.0.1:8787")),
                ("include_config".into(), serde_json::json!(true)),
            ]))
            .unwrap();
        let persisted: toml::Value =
            toml::from_str(&fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(persisted["service"]["profile"].as_str(), Some("prod"));
        assert_eq!(persisted["service"]["instance_id"].as_str(), Some("demo"));
        assert_eq!(persisted["web"]["console"]["enabled"].as_bool(), Some(true));
        assert_eq!(
            persisted["web"]["console"]["listen"].as_str(),
            Some("127.0.0.1:8787")
        );
        assert_eq!(
            persisted["web"]["console"]["include_config"].as_bool(),
            Some(true)
        );
        assert!(matches!(
            store.patch_product_surface(BTreeMap::from([(
                "instance_id".into(),
                serde_json::json!("hijack")
            )])),
            Err(ConfigError::ProductSurfaceReadOnly { .. })
        ));
    }

    #[test]
    fn explicit_config_and_secret_files_fail_loud() {
        let root = tempfile::tempdir().unwrap();
        let missing_config = root.path().join("missing.toml");
        assert!(matches!(
            ServiceConfig::load(ConfigOverrides {
                config_file: Some(missing_config.clone()),
                ..Default::default()
            }),
            Err(ConfigError::MissingConfigFile { path }) if path == missing_config
        ));

        let config_path = write_product_config(root.path(), "missing.secret.toml");
        assert!(matches!(
            ServiceConfig::load(ConfigOverrides {
                config_file: Some(config_path),
                ..Default::default()
            }),
            Err(ConfigError::ReadSecretFile { .. })
        ));
    }

    #[test]
    fn partial_sections_keep_host_defaults() {
        let root = tempfile::tempdir().unwrap();
        let config_path = root.path().join("simple.toml");
        fs::write(
            &config_path,
            format!(
                r#"[service]
profile = "simple"
home_dir = "{}"

[ipc]
enabled = false

[plugins]
dynamic_dirs = []

[observe]
json = true
"#,
                root.path()
                    .join("home")
                    .to_string_lossy()
                    .replace('\\', "/")
            ),
        )
        .unwrap();

        let config = ServiceConfig::load(ConfigOverrides {
            config_file: Some(config_path),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(config.service.profile, "simple");
        assert_eq!(config.service.instance_id, "default");
        assert_eq!(config.service.home_dir, root.path().join("home"));
        assert_eq!(
            config.service.data_dir,
            config.service.home_dir.join("data")
        );
        assert!(!config.ipc.enabled);
        assert_eq!(config.ipc.name, "mutsuki-service-default");
        assert!(config.plugins.dynamic_dirs.is_empty());
        assert!(config.observe.json);
        assert_eq!(config.observe.log_file, "service.log");
    }

    #[test]
    fn legacy_builtin_plugin_selection_is_rejected() {
        let error = toml::from_str::<PluginsSection>(
            r#"builtin = ["legacy.plugin"]
dynamic_dirs = []
disabled_dir = "disabled"
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown field `builtin`"));
    }

    #[test]
    fn secret_file_rejects_malformed_empty_and_duplicate_entries() {
        for (name, content, expected) in [
            ("malformed", "not = [valid", "parse"),
            (
                "empty",
                "[secrets]\nQQBOT_CLIENT_SECRET = \"  \"\n",
                "invalid",
            ),
            (
                "duplicate",
                "[secrets]\n\"qqbot-client-secret\" = \"one\"\nQQBOT_CLIENT_SECRET = \"two\"\n",
                "invalid",
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let secret_name = format!("{name}.secret.toml");
            let config_path = write_product_config(root.path(), &secret_name);
            fs::write(root.path().join(secret_name), content).unwrap();
            let error = ServiceConfig::load(ConfigOverrides {
                config_file: Some(config_path),
                ..Default::default()
            })
            .unwrap_err();
            match expected {
                "parse" => assert!(matches!(error, ConfigError::ParseSecretFile { .. })),
                "invalid" => assert!(matches!(error, ConfigError::InvalidSecretFile { .. })),
                _ => unreachable!(),
            }
        }
    }

    fn write_product_config(root: &Path, secret_file: &str) -> PathBuf {
        let path = root.join("local.toml");
        fs::write(
            &path,
            format!(
                r#"[service]
profile = "test"
instance_id = "test"
home_dir = "{}"
data_dir = "data"
log_dir = "logs"
plugin_dir = "plugins"
run_dir = "run"

[ipc]
enabled = false
transport = "named-pipe"
name = "secret-test"

[security]
secret_file = "{secret_file}"
"#,
                root.to_string_lossy().replace('\\', "/")
            ),
        )
        .unwrap();
        path
    }
}
