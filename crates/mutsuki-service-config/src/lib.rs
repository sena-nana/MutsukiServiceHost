use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
}

#[derive(Clone, Default)]
pub struct SecretStore(Arc<BTreeMap<String, String>>);

impl std::fmt::Debug for SecretStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecretStore")
            .field("entries", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl SecretStore {
    pub fn resolve(&self, key: &str) -> Option<String> {
        self.0.get(&normalize_secret_key(key)).cloned()
    }
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
#[serde(default)]
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
        let key = normalize_secret_key(key);
        env::var(format!("{}{key}", self.security.secret_env_prefix))
            .ok()
            .or_else(|| self.secret_store.resolve(&key))
    }

    /// Snapshot used only by Host boundaries that must resolve secret keys
    /// after service configuration has been loaded.
    pub fn secret_store(&self) -> SecretStore {
        self.secret_store.clone()
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
        let mut secrets = BTreeMap::new();
        for (raw_key, value) in file.secrets {
            let key = normalize_secret_key(&raw_key);
            if key.is_empty() {
                return Err(ConfigError::InvalidSecretFile {
                    path,
                    detail: "secret key must not be empty".into(),
                });
            }
            if value.trim().is_empty() {
                return Err(ConfigError::InvalidSecretFile {
                    path,
                    detail: format!("secret {raw_key} must not be empty"),
                });
            }
            if secrets.insert(key.clone(), value).is_some() {
                return Err(ConfigError::InvalidSecretFile {
                    path,
                    detail: format!("duplicate normalized secret key {key}"),
                });
            }
        }
        self.security.secret_file = Some(path);
        self.secret_store = SecretStore(Arc::new(secrets));
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SecretFile {
    secrets: BTreeMap<String, String>,
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
    use std::sync::Mutex;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
