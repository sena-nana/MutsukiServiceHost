use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mutsuki_runtime_contracts::{ArtifactType, PluginDeploymentKind, PluginManifest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum PluginLoaderError {
    #[error("failed to read plugin directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to read plugin manifest {path}: {source}")]
    ReadManifest {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse plugin manifest {path}: {source}")]
    ParseManifest {
        path: PathBuf,
        source: Box<toml::de::Error>,
    },
    #[error("plugin {plugin_id} uses unsupported api version {api_version}")]
    UnsupportedApiVersion {
        plugin_id: String,
        api_version: String,
    },
    #[error("plugin {plugin_id} deployment {deployment:?} does not match artifact {artifact:?}")]
    DeploymentMismatch {
        plugin_id: String,
        deployment: PluginDeploymentKind,
        artifact: ArtifactType,
    },
    #[error(
        "plugin {plugin_id} runtime env contains secret-like key {key}; pass secrets by host backend or environment references"
    )]
    SecretInManifest { plugin_id: String, key: String },
    #[error("requested builtin plugin {0} is not linked into this ServiceHost build")]
    BuiltinUnavailable(String),
}

pub type PluginLoaderResult<T> = Result<T, PluginLoaderError>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginToml {
    pub manifest: PluginManifest,
    #[serde(default)]
    pub runtime: Option<ExternalRuntimeSpec>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRuntimeSpec {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default = "default_runner_link")]
    pub runner_link: String,
}

#[derive(Clone, Debug)]
pub struct PluginRecord {
    pub manifest_path: PathBuf,
    pub manifest: PluginManifest,
    pub runtime: Option<ExternalRuntimeSpec>,
    pub enabled: bool,
}

#[derive(Clone, Default)]
pub struct PluginCatalog {
    pub records: Vec<PluginRecord>,
    pub host_plugins: BTreeMap<String, Arc<dyn HostPlugin>>,
}

#[derive(Clone, Default)]
pub struct BuiltinRegistry {
    manifests: BTreeMap<String, PluginManifest>,
    plugins: BTreeMap<String, Arc<dyn HostPlugin>>,
}

#[derive(Clone, Default)]
pub struct BuiltinSelection {
    pub records: Vec<PluginRecord>,
    pub host_plugins: BTreeMap<String, Arc<dyn HostPlugin>>,
}

/// Control-plane facade for host-linked plugins (not a parallel business runtime path).
/// Callers must route through Core `HostContext`; plugins must not register capabilities at call time.
pub trait HostPlugin: Send + Sync {
    fn manifest(&self) -> &PluginManifest;
    fn call(&self, operation: &str, payload: Value) -> HostPluginCallResult<Value>;
}

#[derive(Debug, thiserror::Error)]
pub enum HostPluginCallError {
    #[error("unsupported plugin operation: {0}")]
    UnsupportedOperation(String),
    #[error("bad plugin request: {0}")]
    BadRequest(String),
    #[error("plugin operation failed: {0}")]
    Failed(String),
}

pub type HostPluginCallResult<T> = Result<T, HostPluginCallError>;

impl BuiltinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, plugin: Arc<dyn HostPlugin>) {
        self.manifests.insert(
            plugin.manifest().plugin_id.clone(),
            plugin.manifest().clone(),
        );
        self.plugins
            .insert(plugin.manifest().plugin_id.clone(), plugin);
    }

    /// Registers a product-linked builtin that has no control-plane `HostPlugin` facade.
    pub fn register_manifest(&mut self, manifest: PluginManifest) {
        self.manifests.insert(manifest.plugin_id.clone(), manifest);
    }

    pub fn load_requested(&self, requested: &[String]) -> PluginLoaderResult<BuiltinSelection> {
        let mut records = Vec::new();
        let mut host_plugins = BTreeMap::new();
        for plugin_id in requested {
            let Some(manifest) = self.manifests.get(plugin_id) else {
                return Err(PluginLoaderError::BuiltinUnavailable(plugin_id.clone()));
            };
            validate_manifest(manifest, None)?;
            records.push(PluginRecord {
                manifest_path: PathBuf::from("<builtin>"),
                manifest: manifest.clone(),
                runtime: None,
                enabled: true,
            });
            if let Some(plugin) = self.plugins.get(plugin_id) {
                host_plugins.insert(plugin_id.clone(), plugin.clone());
            }
        }
        Ok(BuiltinSelection {
            records,
            host_plugins,
        })
    }
}

impl PluginCatalog {
    pub fn scan(
        dynamic_dirs: &[PathBuf],
        disabled_dir: &Path,
        builtin: BuiltinSelection,
    ) -> PluginLoaderResult<Self> {
        let mut records = builtin.records;
        let disabled_plugins = disabled_plugins(disabled_dir);
        for dir in dynamic_dirs {
            if !dir.exists() {
                continue;
            }
            for entry in fs::read_dir(dir).map_err(|source| PluginLoaderError::ReadDir {
                path: dir.clone(),
                source,
            })? {
                let entry = entry.map_err(|source| PluginLoaderError::ReadDir {
                    path: dir.clone(),
                    source,
                })?;
                let path = entry.path();
                let manifest_path = if path.is_dir() {
                    path.join("plugin.toml")
                } else {
                    path
                };
                if manifest_path.file_name().and_then(|name| name.to_str()) != Some("plugin.toml")
                    || !manifest_path.exists()
                {
                    continue;
                }
                let record = read_plugin_manifest(&manifest_path)?;
                let enabled = record.enabled.unwrap_or(true)
                    && !disabled_plugins.contains(&record.manifest.plugin_id);
                validate_manifest(&record.manifest, record.runtime.as_ref())?;
                records.push(PluginRecord {
                    manifest_path,
                    manifest: record.manifest,
                    runtime: record.runtime,
                    enabled,
                });
            }
        }
        records.sort_by(|a, b| a.manifest.plugin_id.cmp(&b.manifest.plugin_id));
        Ok(Self {
            records,
            host_plugins: builtin.host_plugins,
        })
    }

    pub fn external_records(&self) -> impl Iterator<Item = &PluginRecord> {
        self.records.iter().filter(|record| {
            record.enabled
                && matches!(
                    record.manifest.artifact.artifact_type,
                    ArtifactType::Process | ArtifactType::Python
                )
        })
    }
}

fn read_plugin_manifest(path: &Path) -> PluginLoaderResult<PluginToml> {
    let content = fs::read_to_string(path).map_err(|source| PluginLoaderError::ReadManifest {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&content).map_err(|source| PluginLoaderError::ParseManifest {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

fn validate_manifest(
    manifest: &PluginManifest,
    runtime: Option<&ExternalRuntimeSpec>,
) -> PluginLoaderResult<()> {
    if manifest.api_version != "mutsuki-plugin-v1" {
        return Err(PluginLoaderError::UnsupportedApiVersion {
            plugin_id: manifest.plugin_id.clone(),
            api_version: manifest.api_version.clone(),
        });
    }
    let deployment = PluginDeploymentKind::default_for_artifact(&manifest.artifact.artifact_type);
    if !deployment.is_compatible_with_artifact(&manifest.artifact.artifact_type) {
        return Err(PluginLoaderError::DeploymentMismatch {
            plugin_id: manifest.plugin_id.clone(),
            deployment,
            artifact: manifest.artifact.artifact_type.clone(),
        });
    }
    if let Some(runtime) = runtime {
        for key in runtime.env.keys() {
            let upper = key.to_ascii_uppercase();
            if upper.contains("TOKEN") || upper.contains("SECRET") || upper.contains("PASSWORD") {
                return Err(PluginLoaderError::SecretInManifest {
                    plugin_id: manifest.plugin_id.clone(),
                    key: key.clone(),
                });
            }
        }
    }
    Ok(())
}

fn disabled_plugins(disabled_dir: &Path) -> BTreeSet<String> {
    let mut disabled = BTreeSet::new();
    let Ok(entries) = fs::read_dir(disabled_dir) else {
        return disabled;
    };
    for entry in entries.flatten() {
        if let Some(name) = entry.file_name().to_str() {
            disabled.insert(name.trim_end_matches(".disabled").to_string());
        }
    }
    disabled
}

fn default_runner_link() -> String {
    "jsonl-stdio".into()
}
