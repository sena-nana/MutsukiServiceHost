use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use mutsuki_runtime_contracts::{ArtifactType, PluginDeploymentKind, PluginManifest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    #[error("dynamic plugin {0} declares a native artifact that is not linked into this build")]
    DynamicNative(String),
    #[error("duplicate plugin id {0} in builtin or dynamic catalogs")]
    DuplicatePlugin(String),
    #[error("ABI artifact path for plugin {plugin_id} is invalid: {detail}")]
    InvalidArtifactPath { plugin_id: String, detail: String },
    #[error("ABI artifact for plugin {plugin_id} is missing: {path}")]
    MissingArtifact { plugin_id: String, path: PathBuf },
    #[error("ABI artifact for plugin {plugin_id} has an invalid platform extension: {path}")]
    InvalidArtifactExtension { plugin_id: String, path: PathBuf },
    #[error("ABI artifact hash for plugin {plugin_id} must be sha256:<64 lowercase hex>")]
    InvalidArtifactHash { plugin_id: String },
    #[error("ABI artifact hash mismatch for plugin {plugin_id}: expected {expected}, got {actual}")]
    ArtifactHashMismatch {
        plugin_id: String,
        expected: String,
        actual: String,
    },
    #[error("failed to read ABI artifact {path}: {source}")]
    ReadArtifact {
        path: PathBuf,
        source: std::io::Error,
    },
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
    pub resolved_artifact: Option<PathBuf>,
}

#[derive(Clone, Default)]
pub struct PluginCatalog {
    pub records: Vec<PluginRecord>,
}

#[derive(Clone, Default)]
pub struct BuiltinRegistry {
    manifests: BTreeMap<String, PluginManifest>,
}

#[derive(Clone, Default)]
pub struct BuiltinSelection {
    pub records: Vec<PluginRecord>,
}

impl BuiltinRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_manifest(&mut self, manifest: PluginManifest) {
        self.manifests.insert(manifest.plugin_id.clone(), manifest);
    }

    pub fn load_requested(&self, requested: &[String]) -> PluginLoaderResult<BuiltinSelection> {
        let mut records = Vec::new();
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
                resolved_artifact: None,
            });
        }
        Ok(BuiltinSelection { records })
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
                if matches!(record.manifest.artifact.artifact_type, ArtifactType::Native) {
                    return Err(PluginLoaderError::DynamicNative(
                        record.manifest.plugin_id.clone(),
                    ));
                }
                let resolved_artifact =
                    if matches!(record.manifest.artifact.artifact_type, ArtifactType::Abi) {
                        Some(validate_abi_artifact(&manifest_path, &record.manifest)?)
                    } else {
                        None
                    };
                records.push(PluginRecord {
                    manifest_path,
                    manifest: record.manifest,
                    runtime: record.runtime,
                    enabled,
                    resolved_artifact,
                });
            }
        }
        records.sort_by(|a, b| a.manifest.plugin_id.cmp(&b.manifest.plugin_id));
        for pair in records.windows(2) {
            if pair[0].manifest.plugin_id == pair[1].manifest.plugin_id {
                return Err(PluginLoaderError::DuplicatePlugin(
                    pair[0].manifest.plugin_id.clone(),
                ));
            }
        }
        Ok(Self { records })
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

fn validate_abi_artifact(
    manifest_path: &Path,
    manifest: &PluginManifest,
) -> PluginLoaderResult<PathBuf> {
    let plugin_id = manifest.plugin_id.clone();
    let declared = Path::new(&manifest.artifact.path);
    if declared.is_absolute() || manifest.artifact.path.trim().is_empty() {
        return Err(PluginLoaderError::InvalidArtifactPath {
            plugin_id,
            detail: "path must be non-empty and relative to plugin.toml".into(),
        });
    }
    let plugin_dir = manifest_path
        .parent()
        .expect("plugin.toml has a parent directory")
        .canonicalize()
        .map_err(|source| PluginLoaderError::ReadArtifact {
            path: manifest_path.to_path_buf(),
            source,
        })?;
    let candidate = plugin_dir.join(declared);
    if !candidate.is_file() {
        return Err(PluginLoaderError::MissingArtifact {
            plugin_id,
            path: candidate,
        });
    }
    let resolved = candidate
        .canonicalize()
        .map_err(|source| PluginLoaderError::ReadArtifact {
            path: candidate.clone(),
            source,
        })?;
    if !resolved.starts_with(&plugin_dir) {
        return Err(PluginLoaderError::InvalidArtifactPath {
            plugin_id,
            detail: "resolved path escapes the plugin directory".into(),
        });
    }
    if resolved.extension().and_then(|value| value.to_str()) != Some(platform_library_extension()) {
        return Err(PluginLoaderError::InvalidArtifactExtension {
            plugin_id,
            path: resolved,
        });
    }
    let expected = manifest.artifact.sha256.as_str();
    let Some(expected_hex) = expected.strip_prefix("sha256:") else {
        return Err(PluginLoaderError::InvalidArtifactHash { plugin_id });
    };
    if expected_hex.len() != 64
        || !expected_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PluginLoaderError::InvalidArtifactHash { plugin_id });
    }
    let bytes = fs::read(&resolved).map_err(|source| PluginLoaderError::ReadArtifact {
        path: resolved.clone(),
        source,
    })?;
    let actual = format!("sha256:{:x}", Sha256::digest(bytes));
    if actual != expected {
        return Err(PluginLoaderError::ArtifactHashMismatch {
            plugin_id,
            expected: expected.into(),
            actual,
        });
    }
    Ok(resolved)
}

fn platform_library_extension() -> &'static str {
    if cfg!(target_os = "windows") {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
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

#[cfg(test)]
mod tests {
    use super::*;
    use mutsuki_runtime_contracts::{
        LifecyclePolicy, PermissionGrant, PluginArtifact, PluginProvides,
    };
    use tempfile::tempdir;

    #[test]
    fn scan_resolves_and_verifies_abi_artifact() {
        let root = tempdir().unwrap();
        let plugin_dir = root.path().join("installed").join("abi-test");
        fs::create_dir_all(&plugin_dir).unwrap();
        let artifact = plugin_dir.join(format!("abi_test.{}", platform_library_extension()));
        fs::write(&artifact, b"fixture-library").unwrap();
        let hash = format!("sha256:{:x}", Sha256::digest(b"fixture-library"));
        write_plugin(
            &plugin_dir,
            manifest(
                "test.abi",
                ArtifactType::Abi,
                artifact.file_name().unwrap().to_string_lossy().as_ref(),
                &hash,
            ),
        );

        let catalog = PluginCatalog::scan(
            &[root.path().join("installed")],
            &root.path().join("disabled"),
            BuiltinSelection::default(),
        )
        .unwrap();

        assert_eq!(catalog.records.len(), 1);
        assert_eq!(
            catalog.records[0].resolved_artifact.as_deref(),
            Some(artifact.canonicalize().unwrap().as_path())
        );
    }

    #[test]
    fn scan_rejects_hash_mismatch_and_dynamic_native() {
        let root = tempdir().unwrap();
        let installed = root.path().join("installed");
        let abi_dir = installed.join("abi-test");
        fs::create_dir_all(&abi_dir).unwrap();
        let artifact = abi_dir.join(format!("abi_test.{}", platform_library_extension()));
        fs::write(&artifact, b"fixture-library").unwrap();
        write_plugin(
            &abi_dir,
            manifest(
                "test.abi",
                ArtifactType::Abi,
                artifact.file_name().unwrap().to_string_lossy().as_ref(),
                &format!("sha256:{}", "0".repeat(64)),
            ),
        );
        assert!(matches!(
            PluginCatalog::scan(
                std::slice::from_ref(&installed),
                &root.path().join("disabled"),
                BuiltinSelection::default(),
            ),
            Err(PluginLoaderError::ArtifactHashMismatch { .. })
        ));

        fs::remove_dir_all(&abi_dir).unwrap();
        let native_dir = installed.join("native-test");
        fs::create_dir_all(&native_dir).unwrap();
        write_plugin(
            &native_dir,
            manifest(
                "test.native",
                ArtifactType::Native,
                "native",
                "sha256:native",
            ),
        );
        assert!(matches!(
            PluginCatalog::scan(
                &[installed],
                &root.path().join("disabled"),
                BuiltinSelection::default(),
            ),
            Err(PluginLoaderError::DynamicNative(id)) if id == "test.native"
        ));
    }

    fn write_plugin(dir: &Path, manifest: PluginManifest) {
        fs::write(
            dir.join("plugin.toml"),
            toml::to_string(&PluginToml {
                manifest,
                runtime: None,
                enabled: Some(true),
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn manifest(
        plugin_id: &str,
        artifact_type: ArtifactType,
        path: &str,
        sha256: &str,
    ) -> PluginManifest {
        PluginManifest {
            plugin_id: plugin_id.into(),
            version: "0.1.0".into(),
            api_version: "mutsuki-plugin-v1".into(),
            artifact: PluginArtifact {
                artifact_type,
                path: path.into(),
                sha256: sha256.into(),
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
