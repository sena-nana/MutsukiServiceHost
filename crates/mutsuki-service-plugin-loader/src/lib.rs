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
#[serde(deny_unknown_fields)]
pub struct PluginToml {
    pub manifest: PluginManifest,
    #[serde(default)]
    pub runtime: Option<ExternalRuntimeSpec>,
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
    pub resolved_artifact: Option<PathBuf>,
}

#[derive(Clone, Default)]
pub struct PluginInventory {
    pub records: Vec<PluginRecord>,
    pub diagnostics: Vec<PluginDiagnostic>,
}

#[derive(Clone, Default)]
pub struct PluginCatalog {
    /// Artifacts selected by the business configuration and Host deployment resolver.
    pub records: Vec<PluginRecord>,
    /// Every valid artifact discovered for management inventory.
    pub candidates: Vec<PluginRecord>,
    pub diagnostics: Vec<PluginDiagnostic>,
}

impl PluginCatalog {
    pub fn resolved(inventory: PluginInventory, records: Vec<PluginRecord>) -> Self {
        Self {
            records,
            candidates: inventory.records,
            diagnostics: inventory.diagnostics,
        }
    }

    pub fn external_records(&self) -> impl Iterator<Item = &PluginRecord> {
        self.records.iter().filter(|record| {
            matches!(
                record.manifest.artifact.artifact_type,
                ArtifactType::Process | ArtifactType::Python
            )
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PluginDiagnostic {
    pub manifest_path: PathBuf,
    pub plugin_id: Option<String>,
    pub deployment: Option<PluginDeploymentKind>,
    pub detail: String,
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

    pub fn load_all(&self) -> PluginLoaderResult<BuiltinSelection> {
        let mut records = Vec::new();
        for manifest in self.manifests.values() {
            validate_manifest(manifest, None)?;
            records.push(PluginRecord {
                manifest_path: PathBuf::from("<builtin>"),
                manifest: manifest.clone(),
                runtime: None,
                resolved_artifact: None,
            });
        }
        Ok(BuiltinSelection { records })
    }
}

impl PluginInventory {
    pub fn scan(
        dynamic_dirs: &[PathBuf],
        disabled_dir: &Path,
        builtin: BuiltinSelection,
    ) -> PluginLoaderResult<Self> {
        let mut records = builtin.records;
        let mut diagnostics = Vec::new();
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
                let record = match read_plugin_manifest(&manifest_path) {
                    Ok(record) => record,
                    Err(error) => {
                        diagnostics.push(PluginDiagnostic {
                            manifest_path,
                            plugin_id: None,
                            deployment: None,
                            detail: error.to_string(),
                        });
                        continue;
                    }
                };
                let plugin_id = record.manifest.plugin_id.clone();
                let deployment = PluginDeploymentKind::default_for_artifact(
                    &record.manifest.artifact.artifact_type,
                );
                if disabled_plugins.contains(&plugin_id) {
                    diagnostics.push(PluginDiagnostic {
                        manifest_path,
                        plugin_id: Some(plugin_id),
                        deployment: Some(deployment),
                        detail: "artifact is quarantined by the Host".into(),
                    });
                    continue;
                }
                if let Err(error) = validate_manifest(&record.manifest, record.runtime.as_ref()) {
                    diagnostics.push(PluginDiagnostic {
                        manifest_path,
                        plugin_id: Some(plugin_id),
                        deployment: Some(deployment),
                        detail: error.to_string(),
                    });
                    continue;
                }
                if matches!(record.manifest.artifact.artifact_type, ArtifactType::Native) {
                    diagnostics.push(PluginDiagnostic {
                        manifest_path,
                        plugin_id: Some(plugin_id.clone()),
                        deployment: Some(deployment),
                        detail: PluginLoaderError::DynamicNative(plugin_id).to_string(),
                    });
                    continue;
                }
                let resolved_artifact =
                    if matches!(record.manifest.artifact.artifact_type, ArtifactType::Abi) {
                        match validate_abi_artifact(&manifest_path, &record.manifest) {
                            Ok(path) => Some(path),
                            Err(error) => {
                                diagnostics.push(PluginDiagnostic {
                                    manifest_path,
                                    plugin_id: Some(plugin_id),
                                    deployment: Some(deployment),
                                    detail: error.to_string(),
                                });
                                continue;
                            }
                        }
                    } else {
                        None
                    };
                records.push(PluginRecord {
                    manifest_path,
                    manifest: record.manifest,
                    runtime: record.runtime,
                    resolved_artifact,
                });
            }
        }
        records.sort_by(|a, b| {
            candidate_key(a)
                .cmp(&candidate_key(b))
                .then_with(|| a.manifest_path.cmp(&b.manifest_path))
        });
        let mut unique = Vec::new();
        for group in records.chunk_by(|a, b| candidate_key(a) == candidate_key(b)) {
            if group.len() == 1 {
                unique.push(group[0].clone());
            } else {
                let first = &group[0];
                diagnostics.push(PluginDiagnostic {
                    manifest_path: first.manifest_path.clone(),
                    plugin_id: Some(first.manifest.plugin_id.clone()),
                    deployment: Some(PluginDeploymentKind::default_for_artifact(
                        &first.manifest.artifact.artifact_type,
                    )),
                    detail: format!(
                        "multiple {:?} artifacts are installed for plugin {}",
                        PluginDeploymentKind::default_for_artifact(
                            &first.manifest.artifact.artifact_type
                        ),
                        first.manifest.plugin_id
                    ),
                });
            }
        }
        Ok(Self {
            records: unique,
            diagnostics,
        })
    }
}

fn candidate_key(record: &PluginRecord) -> (String, String) {
    (
        record.manifest.plugin_id.clone(),
        format!(
            "{:?}",
            PluginDeploymentKind::default_for_artifact(&record.manifest.artifact.artifact_type)
        ),
    )
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

        let catalog = PluginInventory::scan(
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
    fn scan_reports_hash_mismatch_and_dynamic_native_as_unavailable_inventory() {
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
        let inventory = PluginInventory::scan(
            std::slice::from_ref(&installed),
            &root.path().join("disabled"),
            BuiltinSelection::default(),
        )
        .unwrap();
        assert!(inventory.records.is_empty());
        assert!(inventory.diagnostics[0].detail.contains("hash mismatch"));

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
        let inventory = PluginInventory::scan(
            &[installed],
            &root.path().join("disabled"),
            BuiltinSelection::default(),
        )
        .unwrap();
        assert!(inventory.records.is_empty());
        assert!(
            inventory.diagnostics[0]
                .detail
                .contains("not linked into this build")
        );
    }

    #[test]
    fn legacy_manifest_enabled_field_is_rejected_as_inventory_diagnostic() {
        let root = tempdir().unwrap();
        let plugin_dir = root.path().join("installed").join("legacy");
        fs::create_dir_all(&plugin_dir).unwrap();
        fs::write(
            plugin_dir.join("plugin.toml"),
            "enabled = true\n[manifest]\nplugin_id = \"legacy\"",
        )
        .unwrap();
        let inventory = PluginInventory::scan(
            &[root.path().join("installed")],
            &root.path().join("disabled"),
            BuiltinSelection::default(),
        )
        .unwrap();
        assert!(inventory.records.is_empty());
        assert!(
            inventory.diagnostics[0]
                .detail
                .contains("unknown field `enabled`")
        );
    }

    fn write_plugin(dir: &Path, manifest: PluginManifest) {
        fs::write(
            dir.join("plugin.toml"),
            toml::to_string(&PluginToml {
                manifest,
                runtime: None,
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
