use std::fs;
use std::path::{Path, PathBuf};

use mutsuki_service_config::ServiceConfig;
use mutsuki_service_plugin_loader::PluginRecord;

use crate::{ServiceRuntimeError, ServiceRuntimeResult};

pub(super) fn stage_artifact(
    config: &ServiceConfig,
    record: &PluginRecord,
    source: &Path,
) -> ServiceRuntimeResult<PathBuf> {
    let hash = record
        .manifest
        .artifact
        .sha256
        .strip_prefix("sha256:")
        .expect("plugin loader validated sha256");
    let dir = config
        .service
        .run_dir
        .join("abi")
        .join(&record.manifest.plugin_id)
        .join(hash);
    fs::create_dir_all(&dir).map_err(|error| ServiceRuntimeError::AbiPlugin {
        plugin_id: record.manifest.plugin_id.clone(),
        detail: format!("create ABI staging directory: {error}"),
    })?;
    let target = dir.join(source.file_name().expect("artifact has a file name"));
    if !target.is_file() {
        fs::copy(source, &target).map_err(|error| ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: format!("stage ABI artifact: {error}"),
        })?;
    }
    Ok(target)
}
