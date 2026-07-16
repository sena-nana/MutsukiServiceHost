mod callbacks;
mod connection;
mod staging;

use std::sync::Arc;

use mutsuki_runtime_host::{TransportResourceProvider, TransportRunner};
use mutsuki_runtime_sdk::{LoadedPlugin, RuntimeBootstrapperResourceProvider};
use mutsuki_runtime_wire::InitializedPlugin;
use mutsuki_service_config::ServiceConfig;
use mutsuki_service_plugin_loader::PluginRecord;
use serde_json::Value;

use self::connection::open_connection;
use self::staging::stage_artifact;
use super::{DeferredRuntimeClient, ServiceRuntimeError, ServiceRuntimeResult};

pub(crate) async fn load_abi_plugin(
    record: PluginRecord,
    config: ServiceConfig,
    runtime: Arc<DeferredRuntimeClient>,
    plugin_config: Value,
) -> ServiceRuntimeResult<LoadedPlugin> {
    let plugin_id = record.manifest.plugin_id.clone();
    tokio::task::spawn_blocking(move || {
        load_abi_plugin_blocking(record, config, runtime, plugin_config)
    })
    .await
    .map_err(|error| ServiceRuntimeError::AbiPlugin {
        plugin_id,
        detail: format!("ABI loading task failed: {error}"),
    })?
}

fn load_abi_plugin_blocking(
    record: PluginRecord,
    config: ServiceConfig,
    runtime: Arc<DeferredRuntimeClient>,
    plugin_config: Value,
) -> ServiceRuntimeResult<LoadedPlugin> {
    let source =
        record
            .resolved_artifact
            .as_ref()
            .ok_or_else(|| ServiceRuntimeError::AbiPlugin {
                plugin_id: record.manifest.plugin_id.clone(),
                detail: "validated artifact path is missing".into(),
            })?;
    let staged = stage_artifact(&config, &record, source)?;
    let connection = open_connection(&record.manifest, &staged, runtime)?;
    let hello = connection.hello();
    let ack = connection
        .initialize(Some(plugin_config))
        .map_err(ServiceRuntimeError::Core)?;
    ack.validate_for(&hello)
        .map_err(|error| ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: format!("invalid Wire handshake: {error}"),
        })?;
    let initialized = ack.plugin.ok_or_else(|| ServiceRuntimeError::AbiPlugin {
        plugin_id: record.manifest.plugin_id.clone(),
        detail: "Wire handshake omitted initialized plugin surface".into(),
    })?;
    validate_initialized_plugin(&record, &initialized)?;

    let runners = record
        .manifest
        .provides
        .runners
        .iter()
        .cloned()
        .map(|descriptor| {
            Box::new(TransportRunner::new(descriptor, connection.clone()))
                as Box<dyn mutsuki_runtime_core::Runner>
        })
        .collect();
    let resource_providers = initialized
        .resource_provider_ids
        .into_iter()
        .map(|provider_id| RuntimeBootstrapperResourceProvider {
            provider_id: provider_id.clone(),
            provider: Arc::new(TransportResourceProvider::new(
                provider_id,
                connection.clone(),
            )),
        })
        .collect();
    Ok(LoadedPlugin {
        manifest: record.manifest,
        runners,
        host_services: Vec::new(),
        resource_providers,
    })
}

fn validate_initialized_plugin(
    record: &PluginRecord,
    initialized: &InitializedPlugin,
) -> ServiceRuntimeResult<()> {
    let mut guest_manifest = initialized.manifest.clone();
    guest_manifest.artifact = record.manifest.artifact.clone();
    if guest_manifest != record.manifest {
        return Err(ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: "plugin.toml capability manifest differs from guest handshake".into(),
        });
    }
    let mut expected = record.manifest.provides.resource_providers.clone();
    let mut actual = initialized.resource_provider_ids.clone();
    expected.sort();
    actual.sort();
    if expected != actual {
        return Err(ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: "resource provider ids differ from guest handshake".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
