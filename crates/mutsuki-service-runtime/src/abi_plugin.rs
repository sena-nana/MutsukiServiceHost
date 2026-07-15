use std::ffi::c_void;
use std::fs;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use libloading::Library;
use mutsuki_runtime_contracts::{PluginManifest, RuntimeError, ScalarValue};
use mutsuki_runtime_core::{RuntimeFailure, RuntimeResult};
use mutsuki_runtime_host::{JsonRequestTransport, TransportJsonlRunner, TransportResourceProvider};
use mutsuki_runtime_sdk::abi::{
    ABI_BRIDGE_ID, ABI_CODEC_ID, ABI_ENTRY_SYMBOL, ABI_TRANSPORT_VERSION, AbiBuffer, AbiCallResult,
    AbiEntryV1, AbiHostV1, AbiPluginV1,
};
use mutsuki_runtime_sdk::{
    LoadedPlugin, RuntimeBootstrapperResourceProvider, dispatch_host_request,
};
use mutsuki_service_config::ServiceConfig;
use mutsuki_service_plugin_loader::PluginRecord;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{DeferredRuntimeClient, ServiceRuntimeError, ServiceRuntimeResult};

#[derive(Deserialize)]
struct AbiHandshake {
    transport_version: u32,
    codec_id: String,
    bridge_id: String,
    manifest: PluginManifest,
    resource_provider_ids: Vec<String>,
}

struct HostCallbackContext {
    runtime: Arc<DeferredRuntimeClient>,
}

pub(crate) struct AbiConnection {
    plugin_id: String,
    api: AbiPluginV1,
    _library: Library,
    _host_context: Box<HostCallbackContext>,
    request_lock: Mutex<()>,
    next_request: AtomicU64,
}

// The Core ABI contract serializes guest calls through request_lock and the guest shim serializes
// its mutable plugin state. The library and callback context live for the entire connection.
unsafe impl Send for AbiConnection {}
unsafe impl Sync for AbiConnection {}

impl Drop for AbiConnection {
    fn drop(&mut self) {
        if let Some(close) = self.api.close {
            // SAFETY: context was returned by the paired entry and is closed exactly once here.
            unsafe { close(self.api.context) };
        }
    }
}

impl JsonRequestTransport for AbiConnection {
    fn request(&self, method: &str, params: Value) -> RuntimeResult<Value> {
        let _guard = self
            .request_lock
            .lock()
            .expect("ABI request mutex poisoned");
        let id = format!(
            "abi-{}-{}",
            self.plugin_id,
            self.next_request.fetch_add(1, Ordering::Relaxed) + 1
        );
        let bytes = serde_json::to_vec(&json!({
            "id": id,
            "method": method,
            "params": params,
        }))
        .map_err(|error| abi_failure(&self.plugin_id, "abi.encode", error.to_string()))?;
        let request = self.api.request.ok_or_else(|| {
            abi_failure(
                &self.plugin_id,
                "abi.entry_invalid",
                "request callback is missing",
            )
        })?;
        // SAFETY: plugin context and callback remain valid while this connection owns the library.
        let result = unsafe { request(self.api.context, bytes.as_ptr(), bytes.len()) };
        // SAFETY: plugin owns the response until its paired release callback is invoked below.
        let response = unsafe { result.payload.as_slice() }.to_vec();
        if let Some(release) = self.api.release {
            // SAFETY: release is paired with the plugin-owned response buffer.
            unsafe { release(result.payload) };
        }
        if result.status != 0 {
            return Err(abi_failure(
                &self.plugin_id,
                "abi.request_failed",
                String::from_utf8_lossy(&response),
            ));
        }
        decode_response(&self.plugin_id, &id, &response)
    }
}

pub(crate) fn load_abi_plugin(
    record: &PluginRecord,
    config: &ServiceConfig,
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
    let staged = stage_artifact(config, record, source)?;
    let host_context = Box::new(HostCallbackContext { runtime });
    let host = AbiHostV1 {
        context: (&*host_context as *const HostCallbackContext)
            .cast_mut()
            .cast::<c_void>(),
        request: Some(host_request),
        release: Some(host_release),
    };
    // SAFETY: artifact hash/path/extension were validated before loading. The library remains
    // owned by AbiConnection for every callback and adapter lifetime.
    let library =
        unsafe { Library::new(&staged) }.map_err(|error| ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: error.to_string(),
        })?;
    // SAFETY: the symbol name and function layout are the versioned Core ABI contract.
    let entry: AbiEntryV1 = unsafe {
        *library
            .get::<AbiEntryV1>(ABI_ENTRY_SYMBOL)
            .map_err(|error| ServiceRuntimeError::AbiPlugin {
                plugin_id: record.manifest.plugin_id.clone(),
                detail: format!(
                    "missing {}: {error}",
                    String::from_utf8_lossy(ABI_ENTRY_SYMBOL)
                ),
            })?
    };
    // SAFETY: host callbacks and context remain valid in the resulting connection.
    let api = unsafe { entry(host) };
    if api.transport_version != ABI_TRANSPORT_VERSION
        || api.context.is_null()
        || api.request.is_none()
        || api.release.is_none()
        || api.close.is_none()
    {
        return Err(ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: format!(
                "invalid entry surface or transport version {}, expected {}",
                api.transport_version, ABI_TRANSPORT_VERSION
            ),
        });
    }
    let connection = Arc::new(AbiConnection {
        plugin_id: record.manifest.plugin_id.clone(),
        api,
        _library: library,
        _host_context: host_context,
        request_lock: Mutex::new(()),
        next_request: AtomicU64::new(0),
    });
    let handshake: AbiHandshake = serde_json::from_value(
        connection.request("plugin.initialize", json!({ "config": plugin_config }))?,
    )
    .map_err(|error| ServiceRuntimeError::AbiPlugin {
        plugin_id: record.manifest.plugin_id.clone(),
        detail: format!("invalid handshake: {error}"),
    })?;
    validate_handshake(record, &handshake)?;

    let runners = record
        .manifest
        .provides
        .runners
        .iter()
        .cloned()
        .map(|descriptor| {
            Box::new(TransportJsonlRunner::new(descriptor, connection.clone()))
                as Box<dyn mutsuki_runtime_core::Runner>
        })
        .collect();
    let resource_providers = handshake
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
        manifest: record.manifest.clone(),
        runners,
        host_services: Vec::new(),
        resource_providers,
    })
}

fn stage_artifact(
    config: &ServiceConfig,
    record: &PluginRecord,
    source: &std::path::Path,
) -> ServiceRuntimeResult<std::path::PathBuf> {
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

fn validate_handshake(record: &PluginRecord, handshake: &AbiHandshake) -> ServiceRuntimeResult<()> {
    if handshake.transport_version != ABI_TRANSPORT_VERSION
        || handshake.codec_id != ABI_CODEC_ID
        || handshake.bridge_id != ABI_BRIDGE_ID
    {
        return Err(ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: "handshake transport, codec or bridge mismatch".into(),
        });
    }
    let mut guest_manifest = handshake.manifest.clone();
    guest_manifest.artifact = record.manifest.artifact.clone();
    if guest_manifest != record.manifest {
        return Err(ServiceRuntimeError::AbiPlugin {
            plugin_id: record.manifest.plugin_id.clone(),
            detail: "plugin.toml capability manifest differs from guest handshake".into(),
        });
    }
    let mut expected = record.manifest.provides.resource_providers.clone();
    let mut actual = handshake.resource_provider_ids.clone();
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

unsafe extern "C" fn host_request(
    context: *mut c_void,
    request: *const u8,
    request_len: usize,
) -> AbiCallResult {
    if context.is_null() || (request.is_null() && request_len != 0) {
        return AbiCallResult::failed(b"invalid host callback pointers".to_vec());
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: context is owned by AbiConnection and request is valid for this callback.
        let context = unsafe { &*context.cast::<HostCallbackContext>() };
        // SAFETY: pointer/length are validated above and borrowed only for this call.
        let request = unsafe { std::slice::from_raw_parts(request, request_len) };
        dispatch_host_request(context.runtime.as_ref(), context.runtime.as_ref(), request)
    }));
    match result {
        Ok(response) => AbiCallResult::ok(response),
        Err(_) => AbiCallResult::failed(b"host ABI callback panicked".to_vec()),
    }
}

unsafe extern "C" fn host_release(buffer: AbiBuffer) {
    if buffer.ptr.is_null() || buffer.len == 0 {
        return;
    }
    let slice = ptr::slice_from_raw_parts_mut(buffer.ptr, buffer.len);
    // SAFETY: host callback responses are allocated as Box<[u8]> by AbiBuffer::from_bytes.
    unsafe { drop(Box::from_raw(slice)) };
}

fn decode_response(plugin_id: &str, id: &str, bytes: &[u8]) -> RuntimeResult<Value> {
    let response: Value = serde_json::from_slice(bytes)
        .map_err(|error| abi_failure(plugin_id, "abi.decode", error.to_string()))?;
    if response.get("id") != Some(&Value::String(id.into())) {
        return Err(abi_failure(plugin_id, "abi.response_id_mismatch", id));
    }
    match response.get("ok").and_then(Value::as_bool) {
        Some(true) => Ok(response.get("result").cloned().unwrap_or(Value::Null)),
        Some(false) => {
            let error =
                serde_json::from_value(response.get("error").cloned().unwrap_or(Value::Null))
                    .map_err(|error| abi_failure(plugin_id, "abi.decode", error.to_string()))?;
            Err(RuntimeFailure::new(error))
        }
        None => Err(abi_failure(
            plugin_id,
            "abi.response_invalid",
            "missing ok flag",
        )),
    }
}

fn abi_failure(
    plugin_id: &str,
    route: impl Into<String>,
    detail: impl Into<String>,
) -> RuntimeFailure {
    let mut error = RuntimeError::new(
        mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
        format!("plugin:{plugin_id}"),
        route,
    );
    error
        .evidence
        .insert("detail".into(), ScalarValue::String(detail.into()));
    RuntimeFailure::new(error)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::process::Command;

    use mutsuki_runtime_contracts::{
        PluginDeploymentKind, ReadPlan, RuntimeProfile, RuntimeProfileMode, Task, TaskStatus,
    };
    use mutsuki_runtime_host::RuntimeBootstrapper;
    use mutsuki_runtime_sdk::ResourceProviderGateway;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn real_cdylib_loads_runner_and_resource_provider() {
        let fixture_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("fixtures")
            .join("abi-plugin");
        let status = Command::new(env!("CARGO"))
            .args(["build", "--manifest-path"])
            .arg(fixture_root.join("Cargo.toml"))
            .status()
            .expect("build ABI fixture");
        assert!(status.success());
        let file_name = if cfg!(target_os = "windows") {
            "mutsuki_service_abi_fixture.dll"
        } else if cfg!(target_os = "macos") {
            "libmutsuki_service_abi_fixture.dylib"
        } else {
            "libmutsuki_service_abi_fixture.so"
        };
        let artifact = fixture_root
            .join("..")
            .join("..")
            .join("target")
            .join("debug")
            .join(file_name);
        assert!(
            artifact.is_file(),
            "fixture artifact: {}",
            artifact.display()
        );
        let bytes = fs::read(&artifact).unwrap();
        let sha256 = format!("sha256:{:x}", Sha256::digest(bytes));
        let manifest = mutsuki_service_abi_fixture::fixture_manifest(file_name, &sha256);
        let root = tempdir().unwrap();
        let mut config = ServiceConfig::default();
        config.service.run_dir = root.path().join("run");
        let record = PluginRecord {
            manifest_path: root.path().join("plugin.toml"),
            manifest: manifest.clone(),
            runtime: None,
            resolved_artifact: Some(artifact),
        };

        let plugin = load_abi_plugin(
            &record,
            &config,
            Arc::new(DeferredRuntimeClient::default()),
            json!({"fixture": true}),
        )
        .unwrap();
        assert_eq!(plugin.runners.len(), 1);
        assert_eq!(plugin.resource_providers.len(), 1);
        let provider: &dyn ResourceProviderGateway = plugin.resource_providers[0].provider.as_ref();
        let resource = provider
            .create_blob_resource("fixture.v1", b"input".to_vec())
            .unwrap();
        let bytes = provider
            .collect_read_plan(&ReadPlan {
                plan_id: "fixture-read".into(),
                resource,
                operation: "collect".into(),
                args: json!({}),
            })
            .unwrap();
        assert_eq!(bytes, b"fixture-resource");

        let mut bootstrapper = RuntimeBootstrapper::new();
        bootstrapper.register_loaded_plugin(plugin);
        let mut runtime = bootstrapper
            .into_runtime(RuntimeProfile {
                profile_id: "abi-fixture".into(),
                mode: RuntimeProfileMode::ExtensibleRuntime,
                enabled_plugins: vec![manifest.plugin_id.clone()],
                bindings: BTreeMap::new(),
                plugin_deployments: [(manifest.plugin_id.clone(), PluginDeploymentKind::Abi)]
                    .into(),
                observability: Default::default(),
                allow_dynamic_registration: false,
                allow_hot_reload: true,
            })
            .unwrap();
        runtime
            .submit_task(Task::new(
                "abi-fixture-task",
                "mutsuki.test.abi.echo",
                json!({ "message": "hello" }),
            ))
            .unwrap();
        runtime.run_until_idle(4).unwrap();
        assert_eq!(
            runtime.tasks().get("abi-fixture-task").unwrap().status,
            TaskStatus::Completed
        );
    }
}
