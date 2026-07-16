use std::collections::BTreeMap;
use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use mutsuki_runtime_contracts::{
    PluginDeploymentKind, ReadPlan, RuntimeProfile, RuntimeProfileMode, Task, TaskStatus,
};
use mutsuki_runtime_host::RuntimeBootstrapper;
use mutsuki_runtime_sdk::ResourceProviderGateway;
use mutsuki_service_config::ServiceConfig;
use mutsuki_service_plugin_loader::PluginRecord;
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use super::load_abi_plugin;
use crate::DeferredRuntimeClient;

#[tokio::test]
async fn real_cdylib_loads_runner_and_resource_provider() {
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

    let loading = Arc::new(AtomicBool::new(true));
    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let heartbeat = {
        let loading = loading.clone();
        let heartbeat_count = heartbeat_count.clone();
        tokio::spawn(async move {
            while loading.load(Ordering::Acquire) {
                heartbeat_count.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        })
    };
    let plugin = load_abi_plugin(
        record,
        config,
        Arc::new(DeferredRuntimeClient::default()),
        json!({"fixture": true}),
    )
    .await
    .unwrap();
    loading.store(false, Ordering::Release);
    heartbeat.await.unwrap();
    assert!(
        heartbeat_count.load(Ordering::Relaxed) > 0,
        "ABI staging, dlopen and handshake must yield the async runtime"
    );
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
            plugin_deployments: [(manifest.plugin_id.clone(), PluginDeploymentKind::Abi)].into(),
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

#[tokio::test]
async fn real_cdylib_keeps_manifest_selected_abi_v1_compatibility() {
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
    let sha256 = format!("sha256:{:x}", Sha256::digest(fs::read(&artifact).unwrap()));
    let manifest = mutsuki_service_abi_fixture::fixture_manifest_v1(file_name, &sha256);
    let root = tempdir().unwrap();
    let mut config = ServiceConfig::default();
    config.service.run_dir = root.path().join("run");
    let plugin = load_abi_plugin(
        PluginRecord {
            manifest_path: root.path().join("plugin.toml"),
            manifest,
            runtime: None,
            resolved_artifact: Some(artifact),
        },
        config,
        Arc::new(DeferredRuntimeClient::default()),
        json!({"fixture": true}),
    )
    .await
    .unwrap();
    assert_eq!(plugin.runners.len(), 1);
    assert_eq!(plugin.resource_providers.len(), 1);
}
