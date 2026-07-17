use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use mutsuki_runtime_contracts::{ArtifactType, PluginArtifact};
use mutsuki_service_benchmarks::{FixtureRunner, PLUGIN_ID, fixture_manifest_for};
use mutsuki_service_config::{ConfiguredPluginSelection, ServiceConfig};
use mutsuki_service_runtime::ServiceRuntimeBuilder;
use serde_json::json;

struct Args {
    root: PathBuf,
    ready: PathBuf,
    stop: PathBuf,
    token: String,
    instance: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let mut config = ServiceConfig::default();
    config.service.profile = "distributed-benchmark".into();
    config.service.instance_id.clone_from(&args.instance);
    config.service.home_dir = args.root.clone();
    config.service.data_dir = args.root.join("data");
    config.service.log_dir = args.root.join("logs");
    config.service.run_dir = args.root.join("run");
    config.service.plugin_dir = args.root.join("plugins");
    config.plugins.dynamic_dirs = vec![args.root.join("installed")];
    config.plugins.disabled_dir = args.root.join("disabled");
    config.plugins.configured = vec![ConfiguredPluginSelection {
        id: PLUGIN_ID.into(),
        enabled: true,
        config: json!({"fixture": true, "benchmark_service_only": true}),
    }];
    config.ipc.name = format!("{}-{}", args.instance, std::process::id());
    config.ipc.token = Some(args.token.clone());
    config.observe.console = false;
    config.runners.graceful_shutdown_ms = 500;
    let manifest = fixture_manifest_for(PluginArtifact {
        artifact_type: ArtifactType::Native,
        path: "<builtin>".into(),
        sha256: "sha256:benchmark-service-builtin".into(),
    });
    let descriptor = manifest.provides.runners[0].clone();
    let runtime = ServiceRuntimeBuilder::new(config.clone())
        .register_builtin_plugin(manifest)
        .register_builtin_runner(move || Box::new(FixtureRunner::new(descriptor.clone())))
        .start()
        .await?;
    if let Some(parent) = args.ready.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(
        &args.ready,
        serde_json::to_vec(&json!({
            "endpoint": config.ipc_endpoint(),
            "token": args.token,
            "pid": std::process::id(),
        }))?,
    )?;
    while !args.stop.is_file() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    runtime.shutdown().await;
    Ok(())
}

fn parse_args() -> Result<Args, &'static str> {
    let mut values = std::env::args().skip(1);
    let root = values.next().map(PathBuf::from).ok_or("missing root")?;
    let ready = values
        .next()
        .map(PathBuf::from)
        .ok_or("missing ready path")?;
    let stop = values
        .next()
        .map(PathBuf::from)
        .ok_or("missing stop path")?;
    let token = values.next().ok_or("missing token")?;
    let instance = values.next().ok_or("missing instance")?;
    if values.next().is_some() {
        return Err("unexpected argument");
    }
    Ok(Args {
        root,
        ready,
        stop,
        token,
        instance,
    })
}
