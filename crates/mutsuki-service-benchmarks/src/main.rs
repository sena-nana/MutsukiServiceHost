use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use mutsuki_runtime_contracts::{ArtifactType, PluginArtifact, Task, TaskBatch};
use mutsuki_service_benchmarks::{FixtureRunner, PLUGIN_ID, fixture_manifest_for};
use mutsuki_service_config::{ConfiguredPluginSelection, ServiceConfig};
use mutsuki_service_control::{
    ControlMethod, TaskOutcomeView, TaskSubmitBatchParam, TaskSubmitBatchResponse,
};
use mutsuki_service_ipc::{ControlClient, ControlClientConfig};
use mutsuki_service_plugin_loader::{ExternalRuntimeSpec, PluginToml};
use mutsuki_service_runtime::{ServiceRuntime, ServiceRuntimeBuilder};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug)]
enum Deployment {
    Builtin,
    Abi,
    RustProcess,
}

impl Deployment {
    const ALL: [Self; 3] = [Self::Builtin, Self::Abi, Self::RustProcess];

    fn name(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Abi => "abi",
            Self::RustProcess => "rust-process-jsonl",
        }
    }
}

#[derive(Debug)]
struct Args {
    mode: String,
    warmup: usize,
    samples: usize,
    output: PathBuf,
}

#[derive(Debug)]
struct Sample {
    startup_ns: f64,
    health_ipc_ns: f64,
    echo_e2e_ns: f64,
    reload_ns: f64,
    shutdown_ns: f64,
}

struct PreparedRuntime {
    _root: TempDir,
    config: ServiceConfig,
    runtime: ServiceRuntime,
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let started = cpu_time::ProcessTime::now();
    let mut all_samples = BTreeMap::<String, Vec<Sample>>::new();
    let mut fixture_hashes = BTreeMap::new();
    let mut failures = Vec::new();

    for deployment in Deployment::ALL {
        for _ in 0..args.warmup {
            run_sample(deployment, false, &mut fixture_hashes, &mut failures)
                .await
                .unwrap_or_else(|error| panic!("{} warmup failed: {error}", deployment.name()));
        }
        let mut samples = Vec::with_capacity(args.samples);
        for index in 0..args.samples {
            samples.push(
                run_sample(deployment, index == 0, &mut fixture_hashes, &mut failures)
                    .await
                    .unwrap_or_else(|error| panic!("{} sample failed: {error}", deployment.name())),
            );
        }
        all_samples.insert(deployment.name().into(), samples);
    }

    let cpu_ns = started.elapsed().as_nanos() as f64;
    let report = build_report(&args, &all_samples, &fixture_hashes, &failures, cpu_ns);
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent).expect("create report directory");
    }
    fs::write(
        &args.output,
        serde_json::to_vec_pretty(&report).expect("serialize report"),
    )
    .expect("write benchmark report");
    println!("wrote {}", args.output.display());
    if !failures.is_empty() {
        panic!("fixture correctness failures: {failures:?}");
    }
}

async fn run_sample(
    deployment: Deployment,
    run_fixture_suite: bool,
    fixture_hashes: &mut BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> Result<Sample, String> {
    let startup = Instant::now();
    let prepared = start_runtime(deployment).await?;
    let startup_ns = startup.elapsed().as_nanos() as f64;
    let client = ControlClient::new(ControlClientConfig::from(&prepared.config));

    let health = Instant::now();
    checked_request(&client, ControlMethod::HealthCheck, Value::Null).await?;
    let health_ipc_ns = health.elapsed().as_nanos() as f64;

    let echo = Instant::now();
    let output = execute_fixture(
        &client,
        "runner.echo",
        json!({"message": "mutsuki", "sequence": 1}),
        "measured-echo",
    )
    .await?;
    let echo_e2e_ns = echo.elapsed().as_nanos() as f64;
    let echo_hash = canonical_hash(&output);
    if echo_hash != "e410c601945ccfb5b6f145d04ad4e2c8ae402ab6f96119acba08ba33349d67b3" {
        failures.push(format!("{} echo hash {echo_hash}", deployment.name()));
    }

    if run_fixture_suite {
        verify_fixtures(&client, deployment, fixture_hashes, failures).await?;
    }

    let reload = Instant::now();
    checked_request(&client, ControlMethod::PluginReload, Value::Null).await?;
    let reload_ns = reload.elapsed().as_nanos() as f64;

    let shutdown = Instant::now();
    prepared.runtime.shutdown().await;
    let shutdown_ns = shutdown.elapsed().as_nanos() as f64;
    Ok(Sample {
        startup_ns,
        health_ipc_ns,
        echo_e2e_ns,
        reload_ns,
        shutdown_ns,
    })
}

async fn start_runtime(deployment: Deployment) -> Result<PreparedRuntime, String> {
    let root = benchmark_tempdir()?;
    let mut config = ServiceConfig::default();
    config.service.profile = format!("benchmark-{}", deployment.name());
    config.service.instance_id = format!("benchmark-{}", deployment.name());
    config.service.home_dir = root.path().to_path_buf();
    config.service.data_dir = root.path().join("data");
    config.service.log_dir = root.path().join("logs");
    config.service.run_dir = root.path().join("run");
    config.service.plugin_dir = root.path().join("plugins");
    config.plugins.dynamic_dirs = vec![root.path().join("installed")];
    config.plugins.disabled_dir = root.path().join("disabled");
    config.plugins.configured = vec![ConfiguredPluginSelection {
        id: PLUGIN_ID.into(),
        enabled: true,
        config: json!({"fixture": true, "benchmark_runner_only": true}),
    }];
    config.ipc.name = format!("mb{}", std::process::id());
    config.ipc.token = Some("benchmark-control-token".into());
    config.observe.console = false;
    config.runners.graceful_shutdown_ms = 250;

    let runtime = match deployment {
        Deployment::Builtin => {
            let manifest = fixture_manifest_for(PluginArtifact {
                artifact_type: ArtifactType::Native,
                path: "<builtin>".into(),
                sha256: "sha256:benchmark-builtin".into(),
            });
            let descriptor = manifest.provides.runners[0].clone();
            ServiceRuntimeBuilder::new(config.clone())
                .register_builtin_plugin(manifest)
                .register_builtin_runner(move || Box::new(FixtureRunner::new(descriptor.clone())))
                .start()
                .await
                .map_err(|error| error.to_string())?
        }
        Deployment::Abi => {
            install_abi_plugin(&config)?;
            ServiceRuntime::start(config.clone())
                .await
                .map_err(|error| error.to_string())?
        }
        Deployment::RustProcess => {
            install_process_plugin(&config)?;
            ServiceRuntime::start(config.clone())
                .await
                .map_err(|error| error.to_string())?
        }
    };
    Ok(PreparedRuntime {
        _root: root,
        config,
        runtime,
    })
}

fn benchmark_tempdir() -> Result<TempDir, String> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("msh");
    #[cfg(unix)]
    {
        builder
            .tempdir_in("/tmp")
            .map_err(|error| error.to_string())
    }
    #[cfg(not(unix))]
    {
        builder.tempdir().map_err(|error| error.to_string())
    }
}

fn install_abi_plugin(config: &ServiceConfig) -> Result<(), String> {
    let source = current_binary_dir()?.join(platform_abi_name());
    if !source.is_file() {
        return Err(format!(
            "ABI fixture {} is missing; run the benchmark script so fixtures are built first",
            source.display()
        ));
    }
    let plugin_dir = config.plugins.dynamic_dirs[0].join("abi");
    fs::create_dir_all(&plugin_dir).map_err(|error| error.to_string())?;
    let artifact = plugin_dir.join(platform_abi_name());
    fs::copy(&source, &artifact).map_err(|error| error.to_string())?;
    let sha256 = format!("sha256:{:x}", Sha256::digest(fs::read(&artifact).unwrap()));
    let manifest = mutsuki_service_abi_fixture::benchmark_manifest(platform_abi_name(), &sha256);
    write_plugin(
        &plugin_dir,
        PluginToml {
            manifest,
            runtime: None,
        },
    )
}

fn install_process_plugin(config: &ServiceConfig) -> Result<(), String> {
    let process = current_binary_dir()?.join(platform_process_name());
    if !process.is_file() {
        return Err(format!(
            "Rust process fixture {} is missing; run the benchmark script so fixtures are built first",
            process.display()
        ));
    }
    let plugin_dir = config.plugins.dynamic_dirs[0].join("rust-process");
    fs::create_dir_all(&plugin_dir).map_err(|error| error.to_string())?;
    let manifest = fixture_manifest_for(PluginArtifact {
        artifact_type: ArtifactType::Process,
        path: platform_process_name().into(),
        sha256: "sha256:benchmark-process".into(),
    });
    write_plugin(
        &plugin_dir,
        PluginToml {
            manifest,
            runtime: Some(ExternalRuntimeSpec {
                command: process.to_string_lossy().into_owned(),
                args: Vec::new(),
                env: BTreeMap::new(),
                cwd: None,
                runner_link: "jsonl-stdio".into(),
            }),
        },
    )
}

fn write_plugin(directory: &Path, plugin: PluginToml) -> Result<(), String> {
    fs::write(
        directory.join("plugin.toml"),
        toml::to_string_pretty(&plugin).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

async fn verify_fixtures(
    client: &ControlClient,
    deployment: Deployment,
    hashes: &mut BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> Result<(), String> {
    let fixtures = [
        ("runner.noop", json!({}), Some(json!({"status": "ok"}))),
        (
            "runner.echo",
            json!({"message": "mutsuki", "sequence": 1}),
            Some(json!({"echo": {"message": "mutsuki", "sequence": 1}})),
        ),
        (
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64}),
            Some(
                json!({"checksum": mutsuki_service_benchmarks::calibrated_checksum(1_297_435_713)}),
            ),
        ),
        (
            "runner.wait",
            json!({"wake_key": "fixture-wake"}),
            Some(json!({"resumed": true})),
        ),
        (
            "runner.resource",
            json!({"resource_ref": "fixture-resource", "version": 1}),
            Some(json!({"resource_id": "fixture-resource", "version": 1})),
        ),
        ("runner.fault", json!({"fault": "error"}), None),
    ];
    for (index, (protocol, payload, expected)) in fixtures.into_iter().enumerate() {
        let task_id = format!("fixture-{}-{index}", deployment.name());
        match execute_fixture(client, protocol, payload, &task_id).await {
            Ok(output) if expected.as_ref() == Some(&output) => {
                hashes.insert(
                    format!("{}:{protocol}", deployment.name()),
                    canonical_hash(&output),
                );
            }
            Err(error) if expected.is_none() && error.contains("fixture.failure") => {
                hashes.insert(
                    format!("{}:{protocol}", deployment.name()),
                    canonical_hash(
                        &json!({"error": {"code": "fixture.failure", "retryable": false}}),
                    ),
                );
            }
            result => failures.push(format!(
                "{} {protocol} expected {expected:?}, got {result:?}",
                deployment.name()
            )),
        }
    }
    Ok(())
}

async fn execute_fixture(
    client: &ControlClient,
    protocol: &str,
    payload: Value,
    task_id: &str,
) -> Result<Value, String> {
    let submitted = checked_request(
        client,
        ControlMethod::TaskSubmitBatch,
        serde_json::to_value(TaskSubmitBatchParam {
            batch: TaskBatch::one(
                format!("batch-{task_id}"),
                Task::new(task_id, protocol, payload),
            ),
        })
        .map_err(|error| error.to_string())?,
    )
    .await?;
    let submitted: TaskSubmitBatchResponse =
        serde_json::from_value(submitted).map_err(|error| error.to_string())?;
    if submitted.handles.len() != 1 {
        return Err("submission returned an unexpected handle count".into());
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let outcome =
            checked_request(client, ControlMethod::TaskOutcome, json!({"id": task_id})).await?;
        let outcome: TaskOutcomeView =
            serde_json::from_value(outcome).map_err(|error| error.to_string())?;
        match outcome.status.as_str() {
            "completed" => return Ok(outcome.output.unwrap_or(Value::Null)),
            "failed" | "cancelled" | "expired" | "dead_letter" => {
                return Err(format!(
                    "{}:{}:{}",
                    outcome.error_code.unwrap_or_default(),
                    outcome.reason.unwrap_or_default(),
                    serde_json::to_string(&outcome.evidence).unwrap_or_default()
                ));
            }
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(1)).await,
            status => return Err(format!("task {task_id} timed out in state {status}")),
        }
    }
}

async fn checked_request(
    client: &ControlClient,
    method: ControlMethod,
    params: Value,
) -> Result<Value, String> {
    let response = client
        .request(method, params)
        .await
        .map_err(|error| error.to_string())?;
    if response.ok {
        Ok(response.result.unwrap_or(Value::Null))
    } else {
        Err(response
            .error
            .map(|error| format!("{}: {}", error.code, error.message))
            .unwrap_or_else(|| "control request failed without an error".into()))
    }
}

fn build_report(
    args: &Args,
    samples: &BTreeMap<String, Vec<Sample>>,
    fixture_hashes: &BTreeMap<String, String>,
    failures: &[String],
    cpu_ns: f64,
) -> Value {
    let revision = command_output("git", &["rev-parse", "HEAD"]);
    let dirty = !command_output("git", &["status", "--porcelain"]).is_empty();
    let revisions = json!({
        "MutsukiServiceHost": {
            "revision": revision,
            "dirty": dirty,
            "remote": "https://github.com/sena-nana/MutsukiServiceHost.git"
        }
    });
    let revision_lock_hash = canonical_hash(&revisions);
    let environment = environment(args);
    let environment_id = canonical_hash(&environment);
    let mut cases = Vec::new();
    for (deployment, values) in samples {
        for (metric, observations) in [
            (
                "startup",
                values
                    .iter()
                    .map(|value| value.startup_ns)
                    .collect::<Vec<f64>>(),
            ),
            (
                "health-ipc",
                values
                    .iter()
                    .map(|value| value.health_ipc_ns)
                    .collect::<Vec<f64>>(),
            ),
            (
                "echo-e2e",
                values
                    .iter()
                    .map(|value| value.echo_e2e_ns)
                    .collect::<Vec<f64>>(),
            ),
            (
                "reload",
                values
                    .iter()
                    .map(|value| value.reload_ns)
                    .collect::<Vec<f64>>(),
            ),
            (
                "shutdown",
                values
                    .iter()
                    .map(|value| value.shutdown_ns)
                    .collect::<Vec<f64>>(),
            ),
        ] {
            cases.push(json!({
                "case_id": format!("service-host.{deployment}.{metric}"),
                "measurement_mode": "time",
                "dimensions": {"deployment": deployment, "operation": metric},
                "metrics": {"latency_ns": distribution(&observations, "ns")},
                "correctness": {"passed": failures.is_empty(), "counters": {}},
            }));
        }
    }
    cases.push(json!({
        "case_id": "service-host.process-cpu",
        "measurement_mode": "system",
        "dimensions": {"scope": "complete-suite"},
        "metrics": {"cpu_time_ns": distribution(&[cpu_ns], "ns")},
        "correctness": {"passed": failures.is_empty(), "counters": {}},
    }));
    let case_count = cases.len();
    json!({
        "schema_version": "mutsuki.performance.report/v1",
        "suite_version": "service-host-performance/v1",
        "workload_version": "runner-fixtures/v1",
        "report_id": format!("service-host-{}-{}", args.mode, std::process::id()),
        "generated_at": command_output("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]),
        "revision_lock_hash": revision_lock_hash,
        "repository_revisions": revisions,
        "environment_id": environment_id,
        "environment": environment,
        "feature_set": ["actual-ipc", "builtin", "abi", "rust-process-jsonl", "lifecycle-reload"],
        "deployment": "service-host-matrix",
        "measurement_boundary": "ServiceRuntime start through authenticated IPC, Core task completion, reload and graceful shutdown",
        "sampling": {"warmup_iterations": args.warmup, "samples_per_process": args.samples, "process_runs": 1},
        "cases": cases,
        "correctness": {"passed": failures.is_empty(), "counters": {"failures": failures.len()}},
        "gates": [{"gate_id": "service-host.correctness", "passed": failures.is_empty(), "actual": failures.len(), "limit": 0, "unit": "failures"}],
        "metadata": {"fixture_output_hashes": fixture_hashes, "fixture_checks": fixture_hashes.len(), "case_count": case_count, "failures": failures, "public_runner_gate": "correctness-only"},
    })
}

fn distribution(values: &[f64], unit: &str) -> Value {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = percentile(&sorted, 0.5);
    let mut deviations = sorted
        .iter()
        .map(|value| (value - median).abs())
        .collect::<Vec<_>>();
    deviations.sort_by(f64::total_cmp);
    json!({
        "median": median,
        "p95": percentile(&sorted, 0.95),
        "p99": percentile(&sorted, 0.99),
        "mad": percentile(&deviations, 0.5),
        "min": sorted[0],
        "max": sorted[sorted.len() - 1],
        "unit": unit,
        "sample_count": sorted.len(),
        "samples": sorted,
    })
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    let index = ((values.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn environment(args: &Args) -> Value {
    let target = command_output("rustc", &["-vV"])
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or("unknown")
        .to_string();
    let cpu_model = if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        command_output("uname", &["-m"])
    };
    let ram_bytes = if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "hw.memsize"])
            .parse::<u64>()
            .unwrap_or(1)
    } else {
        1
    };
    json!({
        "cpu_model": if cpu_model.is_empty() { "unknown" } else { &cpu_model },
        "cpu_topology": format!("logical={}", std::thread::available_parallelism().map(usize::from).unwrap_or(1)),
        "ram_bytes": ram_bytes,
        "os": std::env::consts::OS,
        "kernel": command_output("uname", &["-sr"]),
        "architecture": std::env::consts::ARCH,
        "target_triple": target,
        "toolchains": {"rustc": command_output("rustc", &["--version"]), "cargo": command_output("cargo", &["--version"])},
        "release_profile": {"name": "release", "lto": false, "codegen_units": 16},
        "power_mode": "local-unspecified",
        "virtualization": "local-unspecified",
        "runner_configuration": {"mode": args.mode, "warmup": args.warmup, "samples": args.samples, "ipc": "platform-default"},
    })
}

fn canonical_hash(value: &Value) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn current_binary_dir() -> Result<PathBuf, String> {
    std::env::current_exe()
        .map_err(|error| error.to_string())?
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "benchmark executable has no parent directory".into())
}

fn platform_abi_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "mutsuki_service_abi_fixture.dll"
    } else if cfg!(target_os = "macos") {
        "libmutsuki_service_abi_fixture.dylib"
    } else {
        "libmutsuki_service_abi_fixture.so"
    }
}

fn platform_process_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "mutsuki-benchmark-runner.exe"
    } else {
        "mutsuki-benchmark-runner"
    }
}

fn parse_args() -> Args {
    let mut mode = "smoke".to_string();
    let mut warmup = None;
    let mut samples = None;
    let mut output = PathBuf::from("target/mutsuki-benchmarks/service-host-smoke.json");
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--mode" => mode = arguments.next().expect("--mode value"),
            "--warmup" => {
                warmup = Some(
                    arguments
                        .next()
                        .expect("--warmup value")
                        .parse()
                        .expect("warmup integer"),
                )
            }
            "--samples" => {
                samples = Some(
                    arguments
                        .next()
                        .expect("--samples value")
                        .parse()
                        .expect("samples integer"),
                )
            }
            "--output" => output = PathBuf::from(arguments.next().expect("--output value")),
            unknown => panic!("unknown argument {unknown}"),
        }
    }
    let (default_warmup, default_samples) = match mode.as_str() {
        "smoke" => (0, 1),
        "reference" => (1, 5),
        _ => panic!("mode must be smoke or reference"),
    };
    Args {
        mode,
        warmup: warmup.unwrap_or(default_warmup),
        samples: samples.unwrap_or(default_samples).max(1),
        output,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_retains_samples_and_robust_statistics() {
        let value = distribution(&[1.0, 2.0, 100.0], "ns");
        assert_eq!(value["median"], 2.0);
        assert_eq!(value["mad"], 1.0);
        assert_eq!(value["sample_count"], 3);
    }
}
