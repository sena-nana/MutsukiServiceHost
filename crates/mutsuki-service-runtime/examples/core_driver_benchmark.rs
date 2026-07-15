use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use cpu_time::ProcessTime;
use mutsuki_runtime_contracts::{
    ExecutionClass, ObservabilityProfile, RunnerBatchCapability, RunnerControlCapability,
    RunnerDescriptor, RunnerOrderingCapability, RunnerPayloadCapability, RunnerPurity,
    RunnerResourceCapability, RunnerResult, RuntimeProfile, RuntimeProfileMode, Task, TaskStatus,
};
use mutsuki_runtime_host::{
    HostRuntime, HostRuntimeCommand, HostRuntimeConfig, NativeRunner, RuntimeBootstrapper,
    runner_manifest,
};
use serde_json::{Value, json};

const PLUGIN_ID: &str = "bench.event-driver";
const RUNNER_ID: &str = "bench.event-driver.runner";
const PROTOCOL_ID: &str = "bench.event-driver.work";

fn descriptor() -> RunnerDescriptor {
    RunnerDescriptor {
        runner_id: RUNNER_ID.into(),
        plugin_id: PLUGIN_ID.into(),
        plugin_generation: 1,
        accepted_protocol_ids: vec![PROTOCOL_ID.into()],
        purity: RunnerPurity::Pure,
        execution_class: ExecutionClass::Cpu,
        input_schema: json!({}),
        output_schema: json!({}),
        batch: RunnerBatchCapability::default(),
        payload: RunnerPayloadCapability::default(),
        resources: RunnerResourceCapability::default(),
        ordering: RunnerOrderingCapability::default(),
        control: RunnerControlCapability::default(),
        metadata: BTreeMap::new(),
        contract_surfaces: vec![format!("runner:{RUNNER_ID}")],
    }
}

fn profile() -> RuntimeProfile {
    RuntimeProfile {
        profile_id: "bench".into(),
        mode: RuntimeProfileMode::FullDev,
        enabled_plugins: vec![PLUGIN_ID.into()],
        bindings: BTreeMap::new(),
        plugin_deployments: BTreeMap::new(),
        observability: ObservabilityProfile::default(),
        allow_dynamic_registration: false,
        allow_hot_reload: false,
    }
}

fn runtime(event_driven: bool) -> HostRuntime {
    let runner = descriptor();
    let mut bootstrapper = RuntimeBootstrapper::new();
    bootstrapper.register_manifest(runner_manifest(PLUGIN_ID, vec![runner.clone()]));
    bootstrapper.register_runner(Box::new(NativeRunner::new(runner, |_ctx, task| {
        Ok(RunnerResult::completed(task.task_id))
    })));
    bootstrapper
        .into_host_runtime_with_config(
            profile(),
            HostRuntimeConfig {
                event_driven,
                tick_interval: Duration::from_millis(10),
                ..HostRuntimeConfig::default()
            },
        )
        .expect("benchmark runtime")
}

fn idle_sample(event_driven: bool, duration: Duration) -> Value {
    let runtime = runtime(event_driven);
    let before = runtime.drive_state().expect("driver state");
    let wall_started = Instant::now();
    let cpu_started = ProcessTime::now();
    if event_driven {
        std::thread::sleep(duration);
    } else {
        while wall_started.elapsed() < duration {
            runtime
                .dispatch(HostRuntimeCommand::TickOnce)
                .expect("legacy fixed poll tick");
        }
    }
    let cpu = cpu_started.elapsed();
    let wall = wall_started.elapsed();
    let after = runtime.drive_state().expect("driver state");
    json!({
        "wall_ms": wall.as_secs_f64() * 1_000.0,
        "process_cpu_ms": cpu.as_secs_f64() * 1_000.0,
        "logical_ticks": after.current_step.saturating_sub(before.current_step),
        "timed_wakeups": after.timed_wakeups.saturating_sub(before.timed_wakeups),
    })
}

fn wait_completed(runtime: &HostRuntime, task_id: &str) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(1) {
        if runtime.task_status(task_id) == Some(TaskStatus::Completed) {
            return;
        }
        std::thread::yield_now();
    }
    panic!("task {task_id} did not complete");
}

fn latency_samples(event_driven: bool, samples: usize) -> Value {
    let runtime = runtime(event_driven);
    let mut micros = Vec::with_capacity(samples);
    for index in 0..samples {
        let task_id = format!("latency-{event_driven}-{index}");
        let started = Instant::now();
        runtime
            .dispatch(HostRuntimeCommand::SubmitTask(Box::new(Task::new(
                &task_id,
                PROTOCOL_ID,
                json!({}),
            ))))
            .expect("submit benchmark task");
        if !event_driven {
            std::thread::sleep(Duration::from_millis(10));
            runtime
                .dispatch(HostRuntimeCommand::TickOnce)
                .expect("legacy fixed poll tick");
        }
        wait_completed(&runtime, &task_id);
        micros.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    micros.sort_by(f64::total_cmp);
    let sum: f64 = micros.iter().sum();
    let p95_index = ((micros.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(micros.len() - 1);
    json!({
        "samples": samples,
        "average_us": sum / micros.len() as f64,
        "p95_us": micros[p95_index],
        "max_us": micros[micros.len() - 1],
    })
}

fn main() {
    let idle_ms = std::env::var("MUTSUKI_BENCH_IDLE_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000_u64);
    let samples = std::env::var("MUTSUKI_BENCH_SAMPLES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(30_usize)
        .max(1);
    let report = json!({
        "schema": "mutsuki.service-host.issue12.driver-benchmark.v1",
        "platform": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "configuration": {
            "legacy_poll_interval_ms": 10,
            "idle_duration_ms": idle_ms,
            "latency_samples": samples,
            "legacy_latency_phase": "submit immediately after a poll; wait for next 10ms tick",
        },
        "idle": {
            "fixed_poll": idle_sample(false, Duration::from_millis(idle_ms)),
            "event_driven": idle_sample(true, Duration::from_millis(idle_ms)),
        },
        "submit_to_completed_latency": {
            "fixed_poll": latency_samples(false, samples),
            "event_driven": latency_samples(true, samples),
        },
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
