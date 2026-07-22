//! Control IPC performance harness for ServiceHost Issue #16.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use mutsuki_service_config::{IpcCodec, IpcTransport, ServiceConfig};
use mutsuki_service_control::{ControlHandler, ControlMethod, ControlRequest, ControlResponse};
use mutsuki_service_ipc::{
    ControlClient, ControlClientConfig, ControlSession, IpcServer, start_server,
};
use serde::Serialize;
use serde_json::Value;
use tempfile::TempDir;

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

struct TrackingAllocator {
    allocated_bytes: AtomicU64,
}

impl TrackingAllocator {
    const fn new() -> Self {
        Self {
            allocated_bytes: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> u64 {
        self.allocated_bytes.load(Ordering::Relaxed)
    }
}

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            self.allocated_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(ptr, layout, new_size) };
        if !replacement.is_null() {
            self.allocated_bytes
                .fetch_add(new_size as u64, Ordering::Relaxed);
        }
        replacement
    }
}

struct EchoHandler;

impl ControlHandler for EchoHandler {
    fn handle(&self, request: ControlRequest) -> mutsuki_service_control::ControlFuture {
        Box::pin(async move {
            ControlResponse {
                ok: true,
                result: Some(request.params),
                error: None,
            }
        })
    }
}

#[derive(Clone, Copy)]
enum BenchMode {
    OneShotJsonl,
    PersistentJsonl,
    PersistentBinary,
}

impl BenchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::OneShotJsonl => "one-shot-jsonl",
            Self::PersistentJsonl => "persistent-jsonl",
            Self::PersistentBinary => "persistent-binary",
        }
    }

    fn codec(self) -> IpcCodec {
        match self {
            Self::OneShotJsonl | Self::PersistentJsonl => IpcCodec::Jsonl,
            Self::PersistentBinary => IpcCodec::Binary,
        }
    }
}

#[derive(Serialize)]
struct CaseResult {
    case_id: String,
    mode: String,
    in_flight: usize,
    payload_bytes: usize,
    requests: usize,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    throughput_rps: f64,
    allocated_bytes_per_request: f64,
    connections_per_request: f64,
}

#[derive(Serialize)]
struct GateResult {
    gate_id: String,
    passed: bool,
    actual: f64,
    limit: f64,
    unit: String,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    transport: String,
    cases: Vec<CaseResult>,
    gates: Vec<GateResult>,
    passed: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "smoke");
    let output = args
        .windows(2)
        .find(|w| w[0] == "--output")
        .map(|w| PathBuf::from(&w[1]))
        .unwrap_or_else(|| {
            PathBuf::from(if smoke {
                "target/mutsuki-benchmarks/control-ipc-smoke.json"
            } else {
                "target/mutsuki-benchmarks/control-ipc-issue16.json"
            })
        });

    let in_flights: &[usize] = if smoke { &[1] } else { &[1, 16, 64] };
    let payloads: &[usize] = if smoke {
        &[64]
    } else {
        &[64, 4 * 1024, 64 * 1024]
    };
    let requests_per_case = if smoke { 32 } else { 256 };
    let warmup = if smoke { 4 } else { 32 };

    let mut cases = Vec::new();
    for mode in [
        BenchMode::OneShotJsonl,
        BenchMode::PersistentJsonl,
        BenchMode::PersistentBinary,
    ] {
        for &in_flight in in_flights {
            let effective_inflight = match mode {
                BenchMode::PersistentBinary => in_flight,
                _ => 1.min(in_flight),
            };
            if matches!(mode, BenchMode::OneShotJsonl | BenchMode::PersistentJsonl)
                && in_flight > 1
                && !smoke
            {
                continue;
            }
            for &payload_bytes in payloads {
                let case = run_case(
                    mode,
                    effective_inflight,
                    payload_bytes,
                    requests_per_case,
                    warmup,
                )
                .await?;
                println!(
                    "{} inflight={} payload={} p95={}ns alloc/req={:.0} conn/req={:.4} rps={:.0}",
                    case.mode,
                    case.in_flight,
                    case.payload_bytes,
                    case.p95_ns,
                    case.allocated_bytes_per_request,
                    case.connections_per_request,
                    case.throughput_rps
                );
                cases.push(case);
            }
        }
    }

    let gates = evaluate_gates(&cases);
    let passed = gates.iter().all(|g| g.passed);
    let report = Report {
        schema: "mutsuki.servicehost.control-ipc.perf/v1",
        transport: default_transport_name(),
        cases,
        gates,
        passed,
    };
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "control ipc benchmark: result={} report={}",
        if passed { "PASS" } else { "FAIL" },
        output.display()
    );
    if !passed {
        for gate in report.gates.iter().filter(|g| !g.passed) {
            eprintln!(
                "FAIL {} actual={} limit={} {}",
                gate.gate_id, gate.actual, gate.limit, gate.unit
            );
        }
        std::process::exit(1);
    }
    Ok(())
}

fn default_transport_name() -> String {
    #[cfg(windows)]
    {
        "named-pipe".into()
    }
    #[cfg(unix)]
    {
        "unix-socket".into()
    }
}

async fn run_case(
    mode: BenchMode,
    in_flight: usize,
    payload_bytes: usize,
    requests: usize,
    warmup: usize,
) -> Result<CaseResult, Box<dyn std::error::Error>> {
    let (_tmp, config, server) = start_echo_server(mode.codec()).await?;
    let client_config = ControlClientConfig::from(&config);
    let payload = Value::String("x".repeat(payload_bytes));

    // Warmup outside measurement.
    match mode {
        BenchMode::OneShotJsonl => {
            let client = ControlClient::new(client_config.clone());
            for _ in 0..warmup {
                let _ = client
                    .request_oneshot(ControlMethod::HealthCheck, payload.clone())
                    .await?;
            }
        }
        BenchMode::PersistentJsonl | BenchMode::PersistentBinary => {
            let session = ControlSession::connect(client_config.clone()).await?;
            for _ in 0..warmup {
                let _ = session
                    .request(ControlMethod::HealthCheck, payload.clone())
                    .await?;
            }
            session.close().await?;
        }
    }

    let alloc_before = ALLOCATOR.snapshot();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(requests);
    let connections;

    match mode {
        BenchMode::OneShotJsonl => {
            let client = ControlClient::new(client_config.clone());
            for _ in 0..requests {
                let t0 = Instant::now();
                let _ = client
                    .request_oneshot(ControlMethod::HealthCheck, payload.clone())
                    .await?;
                latencies.push(t0.elapsed().as_nanos() as u64);
            }
            connections = requests as u64;
        }
        BenchMode::PersistentJsonl | BenchMode::PersistentBinary => {
            let session = ControlSession::connect(client_config.clone()).await?;
            let session = Arc::new(session);
            let sem = Arc::new(tokio::sync::Semaphore::new(in_flight));
            let mut handles = Vec::with_capacity(requests);
            for _ in 0..requests {
                let permit = sem.clone().acquire_owned().await?;
                let session = session.clone();
                let payload = payload.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = permit;
                    let t0 = Instant::now();
                    let result = session.request(ControlMethod::HealthCheck, payload).await;
                    (t0.elapsed().as_nanos() as u64, result)
                }));
            }
            for handle in handles {
                let (ns, result) = handle.await?;
                result?;
                latencies.push(ns);
            }
            connections = session.connection_count();
            if let Ok(session) = Arc::try_unwrap(session) {
                session.close().await?;
            }
        }
    }

    let elapsed = started.elapsed();
    let alloc_after = ALLOCATOR.snapshot();
    let allocated = alloc_after.saturating_sub(alloc_before);
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p95 = percentile(&latencies, 95);
    let p99 = percentile(&latencies, 99);
    let throughput = requests as f64 / elapsed.as_secs_f64().max(1e-9);

    server.shutdown().await;

    Ok(CaseResult {
        case_id: format!(
            "control-ipc/{}/inflight-{}/payload-{}",
            mode.as_str(),
            in_flight,
            payload_bytes
        ),
        mode: mode.as_str().into(),
        in_flight,
        payload_bytes,
        requests,
        p50_ns: p50,
        p95_ns: p95,
        p99_ns: p99,
        throughput_rps: throughput,
        allocated_bytes_per_request: allocated as f64 / requests as f64,
        connections_per_request: connections as f64 / requests as f64,
    })
}

async fn start_echo_server(
    codec: IpcCodec,
) -> Result<(TempDir, ServiceConfig, IpcServer), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let mut config = ServiceConfig::default();
    config.service.run_dir = tmp.path().to_path_buf();
    config.ipc.enabled = true;
    config.ipc.codec = codec;
    config.ipc.token = Some("bench-token".into());
    config.ipc.name = format!("control-ipc-bench-{}", std::process::id());
    #[cfg(unix)]
    {
        config.ipc.transport = IpcTransport::UnixSocket;
    }
    #[cfg(windows)]
    {
        config.ipc.transport = IpcTransport::NamedPipe;
    }
    config.ipc.max_frame_bytes = 2 * 1024 * 1024;
    config.ipc.max_payload_bytes = 1024 * 1024;
    config.ipc.max_jsonl_line_bytes = 1024 * 1024;
    config.ipc.max_in_flight = 128;
    config.ipc.request_timeout_ms = 30_000;
    config.ipc.idle_timeout_ms = 60_000;

    let server = start_server(&config, Arc::new(EchoHandler))
        .await?
        .expect("ipc enabled");
    Ok((tmp, config, server))
}

fn percentile(sorted: &[u64], pct: u8) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (pct.min(100) as usize * (sorted.len() - 1)).div_ceil(100);
    sorted[rank]
}

fn evaluate_gates(cases: &[CaseResult]) -> Vec<GateResult> {
    let by_id: BTreeMap<String, &CaseResult> =
        cases.iter().map(|c| (c.case_id.clone(), c)).collect();

    let mut gates = Vec::new();

    if let (Some(oneshot), Some(binary)) = (
        by_id.get("control-ipc/one-shot-jsonl/inflight-1/payload-64"),
        by_id.get("control-ipc/persistent-binary/inflight-1/payload-64"),
    ) {
        let reduction = 1.0 - (binary.p95_ns as f64 / oneshot.p95_ns.max(1) as f64);
        gates.push(GateResult {
            gate_id: "p95.persistent-binary.vs.oneshot-jsonl.short".into(),
            passed: reduction >= 0.40,
            actual: reduction,
            limit: 0.40,
            unit: "relative_reduction".into(),
        });
        gates.push(GateResult {
            gate_id: "connections-per-request.persistent-binary.short".into(),
            passed: binary.connections_per_request <= 0.05,
            actual: binary.connections_per_request,
            limit: 0.05,
            unit: "connections/request".into(),
        });
    }

    // Allocation overhead for payloads <= 64 KiB must drop >=30% vs one-shot JSONL.
    for payload in [64_usize, 4096, 65536] {
        let oneshot_key = format!("control-ipc/one-shot-jsonl/inflight-1/payload-{payload}");
        let binary_key = format!("control-ipc/persistent-binary/inflight-1/payload-{payload}");
        let (Some(oneshot), Some(binary)) = (by_id.get(&oneshot_key), by_id.get(&binary_key))
        else {
            continue;
        };
        let oneshot_overhead =
            (oneshot.allocated_bytes_per_request - 2.0 * payload as f64).max(1.0);
        let binary_overhead = (binary.allocated_bytes_per_request - 2.0 * payload as f64).max(0.0);
        let reduction = 1.0 - (binary_overhead / oneshot_overhead);
        gates.push(GateResult {
            gate_id: format!(
                "allocated-overhead.persistent-binary.vs.oneshot-jsonl.payload-{payload}"
            ),
            passed: reduction >= 0.30,
            actual: reduction,
            limit: 0.30,
            unit: "relative_reduction".into(),
        });
    }

    if let Some(binary) = by_id.get("control-ipc/persistent-binary/inflight-16/payload-64") {
        gates.push(GateResult {
            gate_id: "connections-per-request.persistent-binary.inflight-16".into(),
            passed: binary.connections_per_request <= 0.05,
            actual: binary.connections_per_request,
            limit: 0.05,
            unit: "connections/request".into(),
        });
    }

    gates
}
