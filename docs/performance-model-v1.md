# ServiceHost performance model v1

`mutsuki-service-benchmarks` measures the production `ServiceRuntime` boundary for the three
deployments owned by this repository:

- linked builtin Rust Runner;
- verified ABI dynamic library, including staging, `dlopen` and Runtime Wire initialization;
- independently executable Rust process over the JSONL Runner Link.

Every deployment runs the same six `runner-fixtures/v1` behaviors. The timed cases cover startup,
authenticated health IPC, echo submission through terminal outcome, registry reload and graceful
shutdown. Reports use `mutsuki.performance.report/v1`, retain the raw samples used to calculate
median, p95, p99 and MAD, and label the exact measurement boundary and deployment.

Run a fast contract check with:

```sh
python3 crates/mutsuki-service-benchmarks/scripts/run-reference.py \
  --mode smoke --warmup 0 --samples 1 \
  --output target/mutsuki-benchmarks/service-host-smoke.json
```

Capture a local reference run with:

```sh
python3 crates/mutsuki-service-benchmarks/scripts/run-reference.py \
  --mode reference \
  --output artifacts/performance/issue15-macos-arm64-provisional/report.json
```

The sibling `.analysis.json` classifies structural/correctness failures as a benchmark
implementation error and high relative MAD as environmental noise. `fixtures/performance/` is the
authority for the executable six-fixture manifest; PythonRunnerKit mirrors it. This repository owns
the builtin, ABI and Rust-process reports and retains their local/fixed-machine history under
`artifacts/performance/`. Promotion requires a clean repository-revision snapshot, matching
environment fingerprint and an exact-byte approval created with MutsukiCore's performance contract
tooling. A new CI artifact is never promoted automatically.

## Control IPC (Issue #16)

Dedicated control-plane harness:

```sh
cargo run -p mutsuki-service-benchmarks --release --bin mutsuki-control-ipc-bench -- \
  --output artifacts/performance/issue16-macos-arm64/report.json
```

It compares one-shot JSONL, persistent JSONL, and persistent binary across in-flight and payload
sizes, and enforces p95, connections/request, and allocation-overhead gates.
