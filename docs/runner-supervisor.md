# Runner Supervisor

The runner supervisor owns external process lifecycle for sidecars and non-Core-linked runners.

Implemented behavior:

- spawn process with sanitized allowlist environment
- inject `MUTSUKI_HOME`, `MUTSUKI_RUNNER_SESSION_TOKEN`, `MUTSUKI_RUNNER_ID`, and `MUTSUKI_PLUGIN_ID`
- drain stdout/stderr
- report process state
- restart and stop by runner id
- graceful shutdown before service exit

Core-connected `jsonl-stdio` runners are spawned by ServiceHost and registered with Core through
`mutsuki-runtime-host::JsonlRunner`. Task execution calls `runner.run_batch`, `runner.cancel`, and
`runner.dispose` over JSONL stdio (`{ ctx, batch }` -> `CompletionBatch`). ServiceHost does not
implement the obsolete `Runner::step` / `runner.step` path.

## Cancellation and isolation

In-process native runners support cooperative cancellation only. A wall-clock deadline can cancel
the Core task and quarantine the worker, but Rust cannot safely terminate the executing thread.
The Host therefore does not create a replacement until that worker actually exits. Once the
configured isolated-worker limit is reached, the pool reports degraded health and refuses further
dispatch instead of accumulating zombie threads.

Process and Python/Script deployments use a process boundary for hard isolation. On hard timeout,
the Host kills the child process through a thread-safe termination handle, waits for the blocked
JSONL call to return, recreates the process, and only then restores the runner and worker capacity.
Untrusted code, crash isolation, and strict wall-clock termination must use a process/ABI sidecar;
declaring a native runner as `Blocking` or `Script` does not grant thread-level hard termination.
