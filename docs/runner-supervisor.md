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
