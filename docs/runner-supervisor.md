# Runner Supervisor

The runner supervisor owns external process lifecycle for sidecars and non-Core-linked runners.

Implemented behavior:

- spawn process with sanitized allowlist environment
- inject `MUTSUKI_HOME`, `MUTSUKI_RUNNER_SESSION_TOKEN`, `MUTSUKI_RUNNER_ID`, and `MUTSUKI_PLUGIN_ID`
- drain stdout/stderr
- report process state
- restart and stop by runner id
- graceful shutdown before service exit

Core-connected `jsonl-stdio` runners are owned by the Core runner adapter so task execution can synchronously call `runner.step`, `runner.cancel`, and `runner.dispose`.
