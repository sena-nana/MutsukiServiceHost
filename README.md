# MutsukiServiceHost

`MutsukiServiceHost` is the headless, long-running host process for `MutsukiCore`.

It owns the operating environment around the core:

- service profile and directory layout
- Core bootstrap through `mutsuki-runtime-host`
- plugin manifest discovery and validation
- external runner process supervision
- local authenticated control API
- platform IPC transport
- structured logs, panic capture, and health aggregation
- foreground daemon loop

It deliberately does not implement Agent, Bot, QQBot, model provider, Python SDK, plugin marketplace, or GUI logic. Those belong in Core, plugins, runner kits, or UI hosts.

## Workspace

```text
crates/
  mutsuki-service-host              CLI binary
  mutsuki-service-runtime           service lifecycle and Core bootstrap
  mutsuki-service-config            config/profile/path/token loading
  mutsuki-service-plugin-loader     plugin.toml discovery and validation
  mutsuki-service-runner-supervisor external process supervision
  mutsuki-service-control           control request/response API
  mutsuki-service-ipc               named pipe / Unix socket / TCP debug transport
  mutsuki-service-observe           logging and panic hook
```

## Run

```powershell
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token run
```

In another shell:

```powershell
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token status
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token health
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token plugin list
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token stop
```

If no token is configured, ServiceHost creates and reuses a local token in `<home>/run/control.token`.

## Plugin Runtime

Dynamic plugins are discovered from `plugins/installed/**/plugin.toml`. A plugin manifest maps directly to the Mutsuki runtime contracts `PluginManifest`. External process plugins add a `[runtime]` section:

```toml
[runtime]
command = "example-runner"
args = []
env = {}
runner_link = "jsonl-stdio"
```

`jsonl-stdio` runners are launched and registered with Core as external runners. Sidecar processes without Core runner descriptors are supervised by the runner supervisor and exposed through the control API.

## Validation

```powershell
cargo fmt --check
cargo check
cargo test
```
