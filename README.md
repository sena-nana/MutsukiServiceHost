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
- product-side builtin/native-runner assembly and long-lived event-source lifecycle

It deliberately does not implement Agent, Bot, QQBot, model provider, Python SDK, plugin marketplace,
or GUI/business logic. Those belong in Core, StdPlugins, runner kits, or UI hosts.

Optional compile-time host facades:

- `mutsuki.conversation.sim` — **dev/mock** conversation control facade (in-memory turns only; not long-term ServiceHost business state)
- `mutsuki.terminal.tui` — **UI host feature** for the local terminal client; not a runtime effect plugin

Runtime crates are consumed from the sibling `MutsukiCore` workspace (`../MutsukiCore/crates/...`).

## Workspace

```text
crates/
  mutsuki-service-host              CLI binary
  mutsuki-service-runtime           service lifecycle and Core bootstrap
  mutsuki-service-config            config/profile/path/token loading
  mutsuki-service-plugin-loader     plugin.toml discovery and validation
  mutsuki-service-plugin-conversation-sim optional **dev/mock** conversation control facade
  mutsuki-service-plugin-terminal-tui     optional **UI host feature** for local TUI attachment
  mutsuki-service-tui               terminal client library
  mutsuki-service-runner-supervisor external process supervision
  mutsuki-service-control           control request/response API
  mutsuki-service-ipc               named pipe / Unix socket / TCP debug transport
  mutsuki-service-observe           logging and panic hook
  mutsuki-service-daemon            Windows Service installation and lifecycle
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
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token event-source list
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token tui
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token stop
```

If no token is configured, ServiceHost creates and reuses a local token in `<home>/run/control.token`.

## Windows Service

```powershell
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token install
cargo run -p mutsuki-service-host -- --home .mutsuki-dev start
cargo run -p mutsuki-service-host -- --home .mutsuki-dev uninstall
```

`install` creates an automatic Windows Service named from the configured instance id, such as
`mutsuki-service-default`. If `--token` or `MUTSUKI_CONTROL_TOKEN` is used during installation,
the token is stored in `<home>/run/control.token` and is not written into the service command line.

## Plugin Runtime

Dynamic plugins are discovered from `plugins/installed/**/plugin.toml`. A plugin manifest maps directly to the Mutsuki runtime contracts `PluginManifest`. External process plugins add a `[runtime]` section:

```toml
[runtime]
command = "example-runner"
args = []
env = {}
runner_link = "jsonl-stdio"
```

`jsonl-stdio` runners are launched by ServiceHost, wrapped with `mutsuki-runtime-host::JsonlRunner`,
and registered with Core as external runners (`runner.run_batch`). Sidecar processes without Core
runner descriptors are supervised by the runner supervisor and exposed through the control API.

The default binary also links optional host facades for local terminal conversation simulation.
These are **not** ServiceHost runtime business capabilities:

- `mutsuki.conversation.sim` — dev/mock control facade
- `mutsuki.terminal.tui` — UI host feature for `mutsuki-service tui`

Enable them in `[plugins].builtin`, then attach with `mutsuki-service tui`.

## Validation

```powershell
cargo fmt --check
cargo check
cargo test
```
