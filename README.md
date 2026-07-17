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

Runtime crates are consumed from the sibling `MutsukiCore` workspace (`../MutsukiCore/crates/...`).
The interactive terminal client lives in the separate `MutsukiCliHost` repository.

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
  mutsuki-service-daemon            Windows Service, launchd and systemd lifecycle
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
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token plugin set owner.plugin.id abi
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token plugin clear owner.plugin.id
cargo run -p mutsuki-service-host -- --home .mutsuki-dev --token dev-token event-source list
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

## launchd and systemd

macOS and Linux default to a user service. `install` writes and enables the service definition
without starting it; `start` activates it. `stop` uses the authenticated control API so the Runtime
drains IPC, EventSources and runners before exit, and `uninstall` also stops a running service before
removing its definition:

```powershell
cargo run -p mutsuki-service-host -- --config path/to/service.toml install --scope user
cargo run -p mutsuki-service-host -- --config path/to/service.toml start --scope user
cargo run -p mutsuki-service-host -- --config path/to/service.toml stop
cargo run -p mutsuki-service-host -- --config path/to/service.toml uninstall --scope user
```

System scope writes `/Library/LaunchDaemons` or `/etc/systemd/system` and requires both caller-managed
elevation and an explicit non-root account. The daemon library never invokes `sudo`:

```powershell
sudo target/release/mutsuki-service --config path/to/service.toml install --scope system --service-user mutsuki
sudo target/release/mutsuki-service --config path/to/service.toml start --scope system
```

Tokens remain outside service definitions. When installation persists an overridden token, the file
is written to `<home>/run/control.token` with owner-only permissions and assigned to the service user.
The local lifecycle smoke is `scripts/daemon-smoke.sh --scope user`; Ubuntu CI runs the same script in
system scope.

## Plugin Runtime

Dynamic plugins are inventoried from `plugins/installed/**/plugin.toml`, but only IDs selected by
`[[plugins.configured]]` enter the RuntimeLoadPlan. A plugin manifest maps directly to the Mutsuki
runtime contracts `PluginManifest`. External process plugins add a `[runtime]` section:

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

ABI plugins use the same Core JSONL runner/task/resource methods through the versioned byte
transport exported by `mutsuki-runtime-sdk`. An installed package contains `plugin.toml` and one
platform library (`.dll`, `.so`, or `.dylib`) referenced by `manifest.artifact.path`. The path must
remain inside the package directory and `manifest.artifact.sha256` must contain the exact lowercase
`sha256:<hex>` digest. ServiceHost verifies the artifact, stages it under
`<run>/abi/<plugin-id>/<sha256>/`, sends the same owner-defined config through the mandatory Core
ABI v2 `plugin.initialize` request, and only then registers its
Runner and ResourceProvider surfaces. A `native` artifact found in a dynamic directory is rejected;
native plugins must be linked into the product's configured factory catalog.

When builtin and ABI are both available, builtin is the deterministic default. Authenticated Host
management may switch deployment without changing business config; the preference is persisted in
`<data>/plugin-deployments.json`, while the exact resolved artifacts are written to
`<run>/runtime.lock.json`. Invalid unselected artifacts remain visible as inventory diagnostics and
do not prevent unrelated configured plugins from starting.

ABI libraries are trusted in-process code, not a sandbox boundary. Plugins that require crash or
security isolation should use the process/Python deployment instead.

ServiceHost does not link development, conversation, or UI plugins. Product binaries may register
real builtin crates through `ServiceRuntimeBuilder`; missing upstream capabilities remain unavailable.

## Performance

The versioned ServiceHost benchmark matrix exercises real builtin, ABI, and independent Rust JSONL
Runner deployments through authenticated IPC, reload, and graceful shutdown. See
[`docs/performance-model-v1.md`](docs/performance-model-v1.md) for smoke and local reference commands.

## Validation

```powershell
cargo fmt --check
cargo check
cargo test
```
