# Architecture

`MutsukiServiceHost` is a daemon host around `MutsukiCore`.

```text
ServiceHost
  - config/profile/path/security
  - plugin discovery
  - external runner process control
  - local IPC control plane
  - logs/health/lifecycle
      |
      v
mutsuki-runtime-host
      |
      v
MutsukiCore
```

The service host does not own task scheduling. Core remains the source of truth for task pool, runner registry, result routing, resource management, event log, trace log, and load-plan validation.

## Current MVP

- Foreground `run` mode.
- Core bootstrap with an extensible runtime profile.
- Product assembly through `ServiceRuntimeBuilder`: boot-time builtin manifests, recreatable native runner factories, and long-lived event sources.
- Builtin registry boundary. Real builtin plugin crates must be linked and registered explicitly; unavailable builtin names fail startup.
- `plugin.toml` scanning from configured plugin directories.
- JSONL stdio external runners registered with Core via `JsonlRunner::run_batch`.
- Sidecar process supervision for external runtimes that do not expose Core runner descriptors.
- Local control API over Windows named pipe, Unix socket, or explicit TCP debug transport.
- Authenticated service status, core status, plugin list, runner list/restart/stop, event-source list/restart, task list/cancel/outcome, health, shutdown.
- Authenticated plugin reload with manifest rescan, Core generation drain/swap, catalog replacement, and sidecar reconciliation.
- Windows Service install, uninstall, start, and SCM stop handling.

## Product assembly and event sources

`ServiceRuntimeBuilder` is the only product-side registration window. Product manifests are added to the builtin catalog before `RuntimeProfile` and the Core load plan are resolved. Native runners are registered as factories so reload can construct a fresh generation; runtime registration remains disabled.

Factories that initialize fallible external clients use
`register_fallible_builtin_runner`. Their errors fail initial Core boot or the
prepared reload with a structured Host error instead of panicking inside a
plugin factory.

Runtime-client-aware runner factories use the same frozen registration window.
The Host binds their generic SDK client only after Core boot and recreates the
runner on reload; this is not a dynamic registration path or a domain-specific
service API.

Product bundles may register read-only `health` component probes during the
same frozen assembly window. Probe snapshots appear under
`HealthReport.components`; they must not mutate runtime state or expose
secrets.

`HostEventSource` represents a long-lived external connection. Its context exposes only a Core `TaskSubmitter`, a shutdown token, read-only non-secret service configuration, environment-backed secret lookup, structured logging, and the source instance id. It cannot access `TaskPool`, `StateStore`, or `EventLog`. The host isolates source errors and panics, tracks lifecycle/health, supports explicit restart, and bounds shutdown by the configured graceful timeout.

An event source can declare required secret keys on its descriptor. ServiceHost
resolves only their presence through the configured Host secret backend before
Core, IPC, runners, or event sources start, so missing credentials fail loud
without moving secret loading into a domain plugin.

The service tick loop drives tasks submitted by event sources through the normal Core lease, batch runner, completion, and `ResultRouter` path. Plugin reload keeps event sources running; native runners are drained and recreated for the new Core generation.

## Explicit Gaps

- systemd/launchd installation is not implemented yet; non-Windows daemon commands return explicit unsupported errors.
