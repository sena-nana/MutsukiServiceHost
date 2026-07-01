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
- Builtin registry boundary. Real builtin plugin crates must be linked and registered explicitly; unavailable builtin names fail startup.
- `plugin.toml` scanning from configured plugin directories.
- JSONL stdio external runners registered with Core.
- Sidecar process supervision for external runtimes that do not expose Core runner descriptors.
- Local control API over Windows named pipe, Unix socket, or explicit TCP debug transport.
- Authenticated service status, core status, plugin list, runner list/restart/stop, task cancel, health, shutdown.
- Windows Service install, uninstall, start, and SCM stop handling.

## Explicit Gaps

- `task.list` is not faked because the current `mutsuki-runtime-host` command API does not expose a task snapshot command.
- `plugin.reload` returns unsupported until Core generation swap/drain APIs are exposed to this host.
- systemd/launchd installation is not implemented yet; non-Windows daemon commands return explicit unsupported errors.
