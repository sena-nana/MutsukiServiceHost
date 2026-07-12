---
name: daemon
description: Change ServiceHost foreground mode, Windows Service, systemd, launchd, install or uninstall commands, service start or stop, process identity, or long-running daemon integration.
---

# Daemon

- Manage only the ServiceHost process; do not add GUI, plugin-marketplace or business configuration behavior.
- Keep foreground `run` fully functional and share the same runtime path with service mode.
- Store tokens outside service command lines and preserve least-privilege filesystem access.
- Return explicit unsupported errors on unimplemented platforms.
- Coordinate stop with ServiceRuntime graceful shutdown rather than terminating immediately.

Test install arguments, token isolation, unsupported platforms and graceful service stop.
