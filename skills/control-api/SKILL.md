---
name: control-api
description: Change ServiceHost local control requests, responses, IPC transports, authentication, status, health, task operations, plugin control, runner control, log tailing, or CLI-facing contracts.
---

# Control API

- Keep the control plane local by default and token-authenticated; debug TCP requires explicit enablement.
- Expose service/core/plugin/runner/task/log/health control only, not a parallel business runtime.
- Use the public `ControlClient` with endpoint, transport and token; never require consumers to load `ServiceConfig`.
- Reject arbitrary plugin operations; domain calls must enter Core as tasks.
- Use `TaskHandle` for cancellation and outcomes and preserve structured errors.
- Return real backend state; missing capability must be unavailable, never fabricated.
- Keep secrets out of responses and logs.

Test authentication, transport boundaries, invalid requests and real state transitions.
