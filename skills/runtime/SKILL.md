---
name: runtime
description: Change ServiceHost Core bootstrap, ServiceRuntimeBuilder, HostServices, event-source lifecycle, service loop, shutdown, drain, or panic boundaries.
---

# Runtime

- Start Core only through published runtime-host/core APIs and a validated RuntimeLoadPlan.
- Own process and EventSource lifecycle, not Core scheduling or domain behavior.
- Make bootstrap failure unavailable/fatal; never report a service healthy without a real Core runtime.
- Stop IPC, EventSources and runners before releasing HostRuntime.
- Keep standard protocol wrappers in StdPlugins and product selection in product/template repositories.

Test startup failure, real builder assembly, event-source stop and graceful shutdown ordering.
