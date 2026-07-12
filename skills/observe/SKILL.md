---
name: observe
description: Change ServiceHost structured logging, trace, health aggregation, panic or crash capture, runner output, recent errors, or observability control responses.
---

# Observe

- Aggregate runtime health while leaving domain health semantics to the owning plugin.
- Emit structured logs and traces with correlation context.
- Redact tokens and secrets before disk, IPC or console output.
- Preserve existing panic hooks and write crash data under the configured home.
- Report degraded/unhealthy when a real component fails; do not synthesize healthy placeholders.

Test redaction, health aggregation, panic capture and bounded log tailing.
