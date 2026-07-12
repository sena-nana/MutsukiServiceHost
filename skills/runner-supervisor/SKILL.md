---
name: runner-supervisor
description: Change ServiceHost external runner process launch, Runner Link selection, environment isolation, stdout or stderr collection, restart policy, cancellation, disposal, or graceful termination.
---

# Runner Supervisor

- Supervise processes; leave Runner Link codecs and SDK behavior to Core adapters or runner-kit repositories.
- Build the exact process environment in ServiceHost, then launch through Core `SpawnedJsonlRunner`.
- Pass only allowlisted environment plus explicit Host session values and secret references.
- Drain stdout/stderr, rate-limit restart and expose structured failed state after exhaustion.
- Shut down gracefully, then kill only after timeout.
- Reject unsupported links instead of adding a private protocol or local fallback.

Test crash loops, blocked streams, environment filtering, restart limits and shutdown timeout.
