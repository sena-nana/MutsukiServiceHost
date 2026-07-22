# Config

Config load order:

```text
default config
profile config
local service.toml
environment variables
CLI overrides
```

When a caller explicitly selects a service file, that file must exist. Products may keep an
ignored local service file in their repository and pass it to `ServiceConfig::load`; ServiceHost
still owns parsing, validation, profile merge and directory resolution.

Default home:

```text
~/.mutsuki
```

Important directories:

- `data`
- `logs`
- `plugins`
- `run`

Each top-level section may be partial. Omitted fields inherit the section defaults, so product
templates can expose only the settings their users are expected to change while ServiceHost
continues to own advanced runtime defaults.

`core.worker_profile` selects the bounded Host worker topology:

- `low-resource`: one shared compute worker and one blocking worker.
- `desktop` (default): available parallelism for the shared compute pool plus two blocking workers.
- `server`: available parallelism for compute plus a bounded `2..=8` blocking pool.

Orchestration, synchronous runner IO, and CPU work share the compute pool. Blocking and Script
dispatches share the bounded blocking pool; async network and event-source IO remains on Tokio.
Advanced deployments may override `worker_threads`, `blocking_threads`, `pool_queue_limit`,
`pool_max_inflight_bytes`, and `max_isolated_workers`, but zero values fail Host startup.
`runner_wall_clock_timeout_ms`, `cancel_grace_period_ms`, and `worker_health_timeout_ms` enable
Host supervision deadlines. A wall-clock timeout is a hard termination guarantee only for process
runners; native runners remain cooperative and may cause the pool to report degraded health.

## Control IPC

`ipc.codec` selects the control transport profile:

- `binary` (default): persistent length-prefixed MessagePack session with multiplexed `RequestId`s.
- `jsonl`: diagnostics/migration newline-delimited JSON; sequential, with a hard max line length.

Additional bounds (`max_frame_bytes`, `max_payload_bytes`, `max_jsonl_line_bytes`, `max_in_flight`,
`idle_timeout_ms`, `request_timeout_ms`) reject oversized or runaway clients before unbounded
allocation. High-frequency data streams stay on MutsukiLink; do not raise control frame limits to
carry media or tracking payloads.

If no control token is provided through config, `MUTSUKI_CONTROL_TOKEN`, or CLI `--token`, ServiceHost creates `<home>/run/control.token` and reuses it for local clients.
# Configured plugins

Every business plugin is selected through the same product configuration, regardless of whether
the Host resolves it to a linked builtin or an installed ABI/process artifact:

```toml
[[plugins.configured]]
id = "owner.plugin.id"
enabled = true

[plugins.configured.config]
mode = "owner-defined"
credential_key = "HOST_SECRET_KEY"
```

Builtin plugins require a matching `ConfiguredPluginFactory`; ABI-only plugins validate the same
config during `plugin.initialize`. Missing artifacts and duplicate IDs fail startup. The nested
config is opaque to ServiceHost, but raw `secret`, `token`,
`password` and `api_key` values are rejected; use owner-defined `_key` or `_ref` fields resolved
at the Host boundary.

`dynamic_dirs` is installation inventory only and never enables a plugin. Host management choices
are stored in `<data>/plugin-deployments.json`; without a choice, a sole deployment is selected and
builtin wins when builtin and ABI are both available. The exact active deployment is written to
`<run>/runtime.lock.json`.

## Secret file backend

Products may reference a dedicated local TOML file from the primary service config:

```toml
[security]
secret_file = "local.secret.toml"
```

The relative path is resolved from the primary service file directory. The secret file is strict:

```toml
[secrets]
PROVIDER_API_KEY = "local-value"
```

Secret names are normalized like environment-backed keys. Empty or duplicate normalized keys,
missing files and malformed TOML fail startup. `MUTSUKI_SECRET_<KEY>` overrides the file value.
Loaded values remain in the Host `SecretStore` and are excluded from serialization and debug
output. Secret files must be ignored by version control.

Owner integration crates may receive the Host-owned secret store from `ServiceRuntimeBuilder` to
rotate a named credential after an authenticated domain flow such as QR login. Rotation is atomic,
updates all shared Host readers, preserves secret redaction, and is unavailable without
`security.secret_file`. Environment-backed secrets remain read-only and reject runtime rotation.

The builder also exposes a Host-owned configured-plugin store when the service was loaded from a
product config file. It atomically replaces only one selected plugin's opaque `config` table; the
owner still validates the domain value. This keeps management flows from creating a second
plugin-private configuration authority.
