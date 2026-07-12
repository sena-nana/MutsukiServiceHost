# Config

Config load order:

```text
default config
profile config
local service.toml
environment variables
CLI overrides
```

Default home:

```text
~/.mutsuki
```

Important directories:

- `data`
- `logs`
- `plugins`
- `run`

If no control token is provided through config, `MUTSUKI_CONTROL_TOKEN`, or CLI `--token`, ServiceHost creates `<home>/run/control.token` and reuses it for local clients.
# Configured native plugins

Linked native plugins may be selected without adding domain fields to `ServiceConfig`:

```toml
[[plugins.configured]]
id = "owner.plugin.id"
enabled = true

[plugins.configured.config]
mode = "owner-defined"
credential_key = "HOST_SECRET_KEY"
```

The product binary must register the matching `ConfiguredPluginFactory`. Unknown or duplicate
IDs fail startup. The nested config is opaque to ServiceHost, but raw `secret`, `token`,
`password` and `api_key` values are rejected; use owner-defined `_key` or `_ref` fields resolved
at the Host boundary.
