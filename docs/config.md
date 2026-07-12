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
