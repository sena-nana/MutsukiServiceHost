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
