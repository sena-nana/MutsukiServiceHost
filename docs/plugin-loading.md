# Plugin Loading

Dynamic plugin files are named `plugin.toml`. The `[manifest]` table maps to `mutsuki_runtime_contracts::PluginManifest`.

ServiceHost validates:

- `api_version == "mutsuki-plugin-v1"`
- artifact type matches deployment kind
- disabled plugin marker is absent
- runtime env keys do not contain secret-like names such as `TOKEN`, `SECRET`, or `PASSWORD`

External runtimes use:

```toml
[runtime]
command = "runner-binary"
args = []
env = {}
cwd = "optional-working-directory"
runner_link = "jsonl-stdio"
```

`jsonl-stdio` is the first Core-connected runner link. Other external processes can still be supervised as sidecars, but they are not advertised to Core as executable runners.

Builtin host plugins are linked at compile time and enabled through `[plugins].builtin`.
The default ServiceHost build links:

- `mutsuki.conversation.sim`
- `mutsuki.terminal.tui`

If a requested builtin was not linked into the binary, startup fails with `BuiltinUnavailable`.
