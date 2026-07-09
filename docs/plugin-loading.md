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

`jsonl-stdio` is the first Core-connected runner link. ServiceHost wraps the child process with
`mutsuki-runtime-host::JsonlRunner` (`runner.run_batch`). Other external processes can still be
supervised as sidecars, but they are not advertised to Core as executable runners.

Builtin host facades are linked at compile time and enabled through `[plugins].builtin`.
The default ServiceHost build links:

- `mutsuki.conversation.sim` — **dev/mock** conversation control facade (not long-term business state)
- `mutsuki.terminal.tui` — **UI host feature** for the local terminal client (not a runtime effect plugin)

If a requested builtin was not linked into the binary, startup fails with `BuiltinUnavailable`.

`HostPlugin` remains a control-plane facade and must not become a parallel business runtime path.

`plugin_reload` uses the same loader path as startup. The host rescans and validates manifests,
prepares a new Core load-plan generation, drains active runtime work, swaps Core through
`mutsuki-runtime-host`, then replaces the active catalog. External sidecars that are not Core
stdio runners are reconciled after the swap.
