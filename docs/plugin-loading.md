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

Product binaries can add native plugins without editing ServiceHost's internal registry by using `ServiceRuntimeBuilder::register_builtin_plugin`. Their native runners must be supplied through `register_builtin_runner` as recreatable factories; the same factories are used for initial boot and generation reload. Product registrations are frozen before Core resolves the runtime profile/load plan.

An orchestration runner that genuinely needs the SDK `RuntimeClient` can use
`register_runtime_client_runner`. The factory is still boot-time only and is
recreated for each registry generation; receiving a client does not authorize
new manifests, protocols, bindings, or runtime runner registration. Ordinary
child work should continue to use `RunnerResult.tasks`; tasks submitted through
the client still enter the normal TaskPool and lease path.

`HostPlugin` remains a control-plane facade and must not become a parallel business runtime path.

`plugin_reload` uses the same loader path as startup. The host rescans and validates manifests,
prepares a new Core load-plan generation, drains active runtime work, swaps Core through
`mutsuki-runtime-host`, then replaces the active catalog. External sidecars that are not Core
stdio runners are reconciled after the swap.
Product event sources are kept across plugin reload; they can be restarted explicitly through the event-source control API.
