# Control API

The control API is a JSON line request/response protocol.

Request:

```json
{"token":"...","method":"service_status","params":null}
```

Response:

```json
{"ok":true,"result":{}}
```

Error:

```json
{"ok":false,"error":{"code":"unsupported","message":"task.list snapshot is not supported by the current runtime API"}}
```

## Methods

- `service_status`
- `service_shutdown`
- `core_status`
- `plugin_list`
- `plugin_reload`
- `plugin_call`
- `runner_list`
- `runner_restart`
- `runner_stop`
- `task_list`
- `task_cancel`
- `health_check`
- `log_tail`

Unsupported methods are intentionally explicit where the current Core host API has no safe backing operation.

`plugin_reload` rescans configured plugin directories, validates manifests, prepares the next Core load plan generation, drains active runner work, swaps Core to the new generation, then replaces the ServiceHost catalog. Sidecar runners are reconciled after the Core swap; sidecar start/stop errors are returned in `runner_errors` because a successful Core generation swap is not rolled back.

Successful reload response:

```json
{
  "previous_generation": 1,
  "registry_generation": 2,
  "plugin_count": 3,
  "changes": [
    {"surface_id":"runner:example","compatibility":"additive"}
  ],
  "runner_errors": []
}
```

`plugin_call` dispatches to loaded builtin host plugins:

```json
{"plugin_id":"mutsuki.conversation.sim","operation":"send","payload":{"message":"hello"}}
```

`log_tail` reads the configured service log file and returns `{ "cursor": 0, "entries": [] }`.
Pass the returned cursor on the next request for incremental reads. Filters are rejected until a
backed filtering implementation exists.
