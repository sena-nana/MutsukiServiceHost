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
{"ok":false,"error":{"code":"failed","message":"core is not running"}}
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
- `event_source_list`
- `event_source_restart`
- `task_list`
- `task_cancel`
- `task_outcome`
- `health_check`
- `log_tail`

Unsupported methods are intentionally explicit where the current Core host API has no safe backing operation.

`task_list` returns live operational snapshots from the host runtime. Task payloads and detailed
runtime error evidence are intentionally omitted from the control response.

Control responses keep string `task_id` fields for clients. Internally, `task_cancel` and
`task_outcome` reconstruct a Core `TaskHandle` from live task snapshots before calling
`HostRuntimeCommand::CancelTask` / `TaskOutcome`.

Successful task list response:

```json
[
  {
    "task_id": "task-1",
    "protocol_id": "raw.input",
    "status": "ready",
    "priority": 0,
    "ready_at_step": null,
    "created_sequence": 1,
    "registry_generation": 1,
    "target_binding_id": null,
    "runner_hint": null,
    "claimed_by": null,
    "owner_runner": null,
    "lease_id": null,
    "trace_id": null,
    "correlation_id": null,
    "input_refs": [],
    "output_ref": null,
    "continuation_ref": null,
    "required_surfaces": [],
    "failure": null
  }
]
```

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
  "runner_errors": [],
  "event_sources": "kept"
}
```

`event_source_list` returns source id, plugin id, instance id, lifecycle state, health, last error, reconnect count, and last successful task-submission time. `event_source_restart` accepts `{ "id": "source-id" }`. Event sources are product-scoped and remain running during `plugin_reload`; the response makes this explicit with `"event_sources":"kept"`.

The generic CLI exposes the same authenticated operations as
`mutsuki-service event-source list` and
`mutsuki-service event-source restart <source-id>`.

`plugin_call` dispatches to loaded host control facades. These facades are not a parallel business
runtime path; Core task/resource work must go through `HostContext`.

`task_outcome` returns a control-plane view of Core `TaskOutcome` for a task id:

```json
{"id":"task-1"}
```

Successful outcome response:

```json
{"task_id":"task-1","status":"cancelled","output_ref":null,"reason":null,"error_code":null}
```

Non-terminal tasks return `"status":"pending"`.

`log_tail` reads the configured service log file and returns `{ "cursor": 0, "entries": [] }`.
Pass the returned cursor on the next request for incremental reads. Filters are rejected until a
backed filtering implementation exists.
