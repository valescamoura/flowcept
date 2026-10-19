# `flowcept.flowceptor.adapters.galaxy`

Observes [Galaxy](https://galaxyproject.org) workflow invocations through the
Galaxy API (via `bioblend`, `pip install flowcept[galaxy]`).

- Each finished invocation becomes a `WorkflowObject`
  (`workflow_id = galaxy:<host>:invocation:<id>`).
- Each job of the invocation becomes a `TaskObject`
  (`task_id = galaxy:<host>:job:<id>`, `activity_id = tool_id`), with the job
  parameters and input datasets in `used`, output datasets in `generated`,
  Galaxy job metrics in `custom_metadata.metrics`, and `dependencies` pointing
  to the jobs that produced its inputs.
- An invocation is finished when it is `failed`/`cancelled`, or `scheduled`/
  `completed` (depending on the Galaxy release) with all its jobs in a terminal state (`ok`, `error`, `failed`, `deleted`,
  `skipped` or `paused`; Galaxy pauses the jobs downstream of a failed job).
  Unfinished invocations are retried on the next poll. A paused job resumed
  later by a user is not captured again.

## Settings

```yaml
adapters:
  galaxy:
    kind: galaxy
    url: http://localhost:8080
    api_key: ${oc.env:GALAXY_API_KEY,null}
    verify: true            # or the path of a CA bundle
    watch_interval_sec: 30
    invocations_limit: 100  # most recent invocations checked per poll
    state_path: galaxy_interceptor_state.json
```

The ids of the intercepted invocations are kept in `state_path` (no Redis
needed). The state is written in `stop()`, after the buffer is flushed.

## Context resolver

The interceptor only knows Galaxy. An application can attach its own context
(e.g. the campaign an invocation belongs to) by passing a callable:

```python
def resolve(invocation: dict, stored_workflow: dict) -> dict:
    return {"campaign_id": "...", "parent_workflow_id": "..."}

interceptor = GalaxyInterceptor(context_resolver=resolve)
```

`campaign_id` is applied to the workflow and tasks; `parent_workflow_id` to
the workflow. Without a resolver both are left empty.

## Registering the invocation before it finishes

`invocation_workflow_id(galaxy_url, invocation_id)` returns the workflow id
the interceptor uses. An application that triggers invocations can send its
own `WorkflowObject` with that id right away (e.g. with its campaign and
parent workflow); the interceptor later adds the jobs as tasks of the same
workflow, and its own workflow record completes the first one.

## Offline mode

With `db_flush_mode: offline`, records are written to the dump buffer file
only when the interceptor stops. Run it in cycles (`start()` then `stop()`)
so each cycle appends what finished meanwhile.
