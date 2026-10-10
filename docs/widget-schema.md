# Widget state JSON schema

The reviewloop supervisor (`reviewloop daemon run`) writes
`<state_dir>/widget-state.json` after every tick. The path resolves to
`core.widget_state_dir` when configured, otherwise `core.state_dir`. The macOS
Widget extension reads this file and renders the contents.

One document covers the whole machine: every project's jobs, the supervisor's
state, and the health of each registered project. Only the supervisor writes
it; no other command does.

## Schema v1 (current)

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `schema_version` | integer | required | Always `1` for v1 documents. The Swift decoder MUST reject documents with a higher version it cannot handle. |
| `generated_at` | RFC3339 UTC timestamp string | required | UTC timestamp of when this snapshot was written. Rust currently emits whole-second timestamps like `2026-05-06T12:00:00Z`. |
| `project_id` | string | required | Always `""`: the document covers every project. Kept for decoders that require it. |
| `summary.active_count` | integer | required | Total active jobs across every project (`QUEUED`, `SUBMITTED`, and `PROCESSING`) before the `active_jobs` display cap is applied. |
| `summary.failed_recent_24h` | integer | required | Count of the recent-failure result (the five newest non-cancelled `FAILED`, `FAILED_NEEDS_MANUAL`, or `TIMEOUT` jobs of each project) whose `updated_at` is in the last 24 hours. Excludes user cancellations (`last_error = 'cancelled by user'` or `last_error LIKE 'cancelled by user:%'`). |
| `summary.completed_today` | integer | required | Jobs of every project completed since 00:00 UTC of the current calendar date, computed from `COMPLETED` jobs whose `updated_at` starts with today's UTC date. |
| `active_jobs` | array (max 10) | required | Active jobs across every project. Rust orders by `next_poll_at` ascending with `null` first; equal poll times preserve database order. |
| `active_jobs[].project_id` | string | required | The job's project. |
| `active_jobs[].paper_id` | string | required | Paper-id from project config. Unique only within a project: identify a row by `project_id` and `paper_id`. |
| `active_jobs[].status` | string | required | One of `"QUEUED"`, `"SUBMITTED"`, `"PROCESSING"`. |
| `active_jobs[].attempt` | integer | required | Number of attempts so far. |
| `active_jobs[].next_poll_at` | RFC3339 UTC timestamp string | nullable | When the supervisor will next attempt this job. `null` when no poll is scheduled, including queued jobs. |
| `active_jobs[].started_at` | RFC3339 UTC timestamp string | nullable | When the current processing attempt started. `null` for jobs that have not started. |
| `recent_failures` | array (max 5) | required | The newest non-cancelled failures across every project, ordered by `updated_at` descending. |
| `recent_failures[].project_id` | string | required | The job's project. |
| `recent_failures[].paper_id` | string | required | Paper-id; unique only within a project. |
| `recent_failures[].status` | string | required | One of `"FAILED"`, `"FAILED_NEEDS_MANUAL"`, `"TIMEOUT"`. |
| `recent_failures[].last_error` | string (truncated to 80 Unicode scalar values) | required | Error message snippet. Rust emits `"(unknown error)"` when the database value is null and truncates without appending an ellipsis. |
| `recent_failures[].occurred_at` | RFC3339 UTC timestamp string | required | The failed job's `updated_at` timestamp. |
| `last_tick_at` | RFC3339 UTC timestamp string | required | When the supervisor last ticked (paused ticks included). Never `null`. |
| `last_tick_error` | object | nullable | The machine-level failure of the latest tick (the mailbox, retention, the global config). `null` when that tick had none; per-project failures are in `projects[].last_error`. |
| `last_tick_error.at` | RFC3339 UTC timestamp string | required when `last_tick_error` is present | When that tick finished. |
| `last_tick_error.message` | string | required when `last_tick_error` is present | The failure. |
| `tick_health` | string | required | Age of `last_tick_at`: `"normal"` (under 60 s), `"stale"` (under 300 s) or `"stuck"`. |
| `supervisor.state` | string | required | `"running"`, `"paused"` or `"stopped"`. |
| `supervisor.pid` | integer | nullable | Process id of the supervisor that last started. |
| `supervisor.started_at` | RFC3339 UTC timestamp string | nullable | When it started. |
| `supervisor.paused_at` | RFC3339 UTC timestamp string | nullable | When `reviewloop daemon pause` paused it; `null` when not paused. |
| `projects` | array | required | Every registered project, ordered by `project_id`. |
| `projects[].project_id` | string | required | The project. |
| `projects[].enabled` | boolean | required | Whether the supervisor runs it (`reviewloop project enable`). |
| `projects[].state` | string | required | `"disabled"`, `"pending"` (enabled, not run yet), `"ok"` or `"error"`. |
| `projects[].active_count` | integer | required | Its active jobs. |
| `projects[].last_run_at` | RFC3339 UTC timestamp string | nullable | The supervisor's last pass over it. |
| `projects[].last_ok_at` | RFC3339 UTC timestamp string | nullable | Its last pass without errors. |
| `projects[].last_error` | string (truncated to 80 Unicode scalar values) | nullable | The error of its last pass, when that pass failed. |

The `project_id` fields, `supervisor` and `projects` were added for the
multi-project supervisor (OSS-338). They are additive, so the version stays 1.

## Sample document

```json
{
  "schema_version": 1,
  "generated_at": "2026-05-06T12:00:00Z",
  "project_id": "",
  "summary": {
    "active_count": 2,
    "failed_recent_24h": 1,
    "completed_today": 3
  },
  "active_jobs": [
    {
      "project_id": "thesis",
      "paper_id": "paper-a",
      "status": "PROCESSING",
      "attempt": 2,
      "next_poll_at": "2026-05-06T12:05:00Z",
      "started_at": "2026-05-06T11:50:00Z"
    }
  ],
  "recent_failures": [
    {
      "project_id": "thesis",
      "paper_id": "paper-b",
      "status": "FAILED",
      "last_error": "rate limit exceeded",
      "occurred_at": "2026-05-06T11:55:00Z"
    }
  ],
  "last_tick_at": "2026-05-06T11:59:50Z",
  "last_tick_error": {
    "at": "2026-05-06T11:59:50Z",
    "message": "email token ingestion: offline"
  },
  "tick_health": "normal",
  "supervisor": {
    "state": "running",
    "pid": 4242,
    "started_at": "2026-05-06T09:00:00Z",
    "paused_at": null
  },
  "projects": [
    {
      "project_id": "thesis",
      "enabled": true,
      "state": "ok",
      "active_count": 2,
      "last_run_at": "2026-05-06T11:59:50Z",
      "last_ok_at": "2026-05-06T11:59:50Z",
      "last_error": null
    }
  ]
}
```

## Schema bump procedure

When evolving to v2:

1. Add new fields with default values that older Swift can ignore.
2. Bump `schema_version` to `2` only if a breaking change is unavoidable.
3. Update Swift's `WidgetState.swift` to handle BOTH versions (decode based on `schemaVersion` field; provide null defaults for missing v2 fields when reading v1).
4. Ship the Swift widget update **before** the Rust daemon starts emitting v2 documents. Order matters: the user must update the widget app before the daemon writes incompatible JSON.
5. After enough time has passed, drop v1 support from Swift and update this doc.

## Cross-platform notes

- All timestamps are RFC3339 in UTC. Swift parses with `ISO8601DateFormatter`.
- Empty arrays MUST be `[]`, not `null`, to keep Swift's array decoders happy.
- snake_case in JSON; Swift uses `CodingKeys` to map to camelCase struct fields.
