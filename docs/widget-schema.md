# Widget state JSON schema

The reviewloop daemon writes `<state_dir>/widget-state.json` on every tick. The
path resolves to `core.widget_state_dir` when configured, otherwise
`core.state_dir`. The macOS Widget extension reads this file and renders the
contents.

## Schema v1 (current)

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `schema_version` | integer | required | Always `1` for v1 documents. The Swift decoder MUST reject documents with a higher version it cannot handle. |
| `generated_at` | RFC3339 UTC timestamp string | required | UTC timestamp of when this snapshot was written. Rust currently emits whole-second timestamps like `2026-05-06T12:00:00Z`. |
| `project_id` | string | required | The daemon's `project_id`. May be empty for legacy installs; v0.2.0+ projects should have one. |
| `summary.active_count` | integer | required | Total active jobs for the project (`QUEUED`, `SUBMITTED`, and `PROCESSING`) before the `active_jobs` display cap is applied. |
| `summary.failed_recent_24h` | integer | required | Count of the capped recent-failure result (the five newest non-cancelled `FAILED`, `FAILED_NEEDS_MANUAL`, or `TIMEOUT` jobs) whose `updated_at` is in the last 24 hours. Excludes user cancellations (`last_error = 'cancelled by user'` or `last_error LIKE 'cancelled by user:%'`). |
| `summary.completed_today` | integer | required | Jobs completed since 00:00 UTC of the current calendar date, computed from `COMPLETED` jobs whose `updated_at` starts with today's UTC date. |
| `active_jobs` | array (max 10) | required | Active jobs for the project. Rust orders by `next_poll_at` ascending with `null` first; equal poll times preserve database order (`created_at` ascending). |
| `active_jobs[].paper_id` | string | required | Paper-id from project config. Swift uses this as the row identity. |
| `active_jobs[].status` | string | required | One of `"QUEUED"`, `"SUBMITTED"`, `"PROCESSING"`. |
| `active_jobs[].attempt` | integer | required | Number of attempts so far. |
| `active_jobs[].next_poll_at` | RFC3339 UTC timestamp string | nullable | When the daemon will next attempt this job. `null` when no poll is scheduled, including queued jobs. |
| `active_jobs[].started_at` | RFC3339 UTC timestamp string | nullable | When the current processing attempt started. `null` for jobs that have not started. |
| `recent_failures` | array (max 5) | required | Recent non-cancelled failures for the project, ordered by `updated_at` descending. |
| `recent_failures[].paper_id` | string | required | Paper-id. Swift uses this as the row identity. |
| `recent_failures[].status` | string | required | One of `"FAILED"`, `"FAILED_NEEDS_MANUAL"`, `"TIMEOUT"`. |
| `recent_failures[].last_error` | string (truncated to 80 Unicode scalar values) | required | Error message snippet. Rust emits `"(unknown error)"` when the database value is null and truncates without appending an ellipsis. |
| `recent_failures[].occurred_at` | RFC3339 UTC timestamp string | required | The failed job's `updated_at` timestamp. |
| `last_tick_at` | RFC3339 UTC timestamp string | nullable | Timestamp of the most recent daemon event for this project. `null` if no event has been recorded. |
| `last_tick_error` | object | nullable | Most recent `tick_failed` event, but only when it is still the latest event and is not older than three minutes. `null` after recovery or when stale. |
| `last_tick_error.at` | RFC3339 UTC timestamp string | required when `last_tick_error` is present | Timestamp of the surfaced `tick_failed` event. |
| `last_tick_error.message` | string | required when `last_tick_error` is present | Error message from the surfaced `tick_failed` event, or `"(no error message)"` if the event payload omitted one. |
| `tick_health` | string | required | One of `"normal"`, `"stale"`, `"stuck"`, `"unknown"`. |

## Sample document

```json
{
  "schema_version": 1,
  "generated_at": "2026-05-06T12:00:00Z",
  "project_id": "test-proj",
  "summary": {
    "active_count": 2,
    "failed_recent_24h": 1,
    "completed_today": 3
  },
  "active_jobs": [
    {
      "paper_id": "paper-a",
      "status": "PROCESSING",
      "attempt": 2,
      "next_poll_at": "2026-05-06T12:05:00Z",
      "started_at": "2026-05-06T11:50:00Z"
    }
  ],
  "recent_failures": [
    {
      "paper_id": "paper-b",
      "status": "FAILED",
      "last_error": "rate limit exceeded",
      "occurred_at": "2026-05-06T11:55:00Z"
    }
  ],
  "last_tick_at": "2026-05-06T11:59:50Z",
  "last_tick_error": {
    "at": "2026-05-06T11:59:55Z",
    "message": "daemon lost connection"
  },
  "tick_health": "normal"
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
