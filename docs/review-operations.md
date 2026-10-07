# Review operations contract

`reviewloop::application` implements the review operations that the CLI uses
today and that the MCP adapter and the shared agent skill build on. This
document fixes the operation names, the request and result fields, the error
codes and the lifecycle semantics those consumers MAY rely on. The Rust entry
point is `ReviewOps::new(&config, &db)`; `tests/application_ops.rs` exercises
every operation against a temporary config and database.

## Ground rules

- Operations are synchronous and touch only the loaded `Config`, the SQLite
  database and the artifact directory. They never print, never exit the
  process and never contact a review provider. State changes emit `tracing`
  events at INFO; the host's logging configuration decides where they go
  (the CLI's default is stdout, so an MCP host MUST log to stderr).
- Requesting a review is a **persistent enqueue**. Sending the PDF to the
  provider and polling for the result belong to the worker (the daemon) or to
  a caller that executes a job immediately, as `reviewloop submit` does.
- Status queries read the database only. Refreshing a job from the provider
  (`reviewloop check`) is a separate, network-bound action outside this
  contract.
- Results have no token or submitter-email fields and never include raw
  event payloads or `meta.json`. A job's `last_error` has the job's token
  replaced by `[redacted]`; review text and review JSON also have the token
  the review was fetched with replaced.
- JSON is snake_case. Timestamps are RFC3339 UTC strings ending in `Z`; they
  MAY carry fractional seconds. Lists are `[]`, never `null`; absent optional
  values are `null`.
- Tool names and error codes are stable. Fields MAY be added; renaming or
  removing one requires updating this document and its tests together.

## Project and job identity

| Concept | Semantics |
|---|---|
| Project | The `project_id` of a `reviewloop.toml`. `ReviewOps` acts for its config's `project_id`. |
| Unscoped context | A config with an empty `project_id` (no `reviewloop.toml`, as for the menu bar). `get_job`, `get_review` and `cancel_job` by job id then search every project; `retry_job` by job id fails with `project_mismatch` for a job that has a project; `list_papers`, `request_review`, `list_jobs`, `approve_job` and every paper reference fail with `project_required`. A scoped context reports another project's job id as `job_not_found`. |
| Registry | Every `reviewloop.toml` the CLI loads is recorded with its path. `list_projects` reads it. Loading another project's config from that path is the adapter's job, not an operation. |
| `paper_id` | Unique within a project; configured in `[[papers]]`. |
| `job_id` | A UUID, unique across projects. Jobs are scoped to their project. |
| Job reference | A job id, or a paper id. A paper reference needs a project and resolves to the single job of that paper whose status the operation accepts (newest `updated_at` first). Zero matches is `no_eligible_job`; several is `ambiguous_job`. |
| Manuscript version | `pdf_hash` is the SHA-256 of the PDF bytes hashed at request time. Without a git commit, `version_source` is `"pdf_hash"` and `version_key` equals `pdf_hash`. `version_no` increases when a paper's version key changes; `round_no` is one past the highest round held by a pending, in-flight or completed job of that version (failed attempts give their round back). |

## Operations

| Tool | Rust | Read-only | Input | Result | Side effects |
|---|---|---|---|---|---|
| `list_projects` | `ReviewOps::list_projects` | yes | none | `ProjectView[]` | none |
| `list_papers` | `ReviewOps::list_papers` | yes | none (needs a project) | `PaperView[]` | none |
| `request_review` | `ReviewOps::request_review` | no | `ReviewRequest` (needs a project) | `ReviewRequestOutcome` | Created: a job row and one `job_enqueued` event (`source` `manual_submit`, `run` or `agent_request`; `enqueue_mode`; `request_key`), plus with `force` a `force_clear_cooldown` event per other active job of the paper. Existing because covered: a `duplicate_skipped` event. Existing because the key was replayed: nothing. |
| `get_job` | `ReviewOps::get_job` | yes | `job_id` | `JobView` | none |
| `list_jobs` | `ReviewOps::list_jobs` | yes | `JobListQuery` (needs a project) | `JobList` | none |
| `get_review` | `ReviewOps::get_review` | yes | `ReviewQuery` | `ReviewView` | none |
| `approve_job` | `ReviewOps::approve_job` | no | job reference (needs a project) | `TransitionOutcome` | PENDING_APPROVAL → QUEUED; `approved` event `{}`. |
| `retry_job` | `ReviewOps::retry_job` | no | `RetryRequest` | `RetryOutcome` | Re-queues the job (see Retry semantics); `retried` event `{}`, or `manual_rate_limit_override` when forced. |
| `cancel_job` | `ReviewOps::cancel_job` | no | `CancelRequest` | `TransitionOutcome` | Non-terminal → FAILED with `last_error` `"cancelled by user"` or `"cancelled by user: <reason>"`; `cancelled` event `{reason, previous_status}`. The provider is not contacted. |

`ReviewOps::find_job(job_ref, eligibility)` is the shared resolver behind the
job references above. It is a Rust helper, not a tool.

### Inputs

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `ReviewRequest.paper_id` | string | required | A configured paper. Its current PDF is hashed. |
| `ReviewRequest.request_key` | string | nullable | Idempotency key, scoped to the project. The first request binds it to the job it resolves to together with the review identity derived then. A replay derives the identity again from the paper's current PDF and config: while it matches and the job exists, the replay returns that job (`reason` `request_replay`), even once the job has finished. If the PDF, venue or backend changed since, the replay is `request_conflict`; if the paper or its PDF is gone, it is `paper_not_found` / `pdf_not_found`. A blank key is `invalid_request`. A new review round needs a new key. |
| `ReviewRequest.force` | boolean | required | Start a new review round even when a job covers this manuscript, and clear the cooldown (`attempt`, `next_poll_at`) of the paper's other QUEUED, SUBMITTED and PROCESSING jobs. A replayed `request_key` still wins. |
| `ReviewRequest.approval` | `Granted` or `Required` | required | `Granted`: the job starts QUEUED. `Required`: it starts PENDING_APPROVAL and is not sent while pending: only `approve_job` moves it on (`retry_job` refuses it; `cancel_job` ends it). Adapters MUST choose explicitly; the CLI always grants. Neither value changes an `existing` job. |
| `ReviewRequest.origin` | `Submit`, `Run` or `Agent` | required | Recorded as `source` on the `job_enqueued` / `duplicate_skipped` event (`manual_submit`, `run`, `agent_request`) and as `from_command` on `force_clear_cooldown`. Agents use `Agent`. |
| `JobListQuery.paper_id` | string | nullable | Restrict to one paper. |
| `JobListQuery.active_only` | boolean | required | Keep PENDING_APPROVAL, QUEUED, SUBMITTED and PROCESSING jobs. |
| `JobListQuery.limit` | integer | nullable | Default 50, clamped to 1..=200. Newest jobs first. |
| `ReviewQuery.job_id` | string | required | The job whose review to read. |
| `ReviewQuery.part` | `Summary`, `Markdown`, `Section(name)` or `Raw` | required | `Summary`: metadata and section names only. `Markdown` (default): the rendered review. `Section`: one section's text. `Raw`: the provider's review JSON. |
| `RetryRequest.job` | job reference | required | Paper references match QUEUED, SUBMITTED and PROCESSING jobs. |
| `RetryRequest.include_failed` | boolean | required | Paper references also match FAILED, FAILED_NEEDS_MANUAL and TIMEOUT jobs. |
| `RetryRequest.force` | boolean | required | Make the job due now instead of following the schedule. |
| `RetryRequest.caller_executes` | boolean | required | With `force`, the caller submits or polls the job itself right after the call (`reviewloop retry --force`). A forced poll then leaves `next_poll_at` unchanged so no worker polls the job concurrently. Adapters that leave execution to the worker pass `false`. |
| `CancelRequest.job` | job reference | required | Paper references match PENDING_APPROVAL, QUEUED, SUBMITTED and PROCESSING jobs. |
| `CancelRequest.reason` | string | nullable | Recorded in `last_error` and the `cancelled` event. |

## Job lifecycle

| `status` | `phase` | Meaning | `next_poll_at` |
|---|---|---|---|
| `PENDING_APPROVAL` | `awaiting_approval` | Stored; waits for `approve_job`. Not sent while pending. | `null` |
| `QUEUED` | `queued` | Stored locally and **not** accepted by the provider. A worker submits it. | Earliest submission attempt (set after a rate limit); `null` means the next worker tick. |
| `PROCESSING` | `submitted` | The provider accepted the PDF and returned a token (`has_token`). The worker polls for the review. | Next provider poll. |
| `SUBMITTED` | `submitted` | Legacy status that current workers never write (they move QUEUED straight to PROCESSING) and never resume; such a job usually has no token. `retry_job` re-queues it. | As stored. |
| `COMPLETED` | `completed` | The review is stored; `get_review` returns it. | `null` |
| `FAILED`, `FAILED_NEEDS_MANUAL`, `TIMEOUT` | `failed` | The attempt ended. `last_error` explains it; `retry_job` can re-queue it. | `null` |
| `FAILED` with a `cancelled by user` error | `cancelled` | Cancelled locally. A submission the provider already accepted (`has_token`) is not revoked; its result is no longer collected. | `null` |

A `created` request therefore returns a `queued` (or `awaiting_approval`) job,
never a `submitted` one. An `existing` request returns the covering job in
whatever status it has; a `Granted` request does not approve an existing
PENDING_APPROVAL job, so check `job.status` and call `approve_job` if
needed. The worker leaves a job
alone until its `next_poll_at`, or until its next tick (about 30 seconds) when
that is `null`; callers SHOULD NOT check an active job more often than that.

### Retry semantics

| `force` | Token | Accepted statuses | Result | `action` |
|---|---|---|---|---|
| no | yes | any but PENDING_APPROVAL | PROCESSING, `attempt` 0, `next_poll_at` = first schedule step, `last_error` cleared | `poll_scheduled` |
| no | no | any but PENDING_APPROVAL | QUEUED, `attempt` 0, `next_poll_at` `null`, `last_error` cleared | `submission_queued` |
| yes | yes | PROCESSING | PROCESSING, `next_poll_at` = now (unchanged with `caller_executes`); `attempt` and `last_error` kept | `poll_now` |
| yes | no | QUEUED, SUBMITTED, FAILED, FAILED_NEEDS_MANUAL, TIMEOUT | QUEUED, `attempt` 0, `next_poll_at` `null`, `last_error` cleared | `submit_now` |

A PENDING_APPROVAL job is `invalid_state`: it needs `approve_job`. Terminal
jobs, cancelled ones included, may be retried; retrying is an explicit re-run,
so a job cancelled while pending and then retried is queued without a separate
approval. The schedule comes from the
`ReviewOps` config, which MUST be the job's own project; an unscoped context
reports a job that has a project as `project_mismatch`. `reviewloop retry
--force` performs the `poll_now` / `submit_now` step immediately; any other
caller leaves it to the worker's next tick.

## Results

### JobView

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `job_id` | string | required | Job UUID. |
| `project_id` | string | required | Owning project. Empty only for legacy jobs. |
| `paper_id` | string | required | Paper the job reviews. |
| `backend` | string | required | Review provider, e.g. `"stanford"`. |
| `status` | string | required | One of `"PENDING_APPROVAL"`, `"QUEUED"`, `"SUBMITTED"`, `"PROCESSING"`, `"COMPLETED"`, `"FAILED"`, `"FAILED_NEEDS_MANUAL"`, `"TIMEOUT"`. |
| `phase` | string | required | One of `"awaiting_approval"`, `"queued"`, `"submitted"`, `"completed"`, `"failed"`, `"cancelled"` (see the lifecycle table). |
| `terminal` | boolean | required | `true` for COMPLETED, FAILED, FAILED_NEEDS_MANUAL and TIMEOUT. |
| `has_token` | boolean | required | The provider acknowledged a submission. The token itself is never returned. |
| `review_available` | boolean | required | A review is stored; `get_review` succeeds. |
| `review_completed_at` | RFC3339 UTC timestamp string | nullable | When the review was stored. |
| `attempt` | integer | required | Attempts in the current submit or poll cycle. |
| `pdf_path` | string | required | PDF path recorded when the job was created. |
| `pdf_hash` | string | required | SHA-256 of the PDF hashed when the job was created. |
| `venue` | string | nullable | Venue recorded at request time. When it is null, a stanford worker sends the paper's configured venue at submission time, which this field does not show. |
| `version_no` | integer | required | Manuscript version number within the paper. |
| `round_no` | integer | required | Review round within the version. |
| `version_source` | string | required | `"pdf_hash"` or `"git_commit"`. |
| `version_key` | string | required | The hash or commit identifying the version. |
| `git_tag` | string | nullable | Tag that triggered the job, if any. |
| `git_commit` | string | nullable | Commit of that tag, if any. |
| `fallback_used` | boolean | required | The browser fallback submitted the job. |
| `last_error` | string | nullable | Last failure, token-redacted. |
| `created_at` | RFC3339 UTC timestamp string | required | Creation time. |
| `updated_at` | RFC3339 UTC timestamp string | required | Last state change. |
| `started_at` | RFC3339 UTC timestamp string | nullable | When the provider first accepted the job. |
| `next_poll_at` | RFC3339 UTC timestamp string | nullable | See the lifecycle table. |

### Other results

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `ReviewRequestOutcome.disposition` | string | required | `"created"`: a new job was stored. `"existing"`: an earlier job answers the request and is returned; no job was stored. |
| `ReviewRequestOutcome.reason` | string | nullable | For `existing`: `"request_replay"` (the request key was already bound to this job) or `"covered"` (a pending, in-flight or completed job has the same review identity). `null` for `created`. |
| `ReviewRequestOutcome.job` | `JobView` | required | The created or existing job. |
| `ReviewRequestOutcome.input` | `ManuscriptInput` | required | What this request asked to review: `paper_id`, `pdf_path`, `pdf_hash`, `backend`, `venue` (trimmed, `null` when unset), `version_source`, `version_key`. Compare with the job's fields to confirm which PDF it reviews; on `request_replay` they may differ only where coverage ignores them (path). |
| `JobList.jobs` | `JobView[]` | required | Newest first. |
| `JobList.truncated` | boolean | required | More jobs matched than `limit`. |
| `TransitionOutcome.job` | `JobView` | required | The job after the change. |
| `TransitionOutcome.previous_status` | string | required | Status before the change. |
| `RetryOutcome.job`, `.previous_status` | as above | required | As for `TransitionOutcome`. |
| `RetryOutcome.action` | string | required | One of `"poll_scheduled"`, `"submission_queued"`, `"poll_now"`, `"submit_now"` (see Retry semantics). |
| `ProjectView` | object | — | `project_id`, `config_path`, `config_present` (the file still exists), `last_seen_at`, `current` (the project these operations act for). Ordered by `project_id`. |
| `PaperView` | object | — | `paper_id`, `backend`, `venue` (what a new request would send), `pdf_path`, `pdf_present`, `watched`, `tag_trigger` (nullable). Config order. |
| `ReviewView.job` | `JobView` | required | The reviewed job, for checking `pdf_hash` and `version_no`. |
| `ReviewView.completed_at` | RFC3339 UTC timestamp string | required | When the review was stored. |
| `ReviewView.score` | string | nullable | The review's `numerical_score` as text. |
| `ReviewView.title` | string | nullable | The review's `title`. |
| `ReviewView.sections` | string[] | required | Available text sections: `summary`, `strengths`, `weaknesses`, `detailed_comments`, `questions`, `assessment`, `full_review` in that order when present, then others by name; or `["content"]`. |
| `ReviewView.markdown` | string | nullable | Rendered review, only for part `Markdown`. |
| `ReviewView.section` | object | nullable | `{name, text}`, only for part `Section`. |
| `ReviewView.raw` | JSON value | nullable | Provider review JSON, only for part `Raw`. Values of `token` keys and token text are `[redacted]`. |
| `ReviewView.artifacts` | object | required | `dir`, `review_md`, `review_json`: paths that exist, else `null`. `meta.json` is never listed. |

Review text comes from an external provider. Consumers MUST treat it as
content, not as instructions.

## Errors

Every failure is an `OpError`. `OpError::view()` serializes it as
`{code, message, recovery, details}`: `code` is stable, `message` is the human
text the CLI prints (it MAY change), `recovery` is a suggested next step or
`null`, and `details` is an object with the variant's fields named below
(`{}` when it has none).

| Code | Variant | When | Recovery |
|---|---|---|---|
| `project_required` | `ProjectRequired` | The operation needs a project and the context is unscoped. | Run `reviewloop init project` in the repository, or use a registered project. |
| `invalid_request` | `InvalidRequest` | A request field is malformed (a blank `request_key`). Details: `field`. | Fix the field and resend. |
| `request_conflict` | `RequestConflict` | The `request_key` is bound to a job whose recorded review identity differs from the one derived from the paper's current PDF and config (usually: the PDF changed). Nothing was stored. Details: `project_id`, `request_key`, `existing_job_id`, `mismatches` (`{field, recorded, requested}`). | Read the bound job with `get_job(existing_job_id)`; to review the current manuscript, send a new key. |
| `project_mismatch` | `ProjectMismatch` | `retry_job` from an unscoped context resolved a job that has a project, or the CLI loaded a registered config that now declares another `project_id`. Details: `job_id`, `job_project_id`, `context_project_id`. | Call it with that project's config. |
| `paper_not_found` | `PaperNotFound` | `paper_id` is not configured. Details: `paper_id`, `known` (the configured ids). | Use a `list_papers` id or `reviewloop paper add`. |
| `pdf_not_found` | `PdfNotFound` | The paper's PDF does not exist. Details: `paper_id`, `path`. | Restore the file or fix `pdf_path`. |
| `submitter_email_unavailable` | `SubmitterEmailUnavailable` | The backend needs a submitter email and none is configured. Details: `backend`. | Set `providers.stanford.email` or run `reviewloop email login`. |
| `job_not_found` | `JobNotFound` | No job with that id in scope. Details: `job_id`. | Check the id; `list_jobs`. |
| `no_eligible_job` | `NoEligibleJob` | A paper reference matched no job in an accepted status. Details: `paper_id`, `action`, `statuses`. | `list_jobs`, then pass a job id. |
| `ambiguous_job` | `AmbiguousJob` | A paper reference matched several jobs. Details: `paper_id`, `action`, `candidates` (`{job_id, status}`). | Pass one candidate's job id. |
| `invalid_state` | `InvalidState` | The job's status does not allow the operation (approve a non-pending job, cancel a finished one, retry a PENDING_APPROVAL job, force-retry an unsupported status). Details: `job_id`, `status`, `operation` (tool name). | `get_job`; see the operation's accepted statuses; a PENDING_APPROVAL job needs `approve_job`. |
| `review_not_available` | `ReviewNotAvailable` | No review is stored for the job. Details: `job_id`, `status`. | Active job: wait for COMPLETED and re-check after `next_poll_at`. Terminal job: it ended without a review; `retry_job` or `request_review`. |
| `section_not_found` | `SectionNotFound` | The requested section does not exist. Details: `job_id`, `section`, `available`. | Request an available section or the markdown. |
| `internal` | `Internal` | Database, filesystem or other unexpected failure. | None; the message carries the cause. |

## Duplicate requests and idempotency

`request_review` goes through `Db::enqueue`, which resolves a request in one
`BEGIN IMMEDIATE` transaction: request-key lookup, coverage check, version and
round allocation, job insert, key binding and the `job_enqueued` event.
Concurrent requests from separate connections or processes therefore never
both create a job for the same request.

- **Request key.** A caller that may retry (an agent after a dropped
  response, a reconnecting MCP client) SHOULD send a `request_key`. Replaying
  it returns the bound job with reason `request_replay`, whatever that job's
  status, without starting a round or clearing cooldowns, as long as the
  identity derived from the paper's current PDF and config still matches;
  otherwise it is `request_conflict` (see the error table).
- **Coverage.** Without a key, or for a new key, a non-forced request whose
  review identity (paper, backend, PDF hash, venue, version key) matches a
  PENDING_APPROVAL, QUEUED, SUBMITTED, PROCESSING or COMPLETED job returns that
  job with reason `covered`. FAILED, FAILED_NEEDS_MANUAL and TIMEOUT jobs
  cover nothing, so a request after a failure creates a new job. The PDF path
  and the submitter email are not part of the identity.
- **New round.** `force` skips the coverage check and always creates a job in
  a new review round.

## CLI mapping

| Command | Operation | What stays in the CLI |
|---|---|---|
| `submit` | `request_review` (origin `Submit`, approval granted, `--request-key`, `--force`) | On `existing`, print `Skipped submit: <reason> existing job …` and exit 0. On `created`, submit the job to the provider immediately and pull its first poll forward to about 60 seconds. |
| `paper add --submit-now` | as `submit` | |
| `run` | `request_review` (origin `Run`, `force`) | Paper registration, immediate submission, the foreground polling loop and its exit codes. |
| `approve` | `approve_job` | Printing `Approved job …`. |
| `retry` | `find_job`, then `retry_job` with the job's project config (`caller_executes` = `--force`) | Loading a foreign project's config from the registry; with `--force`, the immediate poll or submit. A PENDING_APPROVAL job is refused. |
| `cancel` | `cancel_job` | The `hint:` line for a paper with no active job. |
| `complete --paper-id` | `find_job` | Everything else. |
| `status`, `check`, `import-token`, `complete` | not routed | Unchanged. `check` is the network refresh. |

Exit codes are unchanged. Every command exits 0 on success, 1 on an error
(message on stderr) and 2 on a usage error. In addition, `run` exits 2 when the
job ends FAILED, FAILED_NEEDS_MANUAL or TIMEOUT and 130 on Ctrl-C, and
`import-token` exits 2 when its immediate poll fails.

## Sample documents

`request_review` on a fresh paper:

```json
{
  "disposition": "created",
  "reason": null,
  "job": {
    "job_id": "5b0b8f1e-3c55-4b3f-9a52-1d3f2b7c9e10",
    "project_id": "my-paper",
    "paper_id": "main",
    "backend": "stanford",
    "status": "QUEUED",
    "phase": "queued",
    "terminal": false,
    "has_token": false,
    "review_available": false,
    "review_completed_at": null,
    "attempt": 0,
    "pdf_path": "/home/me/my-paper/paper/main.pdf",
    "pdf_hash": "3f1a…c9",
    "venue": "ICLR",
    "version_no": 1,
    "round_no": 1,
    "version_source": "pdf_hash",
    "version_key": "3f1a…c9",
    "git_tag": null,
    "git_commit": null,
    "fallback_used": false,
    "last_error": null,
    "created_at": "2026-10-07T12:00:00.123456Z",
    "updated_at": "2026-10-07T12:00:00.123456Z",
    "started_at": null,
    "next_poll_at": null
  },
  "input": {
    "paper_id": "main",
    "pdf_path": "/home/me/my-paper/paper/main.pdf",
    "pdf_hash": "3f1a…c9",
    "backend": "stanford",
    "venue": "ICLR",
    "version_source": "pdf_hash",
    "version_key": "3f1a…c9"
  }
}
```

An error:

```json
{
  "code": "review_not_available",
  "message": "review not available for job 5b0b8f1e-3c55-4b3f-9a52-1d3f2b7c9e10: status is PROCESSING",
  "recovery": "wait until the job is COMPLETED; check it again with get_job after next_poll_at",
  "details": {
    "job_id": "5b0b8f1e-3c55-4b3f-9a52-1d3f2b7c9e10",
    "status": "PROCESSING"
  }
}
```

Rust usage:

```rust
use reviewloop::application::{Approval, RequestOrigin, ReviewOps, ReviewRequest};

let ops = ReviewOps::new(&config, &db);
let outcome = ops.request_review(&ReviewRequest {
    paper_id: "main".into(),
    request_key: Some("agent-session-42/main".into()),
    force: false,
    approval: Approval::Required,
    origin: RequestOrigin::Agent,
})?;
let job = ops.get_job(&outcome.job.job_id)?; // database only
```

## Not covered yet

These are deliberately outside this contract and owned by follow-up issues:

- **PDF snapshots (OSS-335).** `pdf_hash` is computed at request time, but the
  worker uploads the paper's current file. A PDF edited between request and
  submission is not detected. Likewise a job stored without a venue is sent
  with the venue configured at submission time.
- **Worker claims and uncertain submissions (OSS-337).** A QUEUED job may be
  picked by more than one process, and there is no "outcome unknown" phase
  for a submission that may have reached the provider.
- **Multi-project daemon (OSS-338).** No operation reports whether a worker is
  running. A queued job waits until the project's daemon (or a CLI command)
  processes it.
- **MCP transport (OSS-339).** Tool argument schemas, annotations, stdio
  framing and logging to stderr. The tool names above are fixed for it.
