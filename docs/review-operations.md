# Review operations contract

`reviewloop::application` implements the review operations that the CLI and
the MCP server (`reviewloop mcp`, see [MCP server](#mcp-server)) use, and
that the [shared agent skill](../integrations/reviewloop/skills/reviewloop/SKILL.md)
builds on. This
document fixes the operation names, the request and result fields, the error
codes and the lifecycle semantics those consumers MAY rely on. The Rust entry
point is `ReviewOps::new(&config, &db)`; `tests/application_ops.rs` exercises
every operation against a temporary config and database, and
`tests/mcp_stdio.rs` drives every tool through the real binary over stdio.

## Ground rules

- Operations are synchronous and touch only the loaded `Config`, the SQLite
  database and the state directory (review artifacts and PDF snapshots). They never print, never exit the
  process and never contact a review provider. State changes emit `tracing`
  events at INFO; the host's logging configuration decides where they go
  (the CLI's default is stdout; `reviewloop mcp` always logs to stderr).
- Requesting a review is a **persistent enqueue**. Sending the PDF to the
  provider and polling for the result belong to the worker (the machine
  supervisor, `reviewloop daemon run`, for every watched project) or to a
  caller that executes a job immediately, as `reviewloop submit` does.
  `get_worker_status` says which applies; a queued job is never reported as
  submitted.
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
| Registry | Every `reviewloop.toml` the CLI loads is recorded with its canonical path. Recording never watches a project and never takes a registration from another config that still declares the same `project_id` (a second clone or worktree): that is reported, not overwritten. `list_projects` reads it. Loading another project's config from that path is the adapter's job, not an operation. |
| Watched project | A registered project the machine supervisor runs: its triggers, queue, polls, timeouts and lease recovery. Only `watch_project` watches one (or, once, the migration of a single-project daemon install); `unwatch_project` stops it. An unwatched project's jobs keep their state, and explicit commands still act on them. One config file backs at most one watched project. |
| `paper_id` | Unique within a project; configured in `[[papers]]`. |
| `job_id` | A UUID, unique across projects. Jobs are scoped to their project. |
| Job reference | A job id, or a paper id. A paper reference needs a project and resolves to the single job of that paper whose status the operation accepts (newest `updated_at` first). Zero matches is `no_eligible_job`; several is `ambiguous_job`. |
| Manuscript version | A request copies the paper's PDF into an immutable, content-addressed snapshot (`snapshot_path`) and `pdf_hash` is the SHA-256 of those bytes; every submission of the job uploads that snapshot, whatever happens to the source file afterwards. Without a git commit, `version_source` is `"pdf_hash"` and `version_key` equals `pdf_hash`. `version_no` increases when a paper's version key changes; `round_no` is one past the highest round held by a pending, in-flight or completed job of that version (failed attempts give their round back). |

## Operations

| Tool | Rust | Read-only | Input | Result | Side effects |
|---|---|---|---|---|---|
| `list_projects` | `ReviewOps::list_projects` | yes | none | `ProjectView[]` | none |
| `list_papers` | `ReviewOps::list_papers` | yes | none (needs a project) | `PaperView[]` | none |
| `request_review` | `ReviewOps::request_review` | no | `ReviewRequest` (needs a project) | `ReviewRequestOutcome` | Always: a PDF snapshot under the state directory (retention prunes snapshots no job references). Created: a job row and one `job_enqueued` event (`source` `manual_submit`, `run` or `agent_request`; `enqueue_mode`; `request_key`), plus with `force` a `force_clear_cooldown` event per other active job of the paper. Existing because covered: a `duplicate_skipped` event. Existing because the key was replayed: nothing. |
| `get_job` | `ReviewOps::get_job` | yes | `job_id` | `JobView` | none |
| `list_jobs` | `ReviewOps::list_jobs` | yes | `JobListQuery` (needs a project) | `JobList` | none |
| `get_review` | `ReviewOps::get_review` | yes | `ReviewQuery` | `ReviewView` | none |
| `approve_job` | `ReviewOps::approve_job` | no | job reference (needs a project) | `TransitionOutcome` | PENDING_APPROVAL → QUEUED at the end of the queue (`next_poll_at` = now); `approved` event `{}`. |
| `retry_job` | `ReviewOps::retry_job` | no | `RetryRequest` | `RetryOutcome` | Re-queues the job (see Retry semantics); `retried` event `{}`, or `manual_rate_limit_override` with `jump_queue`; a job that went back to approval gets `retried` `{awaiting_approval: true, previous_status, previous_submit_stage}`, with `jump_queue` or not. |
| `get_worker_status` | `ReviewOps::get_worker_status` | yes | none | `WorkerStatus` | none |
| `watch_project` | `ReviewOps::watch_project` | no | `WatchProjectRequest` (needs a project) | `ProjectWatch` | Registers the project at `config_path` (moving a registration only when its config is gone, now declares another project, or `replace` is set) and watches it; `project_watched` event `{config_path}`; wakes a sleeping supervisor. |
| `unwatch_project` | `ReviewOps::unwatch_project` | no | `UnwatchProjectRequest` | `ProjectWatch` | Unwatches the project; `project_unwatched` event `{}` when it was watched; wakes a sleeping supervisor. Jobs are not touched. |
| `cancel_job` | `ReviewOps::cancel_job` | no | `CancelRequest` | `TransitionOutcome` | Non-terminal → FAILED with `last_error` `"cancelled by user"` or `"cancelled by user: <reason>"`; `cancelled` event `{reason, previous_status, previous_submit_stage, lease_was_active}`. Check, write and event share one transaction that revokes any worker lease, so a worker finishing at the same time either lands first (the cancel fails as terminal) or has its result rejected. The provider is not contacted. |

`ReviewOps::find_job(job_ref, eligibility)` is the shared resolver behind the
job references above. It is a Rust helper, not a tool.
`ReviewOps::retry_failed_jobs()` is the batch retry behind `reviewloop retry
--all-failed` (see [Batch retry](#batch-retry)). It is a CLI operation, not a
tool, and its result (`FailedJobsRetry`) is not part of the JSON contract.

### Inputs

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `ReviewRequest.paper_id` | string | required | A configured paper. Its current PDF is hashed. |
| `ReviewRequest.request_key` | string | nullable | Idempotency key, scoped to the project. The first request binds it to the job it resolves to together with the review identity derived then. A replay derives the identity again from the paper's current PDF and config: while it matches and the job exists, the replay returns that job (`reason` `request_replay`), even once the job has finished. If the PDF, backend, venue (for `cspaper` the review template) or review options changed since, the replay is `request_conflict`; if the paper or its PDF is gone, it is `paper_not_found` / `pdf_not_found`. A blank key is `invalid_request`. A new review round needs a new key. |
| `ReviewRequest.force` | boolean | required | Start a new review round even when a job covers this manuscript, and clear the cooldown of the paper's other QUEUED, SUBMITTED and PROCESSING jobs: `attempt` back to 0 and a `next_poll_at` still in the future cleared (a due one is a QUEUED job's place in the queue and stays). A replayed `request_key` still wins. |
| `ReviewRequest.approval` | `Granted` or `Required` | required | `Granted`: the job starts QUEUED. `Required`: it starts PENDING_APPROVAL and is not sent while pending: only `approve_job` moves it on (`retry_job` refuses it; `cancel_job` ends it). Adapters MUST choose explicitly; the CLI always grants. Neither value changes an `existing` job. |
| `ReviewRequest.origin` | `Submit`, `Run` or `Agent` | required | Recorded as `source` on the `job_enqueued` / `duplicate_skipped` event (`manual_submit`, `run`, `agent_request`) and as `from_command` on `force_clear_cooldown`. Agents use `Agent`. |
| `JobListQuery.paper_id` | string | nullable | Restrict to one paper. |
| `JobListQuery.active_only` | boolean | required | Keep PENDING_APPROVAL, QUEUED, SUBMITTED and PROCESSING jobs. |
| `JobListQuery.limit` | integer | nullable | Default 50, clamped to 1..=200. Newest jobs first. |
| `ReviewQuery.job_id` | string | required | The job whose review to read. |
| `ReviewQuery.part` | `Summary`, `Markdown`, `Section(name)` or `Raw` | required | `Summary`: metadata and section names only. `Markdown` (default): the rendered review. `Section`: one section's text. `Raw`: the provider's review JSON. |
| `RetryRequest.job` | job reference | required | Paper references match QUEUED, SUBMITTED and PROCESSING jobs. |
| `RetryRequest.include_failed` | boolean | required | Paper references also match FAILED, FAILED_NEEDS_MANUAL and TIMEOUT jobs. |
| `RetryRequest.jump_queue` | boolean | required | Skip the queue: a tokenless job takes its original place, its creation time (`next_poll_at` `null`), instead of the end of the queue, with any cooldown cleared; a PROCESSING job with a token is due for a poll now instead of on the polling schedule, polled in turn with the other due polls. Limited to the statuses in the Retry semantics table (except a job that goes back to approval, which waits for `approve_job`). |
| `RetryRequest.approval` | `Granted` or `Required` | required | Whether a retry that sends the manuscript again (a tokenless job) may queue it. `Granted` (the CLI): the retry is the approval; the job goes back to QUEUED. `Required` (an MCP server started with `--approval required`): the job goes back to QUEUED only when its row proves it was approved before: status QUEUED, SUBMITTED, PROCESSING, FAILED_NEEDS_MANUAL or TIMEOUT, or FAILED with a `last_error` that is not a cancellation. Any other row (a cancelled or COMPLETED job, since a cancel also ends PENDING_APPROVAL jobs and `complete` may finish one) goes back to PENDING_APPROVAL (`action` `awaiting_approval`, `last_error` `"retried; awaiting approval to submit it again"`) and moves on only through `approve_job`. The row is read inside the requeue's transaction. Token-backed retries (polls) are the same either way. |
| `RetryRequest.caller_executes` | boolean | required | With `jump_queue`, the caller submits or polls the job itself right after the call (`reviewloop retry --now`). A poll then leaves `next_poll_at` unchanged so no worker polls the job concurrently. Adapters that leave execution to the worker pass `false`. |
| `CancelRequest.job` | job reference | required | Paper references match PENDING_APPROVAL, QUEUED, SUBMITTED and PROCESSING jobs. |
| `CancelRequest.reason` | string | nullable | Recorded in `last_error` and the `cancelled` event. |
| `WatchProjectRequest.config_path` | path | required | The `reviewloop.toml` the context config was loaded from. It must declare the context's project (`invalid_request` otherwise); it is stored canonical. |
| `WatchProjectRequest.replace` | boolean | required | Move the registration here even when it points at another config that still declares this project. Without it that is `project_conflict`. |
| `UnwatchProjectRequest.project_id` | string | nullable | The project to unwatch; `null` is the context's project. The project's config need not load. |

## Job lifecycle

| `status` | `phase` | Meaning | `next_poll_at` |
|---|---|---|---|
| `PENDING_APPROVAL` | `awaiting_approval` | Stored; waits for `approve_job`. Not sent while pending. | `null` |
| `QUEUED` | `queued` | Stored locally and **not** accepted by the provider. A worker submits it in turn: queued jobs go oldest place first, a job's place being `next_poll_at`, or `created_at` when that is `null`. | Its place: when it entered the queue (a retry or `approve_job` sets it to that moment; `null` is its creation time), or after a rate limit the end of its cooldown, which holds the job back while in the future and stays its place once passed. |
| `PROCESSING` | `submitted` | The provider accepted the PDF and returned a token (`has_token`). The worker polls for the review. | Next provider poll. |
| `SUBMITTED` | `submitted` | The submission may have reached the provider. While a worker holds its lease (`submit_stage` DISPATCHED) the request is in flight; otherwise (`UNCERTAIN`) the outcome is unknown and the job is never resubmitted automatically: a token email (`stanford` only), `import-token --job-id`, an explicit `retry_job` or `cancel_job` settles it. `retry_job` refuses a job whose dispatch is in flight, and polls instead of resubmitting once a receipt token is saved. CSPaper sends no email: find the paper in the CSPaper review list (https://cspaper.org/platform/review) and pass its CSPaper `job_id` to `import-token --job-id`; otherwise `retry_job` resubmits it (may duplicate) or `cancel_job` ends it. | As stored. |
| `COMPLETED` | `completed` | The review is stored; `get_review` returns it. | `null` |
| `FAILED`, `FAILED_NEEDS_MANUAL`, `TIMEOUT` | `failed` | The attempt ended. `last_error` explains it; `retry_job` can re-queue it. | `null` |
| `FAILED` with a `cancelled by user` error | `cancelled` | Cancelled locally. A submission the provider already accepted (`has_token`) is not revoked; its result is no longer collected. | `null` |

A `created` request therefore returns a `queued` (or `awaiting_approval`) job,
never a `submitted` one. An `existing` request returns the covering job in
whatever status it has; a `Granted` request does not approve an existing
PENDING_APPROVAL job, so check `job.status` and call `approve_job` if
needed. The worker leaves a job
alone until its `next_poll_at` while that is in the future, and acts on it at
its next tick (about 30 seconds) otherwise, a queued job in turn; callers
SHOULD NOT check an active job more often than that.

`FAILED_NEEDS_MANUAL` is terminal (`terminal` true, phase `failed`) and fires
a notification. Besides a blocked submission input, a failed fallback and a
terminal provider error, it records two provider outcomes:

- **Credentials refused at submission** (no CSPaper key, or a 401 / 403;
  event `submit_failed_needs_manual`). Nothing was created and the job has no
  token, so after fixing the key `retry_job` queues it for submission again.
- **The provider reported the review failed** (CSPaper `FAILED`; event
  `poll_provider_failed`, `last_error` carries its `failed_reason`). The job
  keeps its token, so `retry_job` only polls the same failure again. A new
  review needs `request_review` with `force`: this job covers nothing, but an
  earlier completed review of the same manuscript would otherwise be returned.

A CSPaper poll refused with 401 / 403 is not terminal: the job stays
PROCESSING and is polled on the schedule until `review_timeout_hours` marks it
TIMEOUT. After fixing the key, `retry_job` polls a TIMEOUT job again: a retry
without `jump_queue` of a job with a token restarts its review timeout.

### Retry semantics

| `jump_queue` | Token | Accepted statuses | Result | `action` |
|---|---|---|---|---|
| no | yes | any but PENDING_APPROVAL | PROCESSING, `attempt` 0, `next_poll_at` = first schedule step, `last_error` cleared, review timeout restarted (`started_at` kept) | `poll_scheduled` |
| no | no | any but PENDING_APPROVAL | QUEUED at the end of the queue, `attempt` 0, `next_poll_at` = now, `last_error` cleared | `submission_queued` |
| either | no | with `approval` `Required`, a row that does not prove an earlier approval: COMPLETED or a cancelled FAILED (with `jump_queue` only the cancelled FAILED) | PENDING_APPROVAL, `attempt` 0, `next_poll_at` `null`, `last_error` `"retried; awaiting approval to submit it again"` | `awaiting_approval` |
| yes | yes | PROCESSING | PROCESSING, `next_poll_at` = now (unchanged with `caller_executes`); `attempt` and `last_error` kept | `poll_now` |
| yes | no | QUEUED, SUBMITTED, FAILED, FAILED_NEEDS_MANUAL, TIMEOUT | QUEUED at its original place, `attempt` 0, `next_poll_at` `null` (its creation time, no cooldown), `last_error` cleared | `submit_now` |

Queued jobs are submitted in order of their place, `next_poll_at` or, when
that is `null`, `created_at`: a job's place is when it entered the queue. A
new job's is its creation time, a retried or approved job's the moment of the
retry or approval, behind every job already waiting; `jump_queue` gives it
back its creation time instead. A rate limit sets `next_poll_at` to the end of
the job's cooldown: that holds the job back until then and is its place
afterwards. Clearing a cooldown (`jump_queue`, or `request_review` with
`force` for the paper's other jobs) puts the job back at its creation time.

A PENDING_APPROVAL job is `invalid_state`: it needs `approve_job`. Re-queuing a
tokenless job is checked again inside the write, and is also `invalid_state`
when a worker is sending its submission right now (`submit_stage` DISPATCHED
under a live lease: wait for the outcome or cancel it) or when a receipt token
landed meanwhile (call `retry_job` again, which then polls it). Terminal
jobs, cancelled ones included, may be retried. With `approval` `Granted` (the
CLI) retrying is an explicit re-run, so a job cancelled while pending and then
retried is queued without a separate approval. With `Required`, a job approved
before (its row is QUEUED, SUBMITTED, PROCESSING, FAILED_NEEDS_MANUAL,
TIMEOUT, or FAILED other than by a cancel) is queued again directly, while a
cancelled or COMPLETED one returns to PENDING_APPROVAL (the `awaiting_approval`
row, which takes precedence over the two tokenless rows). The schedule comes
from the `ReviewOps` config, which MUST be the job's own project; an unscoped
context reports a job that has a project as `project_mismatch`. `reviewloop
retry --now` performs the `poll_now` / `submit_now` step immediately in its own
process; any other caller leaves it to the supervisor. Like `jump_queue`,
`--now` is refused for a job with a token that is not PROCESSING (every
TIMEOUT job keeps its token): retried without it, the job is polled on
schedule.

#### Batch retry

`retry_failed_jobs` (needs a project) selects the project's FAILED,
FAILED_NEEDS_MANUAL and TIMEOUT jobs, oldest first (`created_at`, then `id`),
and retries each FAILED or TIMEOUT one that is the newest job of its paper
and backend: no job of that paper and backend, in any status, was created
after it (`created_at` ties go to the greater `id`). Each goes through
`retry_job` without `jump_queue` and with `approval` `Granted`, so tokenless
jobs join the end of the queue in their relative order (`submission_queued`)
and token-backed ones, every TIMEOUT job among them, are polled on schedule
with their review timeout restarted (`poll_scheduled`). The others are
skipped with the first reason that applies: `Superseded` (naming the newest
later job and its status), `Cancelled`, or `NeedsManual` (FAILED_NEEDS_MANUAL:
`last_error` says how to recover, a fix and a retry by job id or a new
review; never retried in a batch). Each retry re-reads the job: one whose status changed
since it was selected, or that `retry_job` refuses as `invalid_state` (a
submission in flight, a receipt token that landed), is skipped as `Changed`
with the message; any other error ends the batch, keeping the retries already
made.

## Results

### JobView

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `job_id` | string | required | Job UUID. |
| `project_id` | string | required | Owning project. Empty only for legacy jobs. |
| `paper_id` | string | required | Paper the job reviews. |
| `backend` | string | required | Review provider: `"stanford"` or `"cspaper"`. |
| `status` | string | required | One of `"PENDING_APPROVAL"`, `"QUEUED"`, `"SUBMITTED"`, `"PROCESSING"`, `"COMPLETED"`, `"FAILED"`, `"FAILED_NEEDS_MANUAL"`, `"TIMEOUT"`. |
| `phase` | string | required | One of `"awaiting_approval"`, `"queued"`, `"submitted"`, `"completed"`, `"failed"`, `"cancelled"` (see the lifecycle table). |
| `terminal` | boolean | required | `true` for COMPLETED, FAILED, FAILED_NEEDS_MANUAL and TIMEOUT. |
| `has_token` | boolean | required | The provider acknowledged a submission. The token (for `cspaper`, the CSPaper `job_id`) is never returned. |
| `review_available` | boolean | required | A review is stored; `get_review` succeeds. |
| `review_completed_at` | RFC3339 UTC timestamp string | nullable | When the review was stored. |
| `attempt` | integer | required | Attempts in the current submit or poll cycle. |
| `pdf_path` | string | required | The paper's PDF the job was enqueued from. It may have changed or disappeared since. |
| `snapshot_path` | string | nullable | The immutable copy every submission of the job uploads. `null` only for jobs created before snapshots existed (the worker backfills it while the source still matches `pdf_hash`) and for jobs created by `import-token`. |
| `pdf_hash` | string | required | SHA-256 of the snapshot bytes. |
| `venue` | string | nullable | Venue recorded at request time; for `cspaper` the review template (`agent_id`, e.g. `"ICLR_main_2026_1"`). When it is null, a stanford worker sends the paper's configured venue at submission time, which this field does not show. |
| `review_options` | object | required | Provider options recorded at request time, string values keyed by option name, and part of the review identity: `{"desk_rejection_enabled": "true"}` or `"false"` for `cspaper`, `{}` for `stanford`. |
| `version_no` | integer | required | Manuscript version number within the paper. |
| `round_no` | integer | required | Review round within the version. |
| `version_source` | string | required | `"pdf_hash"` or `"git_commit"`. |
| `version_key` | string | required | The hash or commit identifying the version. |
| `git_tag` | string | nullable | Tag that triggered the job, if any. |
| `git_commit` | string | nullable | Commit of that tag, if any. |
| `fallback_used` | boolean | required | The browser fallback submitted the job. `stanford` only; always `false` for `cspaper`, which has no fallback. |
| `submit_stage` | string | nullable | Current submit attempt: `CLAIMED` (a worker owns it, nothing sent), `DISPATCHED` (the request may be in flight) or `UNCERTAIN` (the provider may hold it but no receipt was saved; never resubmitted automatically, see `last_error`). |
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
| `ReviewRequestOutcome.input` | `ManuscriptInput` | required | What this request asked to review: `paper_id`, `pdf_path` (the source), `snapshot_path` (the copy taken for this request), `pdf_hash` (of the snapshot), `backend`, `venue` (trimmed, `null` when unset), `review_options`, `version_source`, `version_key`, `notices` (what the provider will not review, such as pages past the first 15 for `stanford`; `[]` when no page is known to fall outside what it reviews; page counts are estimates). On `existing` the returned job keeps its own snapshot; compare `pdf_hash` to confirm it reviews the same bytes. |
| `JobList.jobs` | `JobView[]` | required | Newest first. |
| `JobList.truncated` | boolean | required | More jobs matched than `limit`. |
| `TransitionOutcome.job` | `JobView` | required | The job after the change. |
| `TransitionOutcome.previous_status` | string | required | Status before the change. |
| `RetryOutcome.job`, `.previous_status` | as above | required | As for `TransitionOutcome`. |
| `RetryOutcome.action` | string | required | One of `"poll_scheduled"`, `"submission_queued"`, `"poll_now"`, `"submit_now"`, `"awaiting_approval"` (see Retry semantics). |
| `ProjectView` | object | — | `project_id`, `config_path`, `config_present` (the file still exists), `last_seen_at`, `current` (the project these operations act for), `watched` (the supervisor runs it), `state` (`"unwatched"`, `"pending"` (watched, not run yet), `"ok"` or `"error"`), `last_run_at` and `last_ok_at` (nullable: the supervisor's last pass, and its last clean one), `last_error` (nullable: the failure of its last pass). Ordered by `project_id`. |
| `WorkerStatus.supervisor` | `SupervisorView` | required | `state` (`"running"`, `"paused"` or `"stopped"`; stopped also when its heartbeat is over 60 s old), `pid`, `version`, `started_at`, `heartbeat_at`, `last_tick_at`, `paused_at` (nullable: the persistent pause), `last_tick_error` (nullable: the machine-level failure of its latest tick). |
| `WorkerStatus.project` | object | nullable | For a scoped context: `project_id`, `availability` and `last_error`. `availability` is `"ready"` (a running supervisor runs the project), `"project_not_registered"`, `"project_unwatched"`, `"supervisor_stopped"`, `"supervisor_paused"` or `"project_failing"` (its last pass failed). Only `"ready"` means queued jobs move without a caller. `null` for an unscoped context. |
| `ProjectWatch` | object | — | `project` (`ProjectView`), `changed` (whether the call changed what the supervisor runs, or which config backs the project), `moved_from` (nullable: the config a watch moved the registration from), `worker` (`WorkerStatus` for that project). |
| `PaperView` | object | — | `paper_id`, `backend`, `venue` and `review_options` (what a new request would send), `pdf_path`, `pdf_present`, `watched` (PDF-change watching, turned on by `reviewloop paper watch` and off by `reviewloop paper unwatch`: with the PDF trigger on (`trigger.pdf.enabled`), each pass requests a review when the PDF differs from the PDF of the paper's latest review request, or the paper has none; not whether the supervisor runs the project, which is `ProjectView.watched`), `tag_trigger` (nullable). Config order. |
| `ReviewView.job` | `JobView` | required | The reviewed job, for checking `pdf_hash` and `version_no`. |
| `ReviewView.completed_at` | RFC3339 UTC timestamp string | required | When the review was stored. |
| `ReviewView.score` | string | nullable | The review's `numerical_score` as text. For `cspaper`: `result_summary.overall_score`, else `mainScoreNorm`, when numeric. |
| `ReviewView.title` | string | nullable | The review's `title`. For `cspaper`: `paper_meta.title`. |
| `ReviewView.sections` | string[] | required | Available text sections: `summary`, `strengths`, `weaknesses`, `detailed_comments`, `questions`, `assessment`, `full_review` in that order when present, then others by name; or `["content"]`. A `cspaper` review is always `["content"]`: the markdown review CSPaper returned. |
| `ReviewView.markdown` | string | nullable | Rendered review, only for part `Markdown`. |
| `ReviewView.section` | object | nullable | `{name, text}`, only for part `Section`. |
| `ReviewView.raw` | JSON value | nullable | Provider review JSON, only for part `Raw`. Values of `token` keys and token text are `[redacted]`. For `cspaper`, the normalized review (`provider`, `provider_job_id`, `title`, `venue`, `agent_id`, `finished_at`, `numerical_score`, `desk_reject`, `result_summary`, `content`) with CSPaper's response verbatim under `provider_raw`; the CSPaper job id is the token, so `provider_job_id` and `provider_raw.data.id` read `[redacted]`. |
| `ReviewView.artifacts` | object | required | `dir`, `review_md`: paths that exist, else `null`. `review.json` (the stored review, unredacted: a CSPaper one holds the provider job id, which is the token) and `meta.json` (stores the token) are never listed; `raw` is the redacted review. |

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
| `invalid_request` | `InvalidRequest` | A request field is malformed (a blank `request_key`). Over MCP also: arguments (a JSON object) whose names, types or enum values do not match the tool's `inputSchema` (`field` `arguments`), a whole-number `limit` of 0 or above 200 (`field` `limit`; a negative, fractional or non-numeric one is `field` `arguments`), both or neither of `job_id` and `paper_id` (`field` `job_id`), a `section` that does not fit `part` (`field` `section`), a blank `project_id`. Details: `field`. | Fix the field and resend. |
| `request_conflict` | `RequestConflict` | The `request_key` is bound to a job whose recorded review identity differs from the one derived from the paper's current PDF and config (usually: the PDF changed; for `cspaper` also the template or the desk-rejection setting). Nothing was stored. Details: `project_id`, `request_key`, `existing_job_id`, `mismatches` (`{field, recorded, requested}`; `field` is one of `paper_id`, `backend`, `pdf_hash`, `venue`, `review_options`, `version_source`, `version_key`, in that order; `review_options` values are sorted-key JSON text such as `{"desk_rejection_enabled":"true"}`, `null` when empty). | Read the bound job with `get_job(existing_job_id)`; to review the current manuscript, send a new key. |
| `project_mismatch` | `ProjectMismatch` | `retry_job` from an unscoped context resolved a job that has a project, or the CLI loaded a registered config that now declares another `project_id`. Details: `job_id`, `job_project_id`, `context_project_id`. | Call it with that project's config. |
| `paper_not_found` | `PaperNotFound` | `paper_id` is not configured. Details: `paper_id`, `known` (the configured ids). | Use a `list_papers` id or `reviewloop paper add`. |
| `pdf_not_found` | `PdfNotFound` | The paper's PDF does not exist. Details: `paper_id`, `path`. | Restore the file or fix `pdf_path`. |
| `submitter_email_unavailable` | `SubmitterEmailUnavailable` | The backend needs a submitter email (`stanford` only; `cspaper` needs none) and none is configured. Details: `backend`. | Set `providers.stanford.email` or run `reviewloop email login`. |
| `provider_not_configured` | `ProviderNotConfigured` | The paper's backend needs a setting that is not configured: `setting` is `api_key` (no CSPaper key in the global config or `REVIEWLOOP_CSPAPER_API_KEY`) or `agent_id` (no review template for the paper). Checked at request time, the key first, before any `request_key` replay; nothing is enqueued. Details: `backend`, `setting`. | Set `providers.cspaper.api_key` in `~/.config/reviewloop/config.toml` (never in `reviewloop.toml`) or export `REVIEWLOOP_CSPAPER_API_KEY`; set `providers.cspaper.agent_id` or the paper's venue (`paper add --agent-id`). |
| `input_rejected` | `InputRejected` | The provider would refuse the paper's PDF: over its size limit (10 MiB for `stanford`) or not a PDF. Nothing was stored. Details: `paper_id`, `backend`. | Fix the PDF as the message says and request again. See `docs/providers/stanford.md`. |
| `job_not_found` | `JobNotFound` | No job with that id in scope. Details: `job_id`. | Check the id; `list_jobs`. |
| `no_eligible_job` | `NoEligibleJob` | A paper reference matched no job in an accepted status. Details: `paper_id`, `action`, `statuses`. | `list_jobs`, then pass a job id. |
| `ambiguous_job` | `AmbiguousJob` | A paper reference matched several jobs. Details: `paper_id`, `action`, `candidates` (`{job_id, status}`). | Pass one candidate's job id. |
| `invalid_state` | `InvalidState` | The job's status does not allow the operation (approve a non-pending job, cancel a finished one, retry a PENDING_APPROVAL job, `jump_queue` from an unsupported status). Details: `job_id`, `status`, `operation` (tool name). | `get_job`; see the operation's accepted statuses; a PENDING_APPROVAL job needs `approve_job`. |
| `review_not_available` | `ReviewNotAvailable` | No review is stored for the job. Details: `job_id`, `status`. | Active job: wait for COMPLETED and re-check after `next_poll_at`. Terminal job: it ended without a review; `retry_job` or `request_review`. |
| `section_not_found` | `SectionNotFound` | The requested section does not exist. Details: `job_id`, `section`, `available`. | Request an available section or the markdown. |
| `project_conflict` | `ProjectConflict` | `watch_project` would take the registration from another config that still declares the project (without `replace`), or the config already backs another watched project. Details: `project_id`, `registered_path`, `requested_path`, `watched_as` (the other watched project, else `null`). | Keep one copy per `project_id` or pass `replace`; for `watched_as`, unwatch that project first. |
| `project_not_registered` | `ProjectNotRegistered` | `unwatch_project` named a project the registry does not know, or an MCP call's `project_id` names one (or, on a server started without a project, the job `retry_job` resolved belongs to one). Details: `project_id`. | Check the id with `list_projects`. |
| `config_unavailable` | none (MCP server only) | The configuration an MCP call needs does not load: the server's own project, the registered `reviewloop.toml` of the call's `project_id`, or that of the project of the job `retry_job` resolved on a server started without a project (missing, unparsable, refused by the foreign-config checks, or now declaring another `project_id`). Details: `project_id` and `config_path` (nullable). | Fix the named file; `reviewloop status` in that repository reports the same error. |
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
  review identity (paper, backend, PDF hash, venue, review options, version
  key) matches a PENDING_APPROVAL, QUEUED, SUBMITTED, PROCESSING or COMPLETED
  job returns that job with reason `covered`. For `cspaper` the venue is the
  review template and the review options hold `desk_rejection_enabled`, so
  switching either asks for a new review. FAILED, FAILED_NEEDS_MANUAL and
  TIMEOUT jobs cover nothing, so a request after a failure creates a new job.
  The PDF path and the submitter email are not part of the identity. Jobs
  stored before review options existed read back with `{}`, which matches a
  `stanford` request.
- **New round.** `force` skips the coverage check and always creates a job in
  a new review round.

## CLI mapping

| Command | Operation | What stays in the CLI |
|---|---|---|
| `submit` | `request_review` (origin `Submit`, approval granted, `--request-key`, `--force`) | On `existing`, print `Skipped submit: <reason> existing job …` and exit 0. On `created`, submit the job to the provider immediately and pull its first poll forward to about 60 seconds. |
| `paper add --submit-now` | as `submit` | |
| `run` | `request_review` (origin `Run`, `force`), `get_worker_status` | Paper registration, immediate submission, and the foreground loop with its exit codes. While a running supervisor runs the project, the loop only observes the job; otherwise it submits or polls this job alone, when the job's schedule makes it due (and reads the mailbox for a token while the submission's outcome is unknown). It never runs other jobs, triggers or retention. |
| `approve` | `approve_job` | Printing `Approved job …`. |
| `retry` | `find_job`, then `retry_job` with the job's project config (approval granted; `jump_queue` and `caller_executes` = `--now`) | Loading a foreign project's config from the registry; with `--now`, the immediate poll or submit in the `retry` process. A PENDING_APPROVAL job is refused. |
| `retry --all-failed` | `retry_failed_jobs` with the current project's config | One line per retried job (what happens next), one per skipped job with its reason, a summary line. `--now`, `--include-failed`, `--job-id` and `--paper-id` are usage errors with it. |
| `cancel` | `cancel_job` | The `hint:` line for a paper with no active job. |
| `complete --paper-id` | `find_job` | Everything else. |
| `project list` | `list_projects`, `get_worker_status` | Showing whether each registered config still declares its project. |
| `project watch` | `watch_project` (`--replace`) | Loading the config (the current repository, `--config`, or the registered config of `--project-id`). |
| `project unwatch` | `unwatch_project` (`--project-id`) | |
| `daemon status` | `list_projects`, `get_worker_status` | The launchd service state, active jobs, mailbox and proxy health. |
| `status`, `check`, `import-token`, `complete` | not routed | `status`, `submit`, `approve` and `retry` print the project's worker availability after the queue state. `check` is the network refresh. |

Exit codes are unchanged. Every command exits 0 on success, 1 on an error
(message on stderr) and 2 on a usage error. In addition, `run` exits 2 when the
job ends FAILED, FAILED_NEEDS_MANUAL or TIMEOUT and 130 on Ctrl-C, and
`import-token` exits 2 when its immediate poll fails.

## MCP server

`reviewloop mcp` serves the twelve operations above as Model Context Protocol
tools over stdio, with the same names. An MCP client starts it as a
subprocess:

```bash
# Claude Code
claude mcp add reviewloop -- reviewloop mcp
# Codex
codex mcp add reviewloop -- reviewloop mcp
```

```json
{
  "mcpServers": {
    "reviewloop": { "command": "reviewloop", "args": ["mcp"] }
  }
}
```

### Transport and lifecycle

- Newline-delimited JSON-RPC 2.0 on stdin/stdout, through the official Rust
  SDK (`rmcp`). Protocol versions 2024-11-05, 2025-03-26, 2025-06-18 and
  2025-11-25 are negotiated at `initialize`; a client asking for another
  version gets 2025-11-25. The server offers tools only.
- stdout carries MCP messages and nothing else. The log goes to stderr at
  `logging.level`, whatever `logging.output` says.
- The server exits 0 when the client closes stdin, before the handshake
  too. It exits 1 before answering anything when the configuration does not
  load at startup (the same check every command makes), or when the first
  message is a notification or a response; the message is on stderr. A
  first request other than `initialize` gets a JSON-RPC error and the server
  keeps waiting, except that the SDK serves a request carrying the
  2026-07-28 per-request lifecycle (`_meta.protocolVersion` and
  `clientCapabilities`) without a handshake.

### Projects and configuration

- **Default project.** Calls act on the project of the `reviewloop.toml`
  around the server's working directory, or of `--config` (`reviewloop
  --config <path> mcp`). The server keeps to the file it started with:
  calls that name no project act on that project only, and fail with
  `config_unavailable` once the file is gone, no longer loads or declares
  another `project_id`, never falling back to another `reviewloop.toml`
  (another clone's included) or to every project. Starting the server
  registers that project, as any command does.
  Many clients start servers in an arbitrary directory; there, pass
  `--config`, or name projects per call.
- **`project_id`.** Every project-scoped tool takes an optional
  `project_id` from `list_projects`. Naming the default project's id acts on
  the default project's config, even when the registry points at another
  clone of it, as long as that config loads and still declares the id;
  otherwise the id is looked up in the registry like any other, so it can
  reach the registered clone. Another id loads that project's registered
  `reviewloop.toml`, checked as `reviewloop retry` checks a config outside the
  current directory. An id the registry does not know is
  `project_not_registered`. Without a default project (no `reviewloop.toml`),
  operations that need one fail with `project_required` unless the call names
  one; job-id lookups search every project, as for an unscoped context.
- **Reloaded per call.** Every call loads the configuration again, as each
  CLI command does, so edits apply without restarting the server. A config
  that no longer loads fails that call with `config_unavailable`; other
  projects keep working. `list_projects` and `unwatch_project` with a
  `project_id` need only the global config.
- `retry_job` re-queues a job on its own project's config, as `reviewloop
  retry` does, and never executes it (`caller_executes` is false): the
  supervisor does.

### Approval

The operator chooses `ReviewRequest.approval` when starting the server; the
agent cannot:

| Flag | `request_review` creates | Next step |
|---|---|---|
| `--approval required` (default) | `PENDING_APPROVAL` | `approve_job`, which tool descriptions tell the agent to call only after the user agreed to send the manuscript to the provider; agreement the user already gave counts, so the agent does not ask again. MCP clients that confirm tool calls ask the user for it separately. A `retry_job` that sends the manuscript again queues the job directly only when its row proves an earlier approval (it is QUEUED, or in a status only reached after QUEUED: sent, failed other than by a cancel, timed out, refused); a cancelled or hand-completed job goes back to PENDING_APPROVAL (`awaiting_approval`). So nothing the agent requests reaches the provider without `approve_job`, and nothing it retries is sent unless a job was approved before or is approved again. A retry of a SUBMITTED job with an unknown outcome (`submit_stage` UNCERTAIN) is queued at once and may duplicate a review the provider already holds, so the `retry_job` description tells the agent to ask the user before retrying a job `get_job` shows SUBMITTED without a receipt (`has_token` false). |
| `--approval granted` | `QUEUED` | None: the supervisor sends it at its next tick. |

Requests are recorded with origin `Agent` (event source `agent_request`).

Approval covers what the agent requests and retries. `watch_project` is not
gated by it: watching lets the supervisor run the project as configured,
which sends jobs already QUEUED (by the CLI, say), every `review-*` git tag
not yet seen (the git trigger is on by default) and, with
`auto_submit_on_change`, changed PDFs, none of them waiting for
`approve_job`. Its description tells the agent to ask the user first, and it
is marked `openWorld`.

### Tools

Arguments are the operation's inputs (see [Inputs](#inputs)); `project_id` is
optional everywhere except `list_projects`. A job reference is exactly one of
`job_id` and `paper_id`. Optional arguments are omitted (an explicit `null`
reads as omitted, booleans included); unknown arguments are
`invalid_request`.

| Tool | Arguments | Result | readOnly | destructive | idempotent | openWorld |
|---|---|---|---|---|---|---|
| `list_projects` | none | `{projects: ProjectView[]}` | yes | no | yes | no |
| `list_papers` | `project_id` | `{project_id, papers: PaperView[]}` | yes | no | yes | no |
| `request_review` | `paper_id` (required), `request_key`, `force`, `project_id` | `ReviewRequestOutcome` | no | no | no | only with `--approval granted` |
| `get_job` | `job_id` (required), `project_id` | `JobView` | yes | no | yes | no |
| `list_jobs` | `paper_id`, `active_only`, `limit` (1..=200), `project_id` | `JobList` | yes | no | yes | no |
| `get_review` | `job_id` (required), `part` (`summary`, `markdown` (default), `section`, `raw`), `section`, `project_id` | `ReviewView` | yes | no | yes | no |
| `approve_job` | job reference, `project_id` | `TransitionOutcome` | no | no | no | yes |
| `retry_job` | job reference, `include_failed`, `jump_queue`, `project_id` | `RetryOutcome` | no | no | no | yes |
| `get_worker_status` | `project_id` | `WorkerStatus` | yes | no | yes | no |
| `watch_project` | `project_id`, `replace` | `ProjectWatch` | no | no | yes | yes |
| `unwatch_project` | `project_id` | `ProjectWatch` | no | no | yes | no |
| `cancel_job` | job reference, `reason`, `project_id` | `TransitionOutcome` | no | yes | no | no |

`get_review` with a `section` and no `part` returns that section. `openWorld`
marks the calls that lead the supervisor to contact the provider; no tool
contacts it itself. `watch_project` registers the config it loaded (the
default project's, or the registered one of `project_id`) as
`WatchProjectRequest.config_path`.

### Results and errors

- **Success:** `structuredContent` is the result document of
  [Results](#results), and a text block carries the same JSON. Each tool
  declares an `outputSchema` generated from the Rust result type: every
  field is required (absent optional values are `null`), enums list their
  values, and the schema is self-contained (no `$ref` or `$schema`).
- **Failure:** a tool result with `isError: true` and no
  `structuredContent`; its text block is the `OpError::view()` JSON
  `{code, message, recovery, details}` of [Errors](#errors). The model sees
  it and can act on `recovery`.
- **Protocol errors** are kept for requests the server cannot route: an
  unknown tool name is JSON-RPC error `-32602`, and a `tools/call` whose
  `params` are not `{name: string, arguments?: object}` (`arguments` given as
  a string or an array) is rejected by the SDK as `-32601`.

### Smoke test without an agent

Any process that speaks newline-delimited JSON-RPC is a client. From a
repository with a `reviewloop.toml`, this prints one response per line (the
handshake, then the project's papers) and exits when stdin closes:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_papers","arguments":{}}}' \
  | reviewloop mcp 2>/dev/null
```

`cargo test --test mcp_stdio` drives the built binary the same way through
the SDK's own client, in a throwaway home, config and state directory: the
handshake, every tool's schemas and annotations, errors, approval, project
scoping, reconnects, and a review carried to its result by the supervisor
against a mock provider.

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
    "snapshot_path": "/home/me/.review_loop/snapshots/3f1a…c9/main.pdf",
    "pdf_hash": "3f1a…c9",
    "venue": "ICLR",
    "review_options": {},
    "version_no": 1,
    "round_no": 1,
    "version_source": "pdf_hash",
    "version_key": "3f1a…c9",
    "git_tag": null,
    "git_commit": null,
    "fallback_used": false,
    "submit_stage": null,
    "last_error": null,
    "created_at": "2026-10-07T12:00:00.123456Z",
    "updated_at": "2026-10-07T12:00:00.123456Z",
    "started_at": null,
    "next_poll_at": null
  },
  "input": {
    "paper_id": "main",
    "pdf_path": "/home/me/my-paper/paper/main.pdf",
    "snapshot_path": "/home/me/.review_loop/snapshots/3f1a…c9/main.pdf",
    "pdf_hash": "3f1a…c9",
    "backend": "stanford",
    "venue": "ICLR",
    "review_options": {},
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

- **Venue at submission.** A `stanford` job stored without a venue is sent
  with the venue configured at submission time, which `JobView.venue` does
  not show. A `cspaper` job is sent with exactly its recorded template.
- **CSPaper caveats.**
  - A PROCESSING `cspaper` job times out after the flat
    `review_timeout_hours`; unlike `stanford`, it is not scaled by page count.
  - Provider capacity is shared: `max_concurrency` and
    `max_submissions_per_tick` are one machine-wide budget per supervisor
    tick across both backends and every project, and the organisation API
    key's quota is shared by every project that uses it.
  - UNCERTAIN `cspaper` submissions are not reconciled automatically; an
    operator matches them against the CSPaper review list (see the lifecycle
    table).
  - The template (`venue` / `agent_id`) is not validated before submission:
    `request_review` only checks that one is set, and an unknown template
    fails the job (FAILED) when CSPaper answers 400.
