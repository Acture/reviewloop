# Review operations contract

`reviewloop::application` implements the review operations that the CLI uses
today and that the MCP adapter and the shared agent skill build on. This
document fixes the operation names, the request and result fields, the error
codes and the lifecycle semantics those consumers MAY rely on. The Rust entry
point is `ReviewOps::new(&config, &db)`; `tests/application_ops.rs` exercises
every operation against a temporary config and database.

## Ground rules

- Operations are synchronous and touch only the loaded `Config`, the SQLite
  database and the state directory (review artifacts and PDF snapshots). They never print, never exit the
  process and never contact a review provider. State changes emit `tracing`
  events at INFO; the host's logging configuration decides where they go
  (the CLI's default is stdout, so an MCP host MUST log to stderr).
- Requesting a review is a **persistent enqueue**. Sending the PDF to the
  provider and polling for the result belong to the worker (the machine
  supervisor, `reviewloop daemon run`, for every enabled project) or to a
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
| Registry | Every `reviewloop.toml` the CLI loads is recorded with its canonical path. Recording never enables a project and never takes a registration from another config that still declares the same `project_id` (a second clone or worktree): that is reported, not overwritten. `list_projects` reads it. Loading another project's config from that path is the adapter's job, not an operation. |
| Enabled project | A registered project the machine supervisor runs: its triggers, queue, polls, timeouts and lease recovery. Only `enable_project` enables one (or, once, the migration of a single-project daemon install); `disable_project` stops it. A disabled project's jobs keep their state, and explicit commands still act on them. One config file backs at most one enabled project. |
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
| `approve_job` | `ReviewOps::approve_job` | no | job reference (needs a project) | `TransitionOutcome` | PENDING_APPROVAL → QUEUED; `approved` event `{}`. |
| `retry_job` | `ReviewOps::retry_job` | no | `RetryRequest` | `RetryOutcome` | Re-queues the job (see Retry semantics); `retried` event `{}`, or `manual_rate_limit_override` when forced. |
| `get_worker_status` | `ReviewOps::get_worker_status` | yes | none | `WorkerStatus` | none |
| `enable_project` | `ReviewOps::enable_project` | no | `EnableProjectRequest` (needs a project) | `ProjectEnablement` | Registers the project at `config_path` (moving a registration only when its config is gone, now declares another project, or `replace` is set) and enables it; `project_enabled` event `{config_path}`; wakes a sleeping supervisor. |
| `disable_project` | `ReviewOps::disable_project` | no | `DisableProjectRequest` | `ProjectEnablement` | Disables the project; `project_disabled` event `{}` when it was enabled; wakes a sleeping supervisor. Jobs are not touched. |
| `cancel_job` | `ReviewOps::cancel_job` | no | `CancelRequest` | `TransitionOutcome` | Non-terminal → FAILED with `last_error` `"cancelled by user"` or `"cancelled by user: <reason>"`; `cancelled` event `{reason, previous_status, previous_submit_stage, lease_was_active}`. Check, write and event share one transaction that revokes any worker lease, so a worker finishing at the same time either lands first (the cancel fails as terminal) or has its result rejected. The provider is not contacted. |

`ReviewOps::find_job(job_ref, eligibility)` is the shared resolver behind the
job references above. It is a Rust helper, not a tool.

### Inputs

| Field | Type | Nullability | Semantics |
|---|---|---|---|
| `ReviewRequest.paper_id` | string | required | A configured paper. Its current PDF is hashed. |
| `ReviewRequest.request_key` | string | nullable | Idempotency key, scoped to the project. The first request binds it to the job it resolves to together with the review identity derived then. A replay derives the identity again from the paper's current PDF and config: while it matches and the job exists, the replay returns that job (`reason` `request_replay`), even once the job has finished. If the PDF, backend, venue (for `cspaper` the review template) or review options changed since, the replay is `request_conflict`; if the paper or its PDF is gone, it is `paper_not_found` / `pdf_not_found`. A blank key is `invalid_request`. A new review round needs a new key. |
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
| `EnableProjectRequest.config_path` | path | required | The `reviewloop.toml` the context config was loaded from. It must declare the context's project (`invalid_request` otherwise); it is stored canonical. |
| `EnableProjectRequest.replace` | boolean | required | Move the registration here even when it points at another config that still declares this project. Without it that is `project_conflict`. |
| `DisableProjectRequest.project_id` | string | nullable | The project to disable; `null` is the context's project. The project's config need not load. |

## Job lifecycle

| `status` | `phase` | Meaning | `next_poll_at` |
|---|---|---|---|
| `PENDING_APPROVAL` | `awaiting_approval` | Stored; waits for `approve_job`. Not sent while pending. | `null` |
| `QUEUED` | `queued` | Stored locally and **not** accepted by the provider. A worker submits it. | Earliest submission attempt (set after a rate limit); `null` means the next worker tick. |
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
alone until its `next_poll_at`, or until its next tick (about 30 seconds) when
that is `null`; callers SHOULD NOT check an active job more often than that.

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
TIMEOUT.

### Retry semantics

| `force` | Token | Accepted statuses | Result | `action` |
|---|---|---|---|---|
| no | yes | any but PENDING_APPROVAL | PROCESSING, `attempt` 0, `next_poll_at` = first schedule step, `last_error` cleared | `poll_scheduled` |
| no | no | any but PENDING_APPROVAL | QUEUED, `attempt` 0, `next_poll_at` `null`, `last_error` cleared | `submission_queued` |
| yes | yes | PROCESSING | PROCESSING, `next_poll_at` = now (unchanged with `caller_executes`); `attempt` and `last_error` kept | `poll_now` |
| yes | no | QUEUED, SUBMITTED, FAILED, FAILED_NEEDS_MANUAL, TIMEOUT | QUEUED, `attempt` 0, `next_poll_at` `null`, `last_error` cleared | `submit_now` |

A PENDING_APPROVAL job is `invalid_state`: it needs `approve_job`. Re-queuing a
tokenless job is checked again inside the write, and is also `invalid_state`
when a worker is sending its submission right now (`submit_stage` DISPATCHED
under a live lease: wait for the outcome or cancel it) or when a receipt token
landed meanwhile (call `retry_job` again, which then polls it). Terminal
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
| `RetryOutcome.action` | string | required | One of `"poll_scheduled"`, `"submission_queued"`, `"poll_now"`, `"submit_now"` (see Retry semantics). |
| `ProjectView` | object | — | `project_id`, `config_path`, `config_present` (the file still exists), `last_seen_at`, `current` (the project these operations act for), `enabled` (the supervisor runs it), `state` (`"disabled"`, `"pending"` (enabled, not run yet), `"ok"` or `"error"`), `last_run_at` and `last_ok_at` (nullable: the supervisor's last pass, and its last clean one), `last_error` (nullable: the failure of its last pass). Ordered by `project_id`. |
| `WorkerStatus.supervisor` | `SupervisorView` | required | `state` (`"running"`, `"paused"` or `"stopped"`; stopped also when its heartbeat is over 60 s old), `pid`, `version`, `started_at`, `heartbeat_at`, `last_tick_at`, `paused_at` (nullable: the persistent pause), `last_tick_error` (nullable: the machine-level failure of its latest tick). |
| `WorkerStatus.project` | object | nullable | For a scoped context: `project_id`, `availability` and `last_error`. `availability` is `"ready"` (a running supervisor runs the project), `"project_not_registered"`, `"project_disabled"`, `"supervisor_stopped"`, `"supervisor_paused"` or `"project_failing"` (its last pass failed). Only `"ready"` means queued jobs move without a caller. `null` for an unscoped context. |
| `ProjectEnablement` | object | — | `project` (`ProjectView`), `changed` (whether the call changed what the supervisor runs, or which config backs the project), `moved_from` (nullable: the config an enable moved the registration from), `worker` (`WorkerStatus` for that project). |
| `PaperView` | object | — | `paper_id`, `backend`, `venue` and `review_options` (what a new request would send), `pdf_path`, `pdf_present`, `watched`, `tag_trigger` (nullable). Config order. |
| `ReviewView.job` | `JobView` | required | The reviewed job, for checking `pdf_hash` and `version_no`. |
| `ReviewView.completed_at` | RFC3339 UTC timestamp string | required | When the review was stored. |
| `ReviewView.score` | string | nullable | The review's `numerical_score` as text. For `cspaper`: `result_summary.overall_score`, else `mainScoreNorm`, when numeric. |
| `ReviewView.title` | string | nullable | The review's `title`. For `cspaper`: `paper_meta.title`. |
| `ReviewView.sections` | string[] | required | Available text sections: `summary`, `strengths`, `weaknesses`, `detailed_comments`, `questions`, `assessment`, `full_review` in that order when present, then others by name; or `["content"]`. A `cspaper` review is always `["content"]`: the markdown review CSPaper returned. |
| `ReviewView.markdown` | string | nullable | Rendered review, only for part `Markdown`. |
| `ReviewView.section` | object | nullable | `{name, text}`, only for part `Section`. |
| `ReviewView.raw` | JSON value | nullable | Provider review JSON, only for part `Raw`. Values of `token` keys and token text are `[redacted]`. For `cspaper`, the normalized review (`provider`, `provider_job_id`, `title`, `venue`, `agent_id`, `finished_at`, `numerical_score`, `desk_reject`, `result_summary`, `content`) with CSPaper's response verbatim under `provider_raw`; the CSPaper job id is the token, so `provider_job_id` and `provider_raw.data.id` read `[redacted]`. |
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
| `invalid_state` | `InvalidState` | The job's status does not allow the operation (approve a non-pending job, cancel a finished one, retry a PENDING_APPROVAL job, force-retry an unsupported status). Details: `job_id`, `status`, `operation` (tool name). | `get_job`; see the operation's accepted statuses; a PENDING_APPROVAL job needs `approve_job`. |
| `review_not_available` | `ReviewNotAvailable` | No review is stored for the job. Details: `job_id`, `status`. | Active job: wait for COMPLETED and re-check after `next_poll_at`. Terminal job: it ended without a review; `retry_job` or `request_review`. |
| `section_not_found` | `SectionNotFound` | The requested section does not exist. Details: `job_id`, `section`, `available`. | Request an available section or the markdown. |
| `project_conflict` | `ProjectConflict` | `enable_project` would take the registration from another config that still declares the project (without `replace`), or the config already backs another enabled project. Details: `project_id`, `registered_path`, `requested_path`, `enabled_as` (the other enabled project, else `null`). | Keep one copy per `project_id` or pass `replace`; for `enabled_as`, disable that project first. |
| `project_not_registered` | `ProjectNotRegistered` | `disable_project` named a project the registry does not know. Details: `project_id`. | Check the id with `list_projects`. |
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
| `run` | `request_review` (origin `Run`, `force`) | Paper registration, immediate submission, the foreground polling loop and its exit codes. |
| `approve` | `approve_job` | Printing `Approved job …`. |
| `retry` | `find_job`, then `retry_job` with the job's project config (`caller_executes` = `--force`) | Loading a foreign project's config from the registry; with `--force`, the immediate poll or submit. A PENDING_APPROVAL job is refused. |
| `cancel` | `cancel_job` | The `hint:` line for a paper with no active job. |
| `complete --paper-id` | `find_job` | Everything else. |
| `project list` | `list_projects`, `get_worker_status` | Showing whether each registered config still declares its project. |
| `project enable` | `enable_project` (`--replace`) | Loading the config (the current repository, `--config`, or the registered config of `--project-id`). |
| `project disable` | `disable_project` (`--project-id`) | |
| `daemon status` | `list_projects`, `get_worker_status` | The launchd service state, active jobs, mailbox and proxy health. |
| `status`, `check`, `import-token`, `complete` | not routed | `status`, `submit`, `approve` and `retry` print the project's worker availability after the queue state. `check` is the network refresh. |

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
- **MCP transport (OSS-339).** Tool argument schemas, annotations, stdio
  framing and logging to stderr. The tool names above are fixed for it.
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
