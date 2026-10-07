# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased]

### Added

- **Atomic, idempotent enqueue** — `Db::enqueue` resolves a request in one
  `BEGIN IMMEDIATE` transaction: request-key lookup, coverage check,
  version/round allocation, job insert, key binding and the `job_enqueued`
  event. Concurrent requests from separate connections or processes can no
  longer both pass the duplicate check. It returns `Created` or `Existing`
  (`RequestReplay` / `Covered`) with the job, and a typed `EnqueueConflict`
  listing the differing fields when a key is reused for different content.
  Schema v2 adds the `enqueue_requests` table; existing databases upgrade in
  place.
- **`submit --request-key <key>`** — replaying a key returns the job it first
  resolved to, even once finished (until retention prunes that job), without
  starting a new round or clearing any cooldown.
- **Git tag requests are idempotent** — a tag first processed on schema v2
  whose seen-tag record is lost (crash, retention pruning) returns its
  original job instead of enqueueing again, while that job is retained.
- **Pinned PDF snapshots** — every job uploads an immutable copy of its PDF
  taken at enqueue, stored owner-only (0o600 files, 0o700 directories on Unix)
  at `<state_dir>/snapshots/<sha256>/<file name>`.
  Editing or deleting the source, or repointing the paper in config, after
  enqueue no longer changes what is submitted: the primary submit, the Node
  fallback and the timeout page count all read the snapshot, re-verified
  against the job's hash before every upload. `submission_input::prepare_input`
  returns the `PreparedInput` that enqueue callers pass as `JobPdf::Pinned`.
  Schema v3 adds `jobs.snapshot_path`; `meta.json` records it.
- **Unpinned jobs are verified before submission.** A job created before
  snapshots existed (or whose snapshot is lost) is snapshotted at submit time
  only if its `pdf_path` still hashes to its `pdf_hash` (event
  `snapshot_backfilled`). Otherwise it moves to `FAILED_NEEDS_MANUAL` with the
  recovery commands in `last_error` (event `submit_blocked_input_mismatch`)
  and nothing is uploaded.

- **`reviewloop::application`** — review operations shared by the CLI and
  the upcoming MCP adapter: `ReviewOps` with `list_projects`,
  `list_papers`, `request_review`, `get_job`, `list_jobs`, `get_review`,
  `approve_job`, `retry_job` and `cancel_job`. Operations are synchronous,
  never print, exit or contact the provider, return token-free DTOs and
  fail with `OpError` codes carrying a recovery hint and structured
  details. `request_review` pins a PDF snapshot and enqueues through
  `Db::enqueue` (request keys included); the CLI still submits immediately
  for `submit` and `run`.
- **`docs/review-operations.md`** — the operation contract: MCP tool
  names, request and result fields, job phases, retry semantics, error
  codes with recovery hints, and what later issues still own.
- `Db::get_review`, `Db::review_completed_at`, `Db::list_project_jobs` and
  `Db::list_registered_projects` (read-only).

### Changed

- **Coverage includes the venue.** A job covers a request with the same
  manuscript hash, backend, venue and version key; the same manuscript for a
  different venue is a new review. Venues are compared trimmed, blank meaning
  unset.
- **Round numbers count live reviews.** A new job takes one past the highest
  round of any pending, in-flight or completed job of the same version
  (previously: completed jobs only), so an explicit re-review started while
  another is in flight no longer shares its round. Failed attempts still give
  their round back.
- `submit`, `run`, `import-token` and both triggers record a single
  `job_enqueued` event (`source` says which, `enqueue_mode` whether it was a
  deduplicating request or an explicit new round) instead of
  `manual_submit_requested` / `run_submit_requested`.
- `submit --force` / `run` clear sibling cooldowns only after a job was
  actually created.
- `submit` resolves the submitter email before checking for an existing job,
  so it now needs a configured email even when it only reports one.
- A job's `pdf_hash` is the hash of its snapshot bytes, so coverage and
  request identity compare what will actually be uploaded.
- A queued job no longer needs its paper to stay in config: the snapshot is
  self-contained and config only supplies a fallback venue. Previously a
  removed paper failed every tick at submit.
- Retention pruning also removes snapshot directories that no job row
  references, after a one-hour grace; `retention_pruned` reports `snapshots`.
  `paper remove --purge-history` leaves them to the next pruning cycle.
- `submitted` / `submitted_via_fallback` events include `pdf_hash` and
  `snapshot_path`.

- `approve`, `cancel`, `retry`, `submit`, `run` and `complete --paper-id`
  call the shared operations. Arguments, events and exit codes are
  unchanged except:
  - `retry` refuses a PENDING_APPROVAL job (approve it instead) rather than
    queueing it without approval.
  - `retry` refuses a job whose registered config now declares another
    `project_id`. Before, a plain retry went ahead with that config and a
    forced one reset the job before failing in the worker.
  - `approve`, `cancel`, `complete` and `retry` with `--paper-id` outside a
    project fail with the project-config error instead of searching jobs
    that have no project.
  - `submit`, `run`, `approve`, `retry` and `cancel` log one more INFO line
    (to stdout under the default logging config).

### Fixed

- Git commands for a project repository ignore `GIT_DIR` / `GIT_INDEX_FILE`
  inherited from the environment. Run from a git hook (the pre-commit quality
  gate), the git trigger and its tests used to act on the repository being
  committed instead of the configured one.

## [0.2.1] — 2026-05-06

Non-blocking polish + defense-in-depth wave from eval3.

### Added

- **`Db::ensure_schema`** (was `init_schema`) — renamed for clarity (the
  function is idempotent and runs migrations, not just initialization).
  Split into private helpers (`enable_wal_mode`, `create_tables_if_missing`,
  `migrate_columns`, `create_indexes`) for targeted error context.
- **WAL pragma read-back** — `ensure_schema` now warns when SQLite falls
  back to a non-WAL journal mode on file-backed DBs (silent failure was
  the prior behaviour).
- **Foreign config validation** (`validate_for_foreign_load`) — when the
  CLI loads a project config from the registry path (not cwd), it now
  rejects `fallback_script` paths outside `$HOME` and warns on absolute
  `widget_state_dir` outside `$HOME` and proxy URLs with embedded
  credentials. Audit event `foreign_config_loaded` recorded in the
  events table for every such load.

### Fixed

- **0o600 file permissions** on widget-state.json, config.toml, and the
  SQLite DB on Unix (was 0o644 from default umask). Failures are now
  logged via `tracing::warn!` rather than silently discarded.
- **Bar UX** — active-job labels reorder to `paper-id (status, in Xs)`
  with paper name leftmost; `attempt=N` dropped when N=0; project
  submenu headers now read `(N active, M failed)` instead of cryptic
  `(NA · MF)`.
- **Bar performance** — background poller holds one `Db` instance for
  its lifetime instead of reopening per 5s tick. `ensure_schema` runs
  once at bar startup, not 720 times per hour.
- **Error vocabulary** — standardized to `"X not found: {id}"` for
  lookup misses; reserved `"no longer exists"` for genuine post-lookup
  races. Eliminates 5 different phrasings of the same situation.
- **`daemon status` timestamps** human-readable path now suffixes with
  ` UTC` (was `Z` which users in non-UTC zones often misread). `--json`
  output unchanged (still RFC3339 with `Z`).
- **`cancel --reason`** default cleaned up — no-reason path now writes
  `"cancelled by user"` instead of redundant `"cancelled by user: cancelled by user"`.

### Documentation

- **`reviewloop init`** doc comment + success output now point to
  `reviewloop init project --project-id <id>` as the next step.
- **`reviewloop retry --include-failed`** help text expanded with
  ambiguity example and `--job-id` fallback hint.
- **`reviewloop daemon status`** on non-macOS now suggests a SQLite
  fallback query in its error message.
- **README "Exit codes" section** added.
- **`status --active`** now mentioned in the doc comment + README
  command reference.

## [0.2.0] — 2026-05-06

First minor release after the Phase 0–8 UX overhaul. Touches every layer of
the daemon, CLI, menu-bar app, and adds a macOS Widget extension.

### Highlights

- Per-paper config (`papers[]` with venue/backend/PDF) replaces the
  hardcoded single-paper `[paper]` table.
- Strict global ↔ project config layering with explicit override chain
  and `Redacted<T>` wrapper for sensitive fields.
- Menu-bar app rewritten as a fleet-wide multi-project dashboard.
- macOS WidgetKit extension reads `widget-state.json` written by the
  daemon every tick.
- `reviewloop cancel --job-id` and `reviewloop retry --job-id` now work
  from any directory via a per-project config registry.
- Schema migration is now data-preserving and self-versioned via
  `PRAGMA user_version`.
- 178 tests, 0 warnings, CI green on Ubuntu + macOS.

### Breaking Changes

- **`reviewloop import-token` exits 2 on immediate failure** (Phase 0,
  N5/U5). Previously always exited 0 on token write. Now polls job state
  immediately after attaching and exits 2 if the poll resolves to a
  failure status (`Failed`, `FailedNeedsManual`, `Timeout`).

  *Migration*: scripts treating exit 0 as "token attached" must also
  check `reviewloop status --paper-id <id>` or handle exit 2 explicitly.
  A future `--no-poll` flag will restore the old behaviour.

- **`reviewloop status --json` shape unified** (Phase 0, C5-followup).
  Both single-paper (`--paper-id X`) and multi-paper paths now return
  the same wrapper shape:

  ```json
  {
    "project_id": "<id>",
    "papers": [
      { "paper_id": "<id>", "rows": [...], "timeline": [...] }
    ]
  }
  ```

  *Migration*: tooling that consumed the old flat-array multi-paper
  output must unwrap `payload.papers` and iterate paper objects.

- **State-machine guard on `Db::update_job_state`** (Phase 0, A2).
  Validates transitions via `JobStatus::can_transition` before writing.
  Override paths (`retry --force`, `complete`, `cancel`) use
  `Db::update_job_state_unchecked`.

### Added

- **`reviewloop-bar` menu-bar app** (Phase 8). Multi-project fleet
  view: aggregate status, per-project submenus with active jobs and
  recent failures, click-to-retry / click-to-cancel / click-to-open
  artifacts and logs. Anti-aliased disc icon, MenuSignature throttling
  to avoid rebuilding while user is reading.
- **macOS Widget Extension** (preview, `apple/ReviewLoopWidget/`). Daemon
  writes `widget-state.json` snapshots every tick; SwiftUI WidgetKit
  extension renders glance UI in macOS desktop / Notification Center.
  Schema documented at `docs/widget-schema.md` with golden round-trip
  test (B5).
- **`reviewloop run <pdf>` quickstart** (Phase 6) — submit + watch +
  print artifact paths in one shot.
- **`reviewloop cancel`** (U10) — mark a job cancelled (terminal). Works
  from any cwd via `--job-id`.
- **`reviewloop retry --include-failed`** (U8) — extend retry candidate
  search to terminal failure statuses.
- **Project registry** (`projects` table) — per-project config paths
  recorded on every successful `load_runtime`. Enables `cmd_retry` to
  resolve the right per-project provider/polling/papers config when
  invoked from any cwd. Self-heals stale entries via
  `forget_project_registration` on `NotFound`.
- **`reviewloop daemon status`** with `--json` flag, tick health, last
  tick error, gmail OAuth status, proxy health.
- **`reviewloop paper add --venue`** (U3) — per-paper venue override.
- **OS notifications via `notify-rust`** (Phase 7) — terminal job state
  changes optionally surface as desktop notifications.
- **HTTP round-robin proxy pool with sequential failover** (commits
  82d5b75 + 3e79877). `core.proxies` accepts a list; failures emit DB
  events.
- **`core.widget_state_enabled`** (default `true`) and
  **`core.widget_state_dir`** (default `state_dir`) config fields.
- **`PRAGMA user_version` schema versioning** (B2). Migrations skip
  already-applied phases. Backfill UPDATEs guarded by column-existence
  checks via `PRAGMA table_info`.

### Fixed

- **Gmail OAuth tokens preemptively refreshed in daemon** (B6). Daemon
  checks `expires_at` ≤ 5 minutes from now before each Gmail API call;
  refreshes and persists. Refresh failure logs `warn!` with re-login
  hint and skips the iteration (no daemon crash). Long-running daemon
  no longer breaks IMAP/Gmail polling after the first hour.
- **`cmd_retry` cross-cwd dispatch correctness** (B1 + B3). Removed
  TOCTOU `exists()` precheck before `load_runtime_for_path`; self-heals
  registry on `io::ErrorKind::NotFound` via `forget_project_registration`
  in the error path. Logging-init reuse documented (no panic on second
  call). Two new regression tests:
  `load_runtime_for_path_does_not_panic_on_repeated_call` and
  `load_effective_config_for_job_self_heals_when_registered_path_missing`.
- **Schema migration data preservation** (B2). Backfill UPDATEs in
  `ensure_schema` are now gated on column existence; pre-existing data
  values survive the upgrade. Three new regression tests.
- **Bar app fleet-view rewrite** (commit 722c58b). Drops requirement
  for `REVIEWLOOP_PROJECT_ID`; reads all projects from shared DB.
  Per-project sub-grouping, legacy-bucket for empty `project_id`.
- **Bar disc icon** (commit c6eaf51). Replaces solid square with
  anti-aliased coloured disc.
- **Schema migration order** (commit 6859eae). Pre-Phase-0 DBs upgrade
  cleanly: tables → ensure_column_exists → indexes (was: all in one
  batch, indexes referenced columns the migration hadn't yet added).
- **Clippy `sort_by_key`** at `src/main.rs:2759` (commit 6859eae).
- **`notify-rust` Linux build** — dropped `default-features = false`
  which was stripping both dbus and zbus backends (commit 6859eae).
- **`reviewloop run` error wording** (B8). Now explains *why* a project
  config is required (state storage location) and suggests the fix.
- **`load_effective_config_for_job` error wording** (B7). Numbered
  steps, concrete `reviewloop status` command, plain-English explanation
  for both "never registered" and "path no longer exists" branches.
- **Bar "Submit new…" pre-validates** (B9). Rejects PDFs outside a
  configured project repo with a native `rfd` alert before spawning
  `reviewloop run`.

### UX

- **Error wording overhaul**: actionable steps, concrete commands, and
  plain-English context for the most-hit error sites in `cmd_retry`,
  `cmd_run`, and the bar's job-action paths.
- **Bar menu structure**: per-project submenu headers, active-job
  submenus with retry/cancel/open-artifacts/open-log actions, recent-
  failures list capped per project (5 each, fleet-wide via SQL window
  function).

### Documentation

- **`docs/widget-schema.md`** — Widget JSON schema v1 with field types,
  semantics, sample document, schema-bump procedure, cross-platform
  notes.
- **README "Deployment model" section** (B4) — documents the v0.2.0
  single-daemon-per-machine constraint.
- Per-command doc comments expanded (Phase 4, F4).

### Known limitations (v0.2.0)

- **Single daemon per machine.** The launchd label `ai.reviewloop.daemon`
  is hardcoded; installing the daemon from a second project repo
  overwrites the first plist. The bar shows fleet-wide job data from
  all projects, but the active daemon services jobs for only one
  project at a time. Multi-daemon (label-per-project) support is on the
  v0.3.0 roadmap.
- **Bar's "Pause / Resume daemon"** controls the single installed
  daemon. There is no per-project pause control.
- **Legacy data with `project_id = ''`** (pre-Phase-0): bar's "Retry"
  button cannot resolve a project config for these. Cancel works from
  any cwd. Manual SQL nuke via `DELETE FROM jobs WHERE project_id = ''`
  is the recommended cleanup; future `reviewloop db purge-legacy`
  command is on the v0.2.1 backlog.

### Internal

- 178 tests pass via `./scripts/quality-gates.sh`
  (`cargo fmt --check + clippy + cargo test`).
- 26 Rust source files in `src/`, 6 Swift in
  `apple/ReviewLoopWidget/`, ~13K LOC Rust + ~400 Swift.
- CI green on `ubuntu-latest` + `macos-latest`.

