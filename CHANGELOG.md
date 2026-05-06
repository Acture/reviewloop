# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

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
  `init_schema` are now gated on column existence; pre-existing data
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

