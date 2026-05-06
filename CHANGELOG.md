# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] — 2026-05-05

### Added

- **macOS Widget Extension (preview).** Daemon now writes `widget-state.json`
  snapshots every tick; a SwiftUI WidgetKit extension under
  `apple/ReviewLoopWidget/` renders glance UI (active job count, recent
  failures) in macOS desktop / Notification Center widgets. Build instructions
  in README.
- **`core.widget_state_enabled`** (default `true`) and
  **`core.widget_state_dir`** (default `None` → `state_dir`) config fields for
  controlling widget snapshot output.

### Known limitations

- **Single daemon per machine.** The launchd label `ai.reviewloop.daemon`
  is hardcoded; installing the daemon from a second project repo overwrites
  the first plist. The bar shows fleet-wide job data from all projects, but
  the active daemon services jobs for only one project at a time. Multi-
  daemon (label-per-project) support is planned for v0.3.0.

---

## [Unreleased] — 2025-05-05

### Breaking Changes

- **`reviewloop import-token` now exits 2 on immediate failure** (N5 / U5).
  Previously the command always exited 0 when the token was successfully
  written to the database.  It now polls the job state immediately after
  attaching the token and exits with code 2 if the poll resolves to a
  failure status (`Failed`, `FailedNeedsManual`, or `Timeout`).

  **Migration**: scripts that treated exit 0 as "token attached successfully"
  must be updated to also check `reviewloop status --paper-id <id>` after the
  import, or to handle exit code 2 explicitly.  A future `--no-poll` flag will
  restore the old behaviour without requiring status polling.

- **`reviewloop status --json` root shape unified** (C5-followup).
  Previously the multi-paper path (no `--paper-id` flag) returned a flat JSON
  array of row objects, while the single-paper path (`--paper-id X`) returned
  an object `{project_id, papers: [{paper_id, rows, timeline}]}`.  Both paths
  now always return the same shape:

  ```json
  {
    "project_id": "<id>",
    "papers": [
      { "paper_id": "<id>", "rows": [...], "timeline": [...] }
    ]
  }
  ```

  An empty result set is `{"project_id": "...", "papers": []}`.

  **Migration**: tooling that consumed the old flat-array multi-paper output
  must be updated to unwrap `payload.papers` and iterate over paper objects
  rather than raw row objects.

### Added

- State-machine guard on `Db::update_job_state` (A2).  The method now
  validates transitions via `JobStatus::can_transition` before writing.
  CLI override paths that legitimately escape terminal states
  (`retry --force`, `complete`, `cancel`) use the new
  `Db::update_job_state_unchecked` variant, which skips the guard.
