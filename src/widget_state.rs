//! Widget state snapshot writer for macOS WidgetKit integration.
//!
//! Every daemon tick this module builds a small JSON file that a parallel
//! Swift WidgetKit extension (W2B) reads to render the home-screen widget.
//!
//! The JSON schema is frozen and shared with W2B; do **not** change field
//! names or types without coordinating with the Swift side.
//!
//! ## `completed_today` note
//! "Completed today" is defined as jobs whose `updated_at` falls on the
//! current **UTC** calendar date. Local-timezone date is acceptable for V1
//! (documented here so W2B can decide whether to call it out in the UI).

use crate::{config::Config, db::Db};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{fs, io::Write, path::Path};

// ---------------------------------------------------------------------------
// Tick-health thresholds (seconds).  Mirrored from `cmd_daemon_status` so
// both the CLI status output and the widget file always agree.
// ---------------------------------------------------------------------------

/// Ticks younger than this are considered healthy.
pub const TICK_HEALTH_NORMAL_SECS: i64 = 60;
/// Ticks between NORMAL and STUCK thresholds are "stale".
pub const TICK_HEALTH_STUCK_SECS: i64 = 300;

/// Compute tick-health label from the age of the last tick (in seconds).
/// Mirrors the logic in `cmd_daemon_status`.
pub fn tick_health_label(last_tick_at: Option<DateTime<Utc>>) -> &'static str {
    match last_tick_at {
        None => "unknown",
        Some(ts) => {
            let age = (Utc::now() - ts).num_seconds();
            if age < TICK_HEALTH_NORMAL_SECS {
                "normal"
            } else if age < TICK_HEALTH_STUCK_SECS {
                "stale"
            } else {
                "stuck"
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schema structs — field names MUST match the frozen JSON contract.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct WidgetState {
    pub schema_version: u32,
    pub generated_at: String,
    pub project_id: String,
    pub summary: WidgetSummary,
    pub active_jobs: Vec<WidgetActiveJob>,
    pub recent_failures: Vec<WidgetFailure>,
    pub last_tick_at: Option<String>,
    pub last_tick_error: Option<WidgetTickError>,
    pub tick_health: &'static str,
}

#[derive(Debug, Serialize)]
pub struct WidgetSummary {
    pub active_count: usize,
    pub failed_recent_24h: usize,
    pub completed_today: usize,
}

#[derive(Debug, Serialize)]
pub struct WidgetActiveJob {
    pub paper_id: String,
    pub status: String,
    pub attempt: u32,
    pub next_poll_at: Option<String>,
    pub started_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct WidgetFailure {
    pub paper_id: String,
    pub status: String,
    pub last_error: String,
    pub occurred_at: String,
}

#[derive(Debug, Serialize)]
pub struct WidgetTickError {
    pub at: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate `s` to at most `max_chars` Unicode scalar values (char-boundary
/// safe). Mirrors `truncate_chars` used elsewhere in the codebase.
fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => &s[..byte_idx],
        None => s,
    }
}

fn fmt_rfc3339(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

/// Build a [`WidgetState`] from the current database state.
pub fn build(config: &Config, db: &Db) -> Result<WidgetState> {
    let project_id = &config.project_id;
    let now = Utc::now();

    // --- last tick timestamp & health ---
    let last_tick_at = db
        .most_recent_event_created_at(project_id)
        .context("failed to read last tick timestamp")?;
    let tick_health = tick_health_label(last_tick_at);

    // --- last tick error (mirrors daemon status logic) ---
    let last_tick_error: Option<WidgetTickError> = {
        let ev_opt = db
            .most_recent_event_of_type(project_id, "tick_failed")
            .context("failed to read tick_failed events")?;
        if let Some(ev) = ev_opt {
            // Only surface the error if it's still the most recent event
            // (the daemon hasn't recovered since) and it's within 3 minutes.
            let recovered = last_tick_at
                .map(|latest| latest > ev.created_at)
                .unwrap_or(false);
            let stale = (now - ev.created_at).num_seconds() > 180;
            if !recovered && !stale {
                let msg = ev
                    .payload
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("(no error message)")
                    .to_string();
                Some(WidgetTickError {
                    at: fmt_rfc3339(ev.created_at),
                    message: msg,
                })
            } else {
                None
            }
        } else {
            None
        }
    };

    // --- active jobs (capped at 10, sorted by next_poll_at ASC, None first) ---
    let mut raw_active = db
        .list_active_jobs_for_project(project_id)
        .context("failed to read active jobs")?;
    raw_active.sort_by(|a, b| match (a.next_poll_at, b.next_poll_at) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(ta), Some(tb)) => ta.cmp(&tb),
    });
    let active_jobs: Vec<WidgetActiveJob> = raw_active
        .iter()
        .take(10)
        .map(|j| WidgetActiveJob {
            paper_id: j.paper_id.clone(),
            status: j.status.as_str().to_string(),
            attempt: j.attempt,
            next_poll_at: j.next_poll_at.map(fmt_rfc3339),
            started_at: j.started_at.map(fmt_rfc3339),
        })
        .collect();

    // --- recent failures (capped at 5, sorted by occurred_at DESC) ---
    // list_failed_jobs_for_project already excludes cancellations (W1a filter).
    let raw_failures = db
        .list_failed_jobs_for_project(project_id, 5)
        .context("failed to read failed jobs")?;
    let recent_failures: Vec<WidgetFailure> = raw_failures
        .iter()
        .map(|j| {
            let raw_err = j.last_error.as_deref().unwrap_or("(unknown error)");
            WidgetFailure {
                paper_id: j.paper_id.clone(),
                status: j.status.as_str().to_string(),
                last_error: truncate_chars(raw_err, 80).to_string(),
                occurred_at: fmt_rfc3339(j.updated_at),
            }
        })
        .collect();

    // --- summary counts ---
    let active_count = raw_active.len();

    let cutoff_24h = now - chrono::Duration::hours(24);
    let failed_recent_24h = raw_failures
        .iter()
        .filter(|j| j.updated_at >= cutoff_24h)
        .count();

    // completed_today: COMPLETED jobs whose updated_at is on today's UTC date.
    let today_str = now.format("%Y-%m-%d").to_string();
    let completed_today = db
        .count_completed_today(project_id, &today_str)
        .context("failed to count completed-today jobs")?;

    Ok(WidgetState {
        schema_version: 1,
        generated_at: fmt_rfc3339(now),
        project_id: project_id.clone(),
        summary: WidgetSummary {
            active_count,
            failed_recent_24h,
            completed_today,
        },
        active_jobs,
        recent_failures,
        last_tick_at: last_tick_at.map(fmt_rfc3339),
        last_tick_error,
        tick_health,
    })
}

/// Write `state` to `path` atomically via a `.tmp.<pid>` sibling + rename.
/// Mirrors the pattern used by `save_toml_file` in `config.rs`.
pub fn write_atomically(path: &Path, state: &WidgetState) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create widget state directory: {}",
                parent.display()
            )
        })?;
    }

    let content =
        serde_json::to_string_pretty(state).context("failed to serialize widget state")?;

    let tmp_name = format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("widget-state.json"),
        std::process::id()
    );
    let tmp_path = path.with_file_name(tmp_name);
    {
        #[cfg(unix)]
        let mut f = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp_path)
                .with_context(|| {
                    format!(
                        "failed to create temp widget state file: {}",
                        tmp_path.display()
                    )
                })?
        };
        #[cfg(not(unix))]
        let mut f = std::fs::File::create(&tmp_path).with_context(|| {
            format!(
                "failed to create temp widget state file: {}",
                tmp_path.display()
            )
        })?;
        f.write_all(content.as_bytes()).with_context(|| {
            format!(
                "failed to write temp widget state file: {}",
                tmp_path.display()
            )
        })?;
        f.sync_all().with_context(|| {
            format!(
                "failed to fsync temp widget state file: {}",
                tmp_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically rename {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

/// Convenience wrapper: build state from DB and write it atomically.
pub fn build_and_write(config: &Config, db: &Db, path: &Path) -> Result<()> {
    let state = build(config, db)?;
    write_atomically(path, &state)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        db::Db,
        model::{JobStatus, NewJob},
    };

    /// Create a fresh, schema-initialized in-memory DB for each test.
    /// Each test gets a unique name to avoid SQLite shared-cache collisions.
    fn make_db(name: &str) -> Db {
        let db = Db::new_in_memory(name).expect("new_in_memory");
        db.ensure_schema().expect("ensure_schema");
        db
    }

    fn default_config_for(project_id: &str) -> Config {
        Config {
            project_id: project_id.to_string(),
            ..Config::default()
        }
    }

    /// Create a job in the given status with an optional last_error.
    /// Uses `create_job` (inserts as the requested status) then
    /// `update_job_state_unchecked` to set status + last_error.
    fn make_job(
        db: &Db,
        project_id: &str,
        paper_id: &str,
        status: JobStatus,
        last_error: Option<&str>,
    ) {
        let new_job = NewJob {
            project_id: project_id.to_string(),
            paper_id: paper_id.to_string(),
            backend: "test".to_string(),
            pdf_path: "/dev/null".to_string(),
            pdf_hash: "deadbeef".to_string(),
            status: JobStatus::Queued,
            email: "t@example.com".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        };
        let job = db.create_job(&new_job).expect("create_job");
        if status != JobStatus::Queued || last_error.is_some() {
            db.update_job_state_unchecked(
                &job.id,
                status,
                None,
                None,
                Some(last_error.map(str::to_string)),
            )
            .expect("update_job_state_unchecked");
        }
    }

    #[test]
    fn build_with_in_memory_db_produces_valid_schema() {
        let db = make_db("widget-schema");
        let cfg = default_config_for("test-proj");

        // 1 Processing job
        make_job(&db, "test-proj", "paper-a", JobStatus::Processing, None);
        // 2 Failed jobs (non-cancellation)
        make_job(
            &db,
            "test-proj",
            "paper-b",
            JobStatus::Failed,
            Some("review generation failed"),
        );
        make_job(
            &db,
            "test-proj",
            "paper-c",
            JobStatus::Failed,
            Some("network timeout"),
        );
        // 1 cancelled — must be excluded from recent_failures
        make_job(
            &db,
            "test-proj",
            "paper-d",
            JobStatus::Failed,
            Some("cancelled by user: explicit cancel"),
        );
        // 1 Completed today
        make_job(&db, "test-proj", "paper-e", JobStatus::Completed, None);

        let state = build(&cfg, &db).expect("build");

        assert_eq!(state.schema_version, 1);
        assert_eq!(state.project_id, "test-proj");

        // active_count = 1 (Processing)
        assert_eq!(state.summary.active_count, 1);
        assert_eq!(state.active_jobs.len(), 1);
        assert_eq!(state.active_jobs[0].paper_id, "paper-a");
        assert_eq!(state.active_jobs[0].status, "PROCESSING");

        // recent_failures: 2 non-cancellation failures; cancelled excluded
        assert_eq!(state.recent_failures.len(), 2);
        let failure_papers: Vec<&str> = state
            .recent_failures
            .iter()
            .map(|f| f.paper_id.as_str())
            .collect();
        assert!(
            !failure_papers.contains(&"paper-d"),
            "cancelled job must be excluded from recent_failures"
        );

        // completed_today = 1
        assert_eq!(state.summary.completed_today, 1);

        // failed_recent_24h = 2 (the two real failures are fresh)
        assert_eq!(state.summary.failed_recent_24h, 2);

        // tick_health is unknown (no events logged for this project)
        assert_eq!(state.tick_health, "unknown");

        // no tick error
        assert!(state.last_tick_error.is_none());
    }

    #[test]
    fn widget_state_v1_serializes_to_documented_shape() {
        use chrono::TimeZone;

        let state = WidgetState {
            schema_version: 1,
            generated_at: fmt_rfc3339(chrono::Utc.with_ymd_and_hms(2026, 5, 6, 12, 0, 0).unwrap()),
            project_id: "test-proj".to_string(),
            summary: WidgetSummary {
                active_count: 2,
                failed_recent_24h: 1,
                completed_today: 3,
            },
            active_jobs: vec![WidgetActiveJob {
                paper_id: "paper-a".to_string(),
                status: "PROCESSING".to_string(),
                attempt: 2,
                next_poll_at: Some(fmt_rfc3339(
                    chrono::Utc.with_ymd_and_hms(2026, 5, 6, 12, 5, 0).unwrap(),
                )),
                started_at: Some(fmt_rfc3339(
                    chrono::Utc.with_ymd_and_hms(2026, 5, 6, 11, 50, 0).unwrap(),
                )),
            }],
            recent_failures: vec![WidgetFailure {
                paper_id: "paper-b".to_string(),
                status: "FAILED".to_string(),
                last_error: "rate limit exceeded".to_string(),
                occurred_at: fmt_rfc3339(
                    chrono::Utc.with_ymd_and_hms(2026, 5, 6, 11, 55, 0).unwrap(),
                ),
            }],
            last_tick_at: Some(fmt_rfc3339(
                chrono::Utc
                    .with_ymd_and_hms(2026, 5, 6, 11, 59, 50)
                    .unwrap(),
            )),
            last_tick_error: Some(WidgetTickError {
                at: fmt_rfc3339(
                    chrono::Utc
                        .with_ymd_and_hms(2026, 5, 6, 11, 59, 55)
                        .unwrap(),
                ),
                message: "daemon lost connection".to_string(),
            }),
            tick_health: "normal",
        };
        let json = serde_json::to_string_pretty(&state).expect("serialise");
        let expected = r#"{
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
}"#;
        assert_eq!(
            json, expected,
            "widget JSON shape changed; bump schema_version and update docs/widget-schema.md"
        );
    }

    #[test]
    fn write_atomically_round_trips() {
        let db = make_db("widget-roundtrip");
        let cfg = default_config_for("rtrip");
        let state = build(&cfg, &db).expect("build");

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("widget-state.json");
        write_atomically(&path, &state).expect("write_atomically");

        let raw = std::fs::read_to_string(&path).expect("read back");
        let v: serde_json::Value = serde_json::from_str(&raw).expect("parse json");

        // Verify all required top-level keys are present.
        for key in &[
            "schema_version",
            "generated_at",
            "project_id",
            "summary",
            "active_jobs",
            "recent_failures",
            "last_tick_at",
            "last_tick_error",
            "tick_health",
        ] {
            assert!(v.get(key).is_some(), "missing key: {key}");
        }
        assert_eq!(v["schema_version"], 1);

        // No leftover .tmp.* files should remain after a successful write.
        let leftover: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftover.is_empty(), "leftover tmp files: {leftover:?}");
    }

    #[cfg(unix)]
    #[test]
    fn widget_state_file_is_0o600_after_atomic_write() {
        use std::os::unix::fs::PermissionsExt;

        let db = make_db("widget-mode");
        let cfg = default_config_for("mode-test");
        let state = build(&cfg, &db).expect("build");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("widget-state.json");

        write_atomically(&path, &state).expect("write_atomically");

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "widget state must be 0o600 after write");
    }

    #[test]
    fn disabled_returns_none_path() {
        let mut cfg = Config::default();
        cfg.core.widget_state_enabled = false;
        assert!(
            cfg.widget_state_path().is_none(),
            "should be None when disabled"
        );
    }

    #[test]
    fn truncates_long_last_error_at_80_chars() {
        // Use a multi-byte character near the boundary to verify char-boundary
        // safety — 'á' is 2 bytes in UTF-8.
        let long: String = "á".repeat(50) + &"x".repeat(50); // 100 Unicode chars
        let truncated = truncate_chars(&long, 80);
        assert_eq!(truncated.chars().count(), 80, "should be exactly 80 chars");
        // No panic means the slice is at a valid UTF-8 boundary.
        assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
    }
}
